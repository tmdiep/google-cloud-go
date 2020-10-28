// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and

package pubsublite

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"google.golang.org/api/option"
	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errOutOfOrderMessages           = errors.New("pubsublite: messages are out of order")
	errInvalidInitialCommitResponse = errors.New("pubsublite: first response from server was not an initial response for streaming commit")
)

const (
	// Represents an uninitialized cursor offset.
	nilCursorOffset int64 = -1

	// The frequency of cursor commits.
	commitCursorPeriod = 50 * time.Millisecond
)

// ackedFunc is invoked when a message has been acked by the user.
type ackedFunc func(*ackReceiver)

// ackReceiver is used for handling message acks. It is attached to a Message
// and also stored within the subscriber client for tracking until the message
// has been acked by the server.
type ackReceiver struct {
	// The message offset.
	Offset int64
	// Bytes released to the flow controller once the message has been acked.
	MsgSize int64

	// Guards access to fields below.
	mu    sync.Mutex
	acked bool
	onAck ackedFunc
}

func newAckReceiver(offset, msgSize int64, onAck ackedFunc) *ackReceiver {
	return &ackReceiver{Offset: offset, MsgSize: msgSize, onAck: onAck}
}

func (ah *ackReceiver) Ack() {
	ah.mu.Lock()
	defer ah.mu.Unlock()

	if ah.acked {
		return
	}
	ah.acked = true
	if ah.onAck != nil {
		// Don't block the user's goroutine with potentially expensive ack
		// processing.
		go ah.onAck(ah)
	}
}

func (ah *ackReceiver) IsAcked() bool {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	return ah.acked
}

// Clear onAck when the ack is obsolete.
func (ah *ackReceiver) Clear() {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	ah.onAck = nil
}

// ackTracker manages outstanding message acks, i.e. messages that have been
// delivered to the user, but not yet acked.
type ackTracker struct {
	// Guards access to fields below.
	mu sync.Mutex

	// All offsets before and including this prefix have been acked by the user.
	ackedPrefixOffset int64
	// Outstanding message acks, strictly ordered by increasing message offsets.
	outstandingAcks *list.List // Value = *ackReceiver
}

func newAckTracker() *ackTracker {
	return &ackTracker{
		ackedPrefixOffset: nilCursorOffset,
		outstandingAcks:   list.New(),
	}
}

// Reset the ackTracker back to its initial state. Any remaining outstanding
// acks are considered to be obsolete.
func (at *ackTracker) Reset() {
	at.mu.Lock()
	defer at.mu.Unlock()

	for elem := at.outstandingAcks.Front(); elem != nil; elem = elem.Next() {
		ack, _ := elem.Value.(*ackReceiver)
		ack.Clear()
	}
	at.outstandingAcks.Init()
	at.ackedPrefixOffset = nilCursorOffset
}

// Push adds an outstanding ack to the tracker.
func (at *ackTracker) Push(ack *ackReceiver) error {
	at.mu.Lock()
	defer at.mu.Unlock()

	// These errors should not occur unless there is a bug in the client library.
	if ack.Offset <= at.ackedPrefixOffset {
		return errOutOfOrderMessages
	}
	if elem := at.outstandingAcks.Back(); elem != nil {
		lastOutstandingAck, _ := elem.Value.(*ackReceiver)
		if ack.Offset <= lastOutstandingAck.Offset {
			return errOutOfOrderMessages
		}
	}

	at.outstandingAcks.PushBack(ack)
	return nil
}

// Pop processes outstanding acks and updates `ackedPrefixOffset`.
func (at *ackTracker) Pop() {
	at.mu.Lock()
	defer at.mu.Unlock()

	for elem := at.outstandingAcks.Front(); elem != nil; elem = elem.Next() {
		ack, _ := elem.Value.(*ackReceiver)
		if !ack.IsAcked() {
			break
		}
		at.ackedPrefixOffset = ack.Offset
		at.outstandingAcks.Remove(elem)
		ack.Clear()
	}
}

// CommitOffset returns the cursor offset that should be committed. May return
// nilCursorOffset if no messages have been acked thus far.
func (at *ackTracker) CommitOffset() int64 {
	at.mu.Lock()
	defer at.mu.Unlock()

	if at.ackedPrefixOffset == nilCursorOffset {
		return nilCursorOffset
	}
	// Convert from last acked to first unacked, which is the cursor offset to be
	// committed.
	return at.ackedPrefixOffset + 1
}

// committedCursorTracker tracks pending and last successful committed offsets.
type committedCursorTracker struct {
	acks *ackTracker
	// Last offset for which the server acknowledged the commit.
	lastConfirmedOffset int64
	// Unacknowledged committed offsets.
	pendingOffsets *list.List // Value = int64
}

func newCommittedCursorTracker(acks *ackTracker) *committedCursorTracker {
	return &committedCursorTracker{
		acks:                acks,
		lastConfirmedOffset: nilCursorOffset,
		pendingOffsets:      list.New(),
	}
}

func extractOffsetFromElem(elem *list.Element) int64 {
	if elem == nil {
		return nilCursorOffset
	}
	offset, _ := elem.Value.(int64)
	return offset
}

// ClearPending discards old pending offsets. Should be called when the stream
// reconnects.
func (ct *committedCursorTracker) ClearPending() {
	ct.pendingOffsets.Init()
}

// NextOffset is the next offset to be sent to the stream, if any.
func (ct *committedCursorTracker) NextOffset() int64 {
	wantCommitOffset := ct.acks.CommitOffset()
	if wantCommitOffset <= ct.lastConfirmedOffset {
		return nilCursorOffset
	}
	if wantCommitOffset <= extractOffsetFromElem(ct.pendingOffsets.Back()) {
		return nilCursorOffset
	}
	return wantCommitOffset
}

// AddPending adds a sent, but not yet confirmed, offset.
func (ct *committedCursorTracker) AddPending(offset int64) {
	ct.pendingOffsets.PushBack(offset)
}

// AcknowledgeOffsets processes the server's acknowledgement of the first
// `num` pending offsets.
func (ct *committedCursorTracker) AcknowledgeOffsets(numAcked int64) error {
	if numPending := int64(ct.pendingOffsets.Len()); numPending < numAcked {
		return fmt.Errorf("pubsublite: server acknowledged %d cursor commits, but only %d were sent", numAcked, numPending)
	}

	for i := int64(0); i < numAcked; i++ {
		front := ct.pendingOffsets.Front()
		ct.lastConfirmedOffset = extractOffsetFromElem(front)
		ct.pendingOffsets.Remove(front)
	}
	fmt.Printf("Last confirmed offset = %d\n", ct.lastConfirmedOffset)
	return nil
}

// committerStatus captures the lifecycle of a committer, in order. Note that
// some statuses may be skipped.
type committerStatus int

const (
	// Note: committer.updateStatus assumes these have the same numeric values as
	// serviceEvent. Refactor if this changes.

	// Committer has not been started.
	committerUninitialized committerStatus = 0
	// Committer is active. Note that the underlying stream may be reconnecting
	// due to retryable errors.
	committerActive committerStatus = 1
	// Committer is gracefully shutting down by flushing all pending commits.
	committerFlushing committerStatus = 2
	// Committer has terminated.
	committerTerminated committerStatus = 3
)

type committer struct {
	// Immutable after creation.
	cursorClient *vkit.CursorClient
	subscription SubscriptionPath
	partition    int
	initialReq   *pb.StreamingCommitCursorRequest
	onEvent      serviceEventFunc

	pollCommitsTicker *time.Ticker
	stopPolling       chan struct{}

	// Guards access to fields below.
	mu sync.Mutex

	stream   *retryableStream
	status   committerStatus
	finalErr error

	cursorTracker *committedCursorTracker
}

func newCursorClient(ctx context.Context, region string, opts ...option.ClientOption) (*vkit.CursorClient, error) {
	if err := validateRegion(region); err != nil {
		return nil, err
	}
	options := append(defaultClientOptions(region), opts...)
	return vkit.NewCursorClient(ctx, options...)
}

func newCommitter(ctx context.Context, cursor *vkit.CursorClient, subs SubscriptionPath, partition int, onEvent serviceEventFunc, acks *ackTracker) *committer {
	commit := &committer{
		cursorClient: cursor,
		subscription: subs,
		partition:    partition,
		initialReq: &pb.StreamingCommitCursorRequest{
			Request: &pb.StreamingCommitCursorRequest_Initial{
				Initial: &pb.InitialCommitCursorRequest{
					Subscription: subs.String(),
					Partition:    int64(partition),
				},
			},
		},
		onEvent:           onEvent,
		pollCommitsTicker: time.NewTicker(commitCursorPeriod),
		stopPolling:       make(chan struct{}),
		cursorTracker:     newCommittedCursorTracker(acks),
	}
	// TODO: the timeout should be from ReceiveSettings.
	commit.stream = newRetryableStream(ctx, commit, time.Minute, reflect.TypeOf(pb.StreamingCommitCursorResponse{}))
	return commit
}

func (c *committer) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stream.Start()
	go c.pollCommits()
}

func (c *committer) Stop() {
}

func (c *committer) newStream(ctx context.Context) (grpc.ClientStream, error) {
	return c.cursorClient.StreamingCommitCursor(ctx)
}

func (c *committer) initialRequest() interface{} {
	return c.initialReq
}

func (c *committer) validateInitialResponse(response interface{}) error {
	commitResponse, _ := response.(*pb.StreamingCommitCursorResponse)
	if commitResponse.GetInitial() == nil {
		return errInvalidInitialCommitResponse
	}
	return nil
}

func (c *committer) onStreamStatusChange(status streamStatus) {
	fmt.Printf("Committer.onStreamStatusChange(%d), err=%v\n", status, c.stream.Error())
	c.mu.Lock()
	defer c.mu.Unlock()

	switch status {
	case streamConnected:
		// TODO: update status
		c.unsafeCommitOffsetToStream()
		c.pollCommitsTicker.Reset(commitCursorPeriod)

	case streamReconnecting:
		c.cursorTracker.ClearPending()
		c.pollCommitsTicker.Stop()

	case streamTerminated:
		close(c.stopPolling)
	}
}

func (c *committer) commitOffsetToStream() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unsafeCommitOffsetToStream()
}

// unsafeCommitOffsetToStream should be called with committer.mu held.
func (c *committer) unsafeCommitOffsetToStream() {
	nextOffset := c.cursorTracker.NextOffset()
	if nextOffset == nilCursorOffset {
		return
	}

	req := &pb.StreamingCommitCursorRequest{
		Request: &pb.StreamingCommitCursorRequest_Commit{
			Commit: &pb.SequencedCommitCursorRequest{
				Cursor: &pb.Cursor{Offset: nextOffset},
			},
		},
	}
	if c.stream.Send(req) {
		fmt.Printf("Sent: %v\n", req)
		c.cursorTracker.AddPending(nextOffset)
	}
}

func (c *committer) onResponse(response interface{}) {
	fmt.Printf("Response: %v\n", response)
	c.mu.Lock()
	defer c.mu.Unlock()

	commitResponse, _ := response.(*pb.StreamingCommitCursorResponse)
	if commitResponse.GetCommit() == nil {
		return
	}
	numAcked := commitResponse.GetCommit().GetAcknowledgedCommits()
	if numAcked <= 0 {
		return
	}
	c.cursorTracker.AcknowledgeOffsets(numAcked)
}

// pollCommits executes in a goroutine to periodically commit cursors to the
// stream.
func (c *committer) pollCommits() {
	fmt.Printf("Started: %v\n", time.Now())
	for {
		select {
		case <-c.stopPolling:
			break
		case <-c.pollCommitsTicker.C:
			c.mu.Lock()
			c.commitOffsetToStream()
			c.mu.Unlock()
		}
	}
	fmt.Printf("Stopped: %v\n", time.Now())
}

// updateStatus must be called with committer.mu held.
func (c *committer) updateStatus(targetStatus committerStatus, err error) bool {
	if c.status >= targetStatus {
		// Already at the same or later stage of the committer lifecycle.
		return false
	}

	c.status = targetStatus
	if err != nil {
		// Prevent nil clobbering an original error.
		c.finalErr = err
	}
	if c.onEvent != nil {
		// Translate committer status to service events. They currently have the
		// same values.
		event := serviceEvent(targetStatus)

		// Notify in a goroutine to prevent deadlocks if the parent is holding a
		// locked mutex.
		go c.onEvent(c, event, c.finalErr)
	}
	return true
}
