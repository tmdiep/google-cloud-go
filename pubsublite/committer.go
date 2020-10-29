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
	errInvalidCommitResponse        = errors.New("pubsublite: received invalid commit response from server")
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
// acks are considered to be obsolete. This should be called when the
// subscriber client terminates.
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
// reconnects, as the acknowledgements for these would not be received.
func (ct *committedCursorTracker) ClearPending() {
	ct.pendingOffsets.Init()
}

// NextOffset is the next offset to be sent to the stream, if any.
func (ct *committedCursorTracker) NextOffset() int64 {
	newCommitOffset := ct.acks.CommitOffset()
	if newCommitOffset <= ct.lastConfirmedOffset {
		return nilCursorOffset
	}
	if newCommitOffset <= extractOffsetFromElem(ct.pendingOffsets.Back()) {
		return nilCursorOffset
	}
	return newCommitOffset
}

// AddPending adds a sent, but not yet acknowledged, committed offset.
func (ct *committedCursorTracker) AddPending(offset int64) {
	ct.pendingOffsets.PushBack(offset)
}

// AcknowledgeOffsets processes the server's acknowledgement of the first
// `numAcked` pending offsets.
func (ct *committedCursorTracker) AcknowledgeOffsets(numAcked int64) error {
	if numPending := int64(ct.pendingOffsets.Len()); numPending < numAcked {
		return fmt.Errorf("pubsublite: server acknowledged %d cursor commits, but only %d were sent", numAcked, numPending)
	}

	for i := int64(0); i < numAcked; i++ {
		front := ct.pendingOffsets.Front()
		ct.lastConfirmedOffset = extractOffsetFromElem(front)
		ct.pendingOffsets.Remove(front)
	}
	fmt.Printf("Last confirmed offset: %d\n", ct.lastConfirmedOffset)
	return nil
}

// Done when there are no more unacknowledged offsets.
func (ct *committedCursorTracker) Done() bool {
	return ct.pendingOffsets.Len() == 0
}

type committer struct {
	// Immutable after creation.
	cursorClient *vkit.CursorClient
	subscription SubscriptionPath
	partition    int
	initialReq   *pb.StreamingCommitCursorRequest

	// Fields below must be guarded with mutex.
	stream *retryableStream

	pollCommitsTicker *time.Ticker
	stopPolling       chan struct{}
	cursorTracker     *committedCursorTracker

	abstractService
}

func newCursorClient(ctx context.Context, region string, opts ...option.ClientOption) (*vkit.CursorClient, error) {
	if err := validateRegion(region); err != nil {
		return nil, err
	}
	options := append(defaultClientOptions(region), opts...)
	return vkit.NewCursorClient(ctx, options...)
}

func newCommitter(ctx context.Context, cursor *vkit.CursorClient, subs SubscriptionPath, partition int, onStatusChange serviceStatusChangeFunc, acks *ackTracker) *committer {
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
		pollCommitsTicker: time.NewTicker(commitCursorPeriod),
		stopPolling:       make(chan struct{}),
		cursorTracker:     newCommittedCursorTracker(acks),
	}
	// TODO: the timeout should be from ReceiveSettings.
	commit.stream = newRetryableStream(ctx, commit, time.Minute, reflect.TypeOf(pb.StreamingCommitCursorResponse{}))
	commit.init(onStatusChange)
	return commit
}

func (c *committer) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stream.Start()
	go c.pollCommits()
}

func (c *committer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unsafeInitiateShutdown(serviceTerminating, nil)
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
	c.mu.Lock()
	defer c.mu.Unlock()

	switch status {
	case streamConnected:
		c.unsafeUpdateStatus(serviceActive, nil)
		c.unsafeCommitOffsetToStream()
		c.pollCommitsTicker.Reset(commitCursorPeriod)

	case streamReconnecting:
		c.cursorTracker.ClearPending()
		c.pollCommitsTicker.Stop()

	case streamTerminated:
		c.unsafeInitiateShutdown(serviceTerminated, c.stream.Error())
	}
}

func (c *committer) commitOffsetToStream() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unsafeCommitOffsetToStream()
}

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
		fmt.Printf("Committed: %v\n", req)
		c.cursorTracker.AddPending(nextOffset)
	}
}

func (c *committer) onResponse(response interface{}) {
	fmt.Printf("Response: %v\n", response)
	c.mu.Lock()
	defer c.mu.Unlock()

	processResponse := func() error {
		commitResponse, _ := response.(*pb.StreamingCommitCursorResponse)
		if commitResponse.GetCommit() == nil {
			return errInvalidCommitResponse
		}
		numAcked := commitResponse.GetCommit().GetAcknowledgedCommits()
		if numAcked <= 0 {
			return fmt.Errorf("pubsublite: server acknowledged an invalid commit count: %d", numAcked)
		}
		c.cursorTracker.AcknowledgeOffsets(numAcked)
		c.unsafeCheckDone()
		return nil
	}
	if err := processResponse(); err != nil {
		c.unsafeInitiateShutdown(serviceTerminated, err)
	}
}

// pollCommits executes in a goroutine to periodically commit cursors to the
// stream.
func (c *committer) pollCommits() {
	fmt.Printf("Started: %v\n", time.Now())
	for {
		select {
		case <-c.stopPolling:
			fmt.Printf("Stopped: %v\n", time.Now())
			return
		case <-c.pollCommitsTicker.C:
			c.commitOffsetToStream()
		}
	}
}

func (c *committer) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	if !c.unsafeUpdateStatus(targetStatus, err) {
		return
	}

	// Stop pollCommits to prevent more in-flight commits added to the stream.
	c.pollCommitsTicker.Stop()

	if targetStatus == serviceTerminating {
		// Try sending final commit to the stream.
		c.unsafeCommitOffsetToStream()
		c.unsafeCheckDone()
		return
	}

	// Otherwise immediately terminate the stream.
	close(c.stopPolling)
	c.stream.Stop()
}

func (c *committer) unsafeCheckDone() {
	if c.status == serviceTerminating && c.cursorTracker.Done() {
		c.stream.Stop()
	}
}
