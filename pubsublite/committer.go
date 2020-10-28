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
)

// ackedFunc is invoked when a message has been acked by the user.
type ackedFunc func(*ackHandler)

// ackHandler is used for handling message acks. It is attached to a Message and
// also stored in the subscriber client for tracking until the message has been
// acked.
type ackHandler struct {
	// Immutable after creation.
	// Offset is the message offset.
	Offset int64
	// MsgSize is released to the flow controller once the message has been acked.
	MsgSize int64

	// Guards access to fields below.
	mu    sync.Mutex
	acked bool
	onAck ackedFunc
}

func newAckHandler(offset, msgSize int64, onAck ackedFunc) *ackHandler {
	return &ackHandler{Offset: offset, MsgSize: msgSize, onAck: onAck}
}

func (ah *ackHandler) Ack() {
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

func (ah *ackHandler) Acked() bool {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	return ah.acked
}

// Clear is called when the ack is obsolete. The ackedFunc is cleared, which
// renders any future acks ineffective.
func (ah *ackHandler) Clear() {
	ah.mu.Lock()
	defer ah.mu.Unlock()
	ah.onAck = nil
}

type ackTracker struct {
	// Guards access to fields below.
	mu sync.Mutex

	// All offsets before and including this prefix have been acked by the user.
	ackedPrefixOffset int64

	// Outstanding message acks, strictly ordered by increasing message offsets.
	outstandingAcks *list.List // Value = *ackHandler
}

func newAckTracker() *ackTracker {
	return &ackTracker{
		ackedPrefixOffset: nilCursorOffset,
		outstandingAcks:   list.New(),
	}
}

// Clear resets the ackTracker back to its initial state. Any remaining
// outstanding acks are considered to be obsolete.
func (at *ackTracker) Clear() {
	at.mu.Lock()
	defer at.mu.Unlock()

	for elem := at.outstandingAcks.Front(); elem != nil; elem = elem.Next() {
		ack, _ := elem.Value.(*ackHandler)
		ack.Clear()
	}
	at.outstandingAcks.Init()
	at.ackedPrefixOffset = nilCursorOffset
}

// Push adds an outstanding ack to the tracker.
func (at *ackTracker) Push(ack *ackHandler) error {
	at.mu.Lock()
	defer at.mu.Unlock()

	// These errors should not occur unless there is a bug in the client library.
	if ack.Offset <= at.ackedPrefixOffset {
		return errOutOfOrderMessages
	}
	if elem := at.outstandingAcks.Back(); elem != nil {
		lastOutstandingAck, _ := elem.Value.(*ackHandler)
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
		ack, _ := elem.Value.(*ackHandler)
		if !ack.Acked() {
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
	// Convert from last acked to first unacked.
	return at.ackedPrefixOffset + 1
}

type committer struct {
	// Immutable after creation.
	cursorClient *vkit.CursorClient
	subscription SubscriptionPath
	partition    int
	initialReq   *pb.StreamingCommitCursorRequest
	onEvent      serviceEventFunc

	// Guards access to fields below.
	mu sync.Mutex

	stream *retryableStream
	//status   publisherStatus
	finalErr error

	acks *ackTracker

	// Last offset sent to the server. May not be acknowledged.
	lastSentOffset int64
	// Last offset for which the server acknowledged the commit.
	lastConfirmedOffset int64
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
		onEvent:             onEvent,
		acks:                acks,
		lastSentOffset:      nilCursorOffset,
		lastConfirmedOffset: nilCursorOffset,
	}
	// TODO: set timeout from settings
	commit.stream = newRetryableStream(ctx, commit, time.Minute, reflect.TypeOf(pb.StreamingCommitCursorResponse{}))
	return commit
}

func (c *committer) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stream.Start()
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
}

func (c *committer) onResponse(response interface{}) {
}
