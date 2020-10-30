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
	"errors"
	"fmt"
	"sync"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errServerOutOfOrderMessages     = errors.New("pubsublite: server delivered messages out of order")
	errOutOfOrderMessages           = errors.New("pubsublite: messages are out of order")
	errTokenCounterDeltaNegative    = errors.New("pubsublite: flow control delta must not be negative")
	errTokenCounterBytesNegative    = errors.New("pubsublite: received messages that account for more bytes than were requested")
	errTokenCounterMessagesNegative = errors.New("pubsublite: received more messages than were requested")
)

// Represents an uninitialized cursor offset.
const nilCursorOffset int64 = -1

// ackedFunc is invoked when a message has been acked by the user.
type ackedFunc func(*ackReplyConsumer)

// ackReplyConsumer is used for handling message acks. It is attached to a
// Message and also stored within the subscriber client for tracking until the
// message has been acked by the server.
type ackReplyConsumer struct {
	// The message offset.
	Offset int64
	// Bytes released to the flow controller once the message has been acked.
	MsgBytes int64

	// Guards access to fields below.
	mu    sync.Mutex
	acked bool
	onAck ackedFunc
}

func newAckReplyConsumer(offset, msgBytes int64, onAck ackedFunc) *ackReplyConsumer {
	return &ackReplyConsumer{Offset: offset, MsgBytes: msgBytes, onAck: onAck}
}

func (ar *ackReplyConsumer) Ack() {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	if ar.acked {
		return
	}
	ar.acked = true
	if ar.onAck != nil {
		// Don't block the user's goroutine with potentially expensive ack
		// processing.
		go ar.onAck(ar)
	}
}

func (ar *ackReplyConsumer) IsAcked() bool {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	return ar.acked
}

// Clear onAck when the ack is obsolete.
func (ar *ackReplyConsumer) Clear() {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.onAck = nil
}

// ackTracker manages outstanding message acks, i.e. messages that have been
// delivered to the user, but not yet acked. It is used by the committer and
// partitionSubscriber, so requires a mutex.
type ackTracker struct {
	// Guards access to fields below.
	mu sync.Mutex

	// All offsets before and including this prefix have been acked by the user.
	ackedPrefixOffset int64
	// Outstanding message acks, strictly ordered by increasing message offsets.
	outstandingAcks *list.List // Value = *ackReplyConsumer
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
		ack, _ := elem.Value.(*ackReplyConsumer)
		ack.Clear()
	}
	at.outstandingAcks.Init()
	at.ackedPrefixOffset = nilCursorOffset
}

// Push adds an outstanding ack to the tracker.
func (at *ackTracker) Push(ack *ackReplyConsumer) error {
	at.mu.Lock()
	defer at.mu.Unlock()

	// These errors should not occur unless there is a bug in the client library.
	if ack.Offset <= at.ackedPrefixOffset {
		return errOutOfOrderMessages
	}
	if elem := at.outstandingAcks.Back(); elem != nil {
		lastOutstandingAck, _ := elem.Value.(*ackReplyConsumer)
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
		ack, _ := elem.Value.(*ackReplyConsumer)
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
// It is only used by the committer.
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
	fmt.Printf("LastConfirmedOffset: %d\n", ct.lastConfirmedOffset)
	return nil
}

// Done when there are no more unacknowledged offsets.
func (ct *committedCursorTracker) Done() bool {
	return ct.pendingOffsets.Len() == 0
}

// subscriberOffsetTracker tracks the offset of the last message received from
// the server and ensures messages are delivered in order. It is only used by
// the wireSubscriber.
type subscriberOffsetTracker struct {
	lastOffset int64
}

func newSubscriberOffsetTracker() *subscriberOffsetTracker {
	return &subscriberOffsetTracker{lastOffset: nilCursorOffset}
}

// Returns the seek request to send when a new subscribe stream reconnects. May
// be nil.
func (ot *subscriberOffsetTracker) RequestForRestart() *pb.SeekRequest {
	if ot.lastOffset == nilCursorOffset {
		return nil
	}
	return &pb.SeekRequest{
		Target: &pb.SeekRequest_Cursor{
			Cursor: &pb.Cursor{Offset: ot.lastOffset + 1},
		},
	}
}

func (ot *subscriberOffsetTracker) Update(msgs []*pb.SequencedMessage) error {
	last := ot.lastOffset
	for i, msg := range msgs {
		offset := msg.GetCursor().GetOffset()
		if offset <= last {
			if i == 0 {
				return fmt.Errorf("pubsublite: server delivered messages with start offset = %d, expected >= %d", offset, ot.lastOffset+1)
			}
			return errServerOutOfOrderMessages
		}
		last = offset
	}
	return nil
}

type tokenCounter struct {
	Bytes    int64
	Messages int64
}

func (tc *tokenCounter) Add(deltaBytes, deltaMsgs int64) error {
	if deltaBytes < 0 || deltaMsgs < 0 {
		// This should not occurr.
		return errTokenCounterDeltaNegative
	}
	// TODO: handle overflow?
	tc.Bytes += deltaBytes
	tc.Messages += deltaMsgs
	return nil
}

func (tc *tokenCounter) Sub(deltaBytes, deltaMsgs int64) error {
	if deltaBytes < 0 || deltaMsgs < 0 {
		// This should not occur.
		return errTokenCounterDeltaNegative
	}
	if deltaBytes > tc.Bytes {
		return errTokenCounterBytesNegative
	}
	if deltaMsgs > tc.Messages {
		return errTokenCounterMessagesNegative
	}
	tc.Bytes -= deltaBytes
	tc.Messages -= deltaMsgs
	return nil
}

func (tc *tokenCounter) Reset() {
	tc.Bytes = 0
	tc.Messages = 0
}

func (tc *tokenCounter) ToFlowControlRequest() *pb.FlowControlRequest {
	return &pb.FlowControlRequest{
		AllowedBytes:    tc.Bytes,
		AllowedMessages: tc.Messages,
	}
}

type flowControlBatcher struct {
	// The current amount of outstanding byte and message flow control tokens.
	clientTokens tokenCounter
	// The pending aggregate flow control request that needs to be sent to the
	// stream.
	pendingTokens tokenCounter
}
