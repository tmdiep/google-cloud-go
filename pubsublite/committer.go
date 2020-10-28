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
	"sync"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errOutOfOrderMessages = errors.New("pubsublite: messages are out of order")
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
		ackedPrefixOffset: -1,
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
	at.ackedPrefixOffset = -1
}

// AckedPrefix indicates that all message offsets before and including this
// prefix have been acked by the user.
func (at *ackTracker) AckedPrefix() int64 {
	at.mu.Lock()
	defer at.mu.Unlock()
	return at.ackedPrefixOffset
}

// Push adds an outstanding ack to the tracker.
func (at *ackTracker) Push(ack *ackHandler) error {
	at.mu.Lock()
	defer at.mu.Unlock()

	// These errors should not occurr unless there is a bug in the client library.
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

// Pop processes outstanding acks, updates and returns AckedPrefix().
func (at *ackTracker) Pop() int64 {
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
	return at.ackedPrefixOffset
}

type committer struct {
	// Immutable after creation.
	cursorClient *vkit.CursorClient
	subscription SubscriptionPath
	partition    int
	initialReq   *pb.StreamingCommitCursorRequest
	//onStatusChange publisherStatusChangeFunc

	// Guards access to fields below.
	mu sync.Mutex

	stream *retryableStream
	//status   publisherStatus
	finalErr error

	acks *ackTracker
}
