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

package wire

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

// TODO: Expedite commits?

var (
	errInvalidInitialCommitResponse = errors.New("pubsublite: first response from server was not an initial response for streaming commit")
	errInvalidCommitResponse        = errors.New("pubsublite: received invalid commit response from server")
)

// The frequency of cursor commits.
const commitCursorPeriod = 50 * time.Millisecond

type committer struct {
	// Immutable after creation.
	cursorClient *vkit.CursorClient
	initialReq   *pb.StreamingCommitCursorRequest

	// Fields below must be guarded with mutex.
	stream *retryableStream

	cursorTracker *committedCursorTracker
	pollCommits   periodicTask

	abstractService
}

func newCommitter(ctx context.Context, cursor *vkit.CursorClient, settings ReceiveSettings, subscription subscriptionPartition, acks *ackTracker) *committer {
	commit := &committer{
		cursorClient: cursor,
		initialReq: &pb.StreamingCommitCursorRequest{
			Request: &pb.StreamingCommitCursorRequest_Initial{
				Initial: &pb.InitialCommitCursorRequest{
					Subscription: subscription.path,
					Partition:    int64(subscription.partition),
				},
			},
		},
		cursorTracker: newCommittedCursorTracker(acks),
	}
	commit.stream = newRetryableStream(ctx, commit, settings.Timeout, reflect.TypeOf(pb.StreamingCommitCursorResponse{}))
	return commit
}

func (c *committer) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stream.Start()
	c.pollCommits.Start(commitCursorPeriod, c.commitOffsetToStream)
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
		c.pollCommits.Resume()

	case streamReconnecting:
		c.cursorTracker.ClearPending()
		c.pollCommits.Pause()

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
		c.cursorTracker.AddPending(nextOffset)
	}
}

func (c *committer) onResponse(response interface{}) {
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

func (c *committer) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	if !c.unsafeUpdateStatus(targetStatus, err) {
		return
	}

	// Stop pollCommits to prevent more in-flight commits added to the stream.
	c.pollCommits.Pause()

	if targetStatus == serviceTerminating {
		// Try sending final commit to the stream.
		c.unsafeCommitOffsetToStream()
		c.unsafeCheckDone()
		return
	}

	// Otherwise immediately terminate the stream.
	c.pollCommits.Stop()
	c.stream.Stop()
}

func (c *committer) unsafeCheckDone() {
	if c.status == serviceTerminating && c.cursorTracker.Done() {
		c.stream.Stop()
	}
}
