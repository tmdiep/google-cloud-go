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
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"google.golang.org/api/option"
	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errInvalidInitialCommitResponse = errors.New("pubsublite: first response from server was not an initial response for streaming commit")
	errInvalidCommitResponse        = errors.New("pubsublite: received invalid commit response from server")
)

// The frequency of cursor commits.
const commitCursorPeriod = 50 * time.Millisecond

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
