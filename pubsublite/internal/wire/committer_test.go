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
	"testing"

	"cloud.google.com/go/pubsublite/internal/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// testCommitter wraps a commiter for ease of testing.
type testCommitter struct {
	cmt *committer
	serviceTestProxy
}

func newTestCommitter(t *testing.T, subscription subscriptionPartition, acks *ackTracker) *testCommitter {
	ctx := context.Background()
	cursorClient, err := newCursorClient(ctx, "ignored", testClientOpts...)
	if err != nil {
		t.Fatal(err)
	}

	tc := &testCommitter{
		cmt: newCommitter(ctx, cursorClient, defaultTestReceiveSettings, subscription, acks, true),
	}
	tc.initAndStart(t, tc.cmt, "Committer")
	return tc
}

// SendBatchCommit invokes the periodic background batch commit. Note that the
// periodic task is disabled in tests.
func (tc *testCommitter) SendBatchCommit() {
	tc.cmt.commitOffsetToStream()
}

func TestCommitterStreamReconnect(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	ack1 := newAckConsumer(33, 0, nil)
	ack2 := newAckConsumer(55, 0, nil)

	acks := newAckTracker()
	acks.Push(ack1)
	acks.Push(ack2)

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// Simulate a transient error that results in a reconnect.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initCommitReq(subscription), initCommitResp(), nil)
	block := stream1.PushWithBlock(commitReq(34), nil, status.Error(codes.Unavailable, "server unavailable"))
	mockServer.AddCommitStream(subscription.Path, subscription.Partition, stream1)

	// When the stream reconnects, the latest commit offset should be sent to the
	// server.
	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initCommitReq(subscription), initCommitResp(), nil)
	stream2.Push(commitReq(56), commitResp(1), nil)
	mockServer.AddCommitStream(subscription.Path, subscription.Partition, stream2)

	cmt := newTestCommitter(t, subscription, acks)
	defer cmt.StopVerifyNoError()
	if gotErr := cmt.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	// Send 2 commits.
	ack1.Ack()
	cmt.SendBatchCommit()
	ack2.Ack()
	cmt.SendBatchCommit()

	// Then send the retryable error, which results in reconnect.
	block.Release()
}

// To test:
// Stop flushes commits
// Invalid server respondes
