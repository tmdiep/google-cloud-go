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

func newTestPartitionCountWatcher(t *testing.T, topicPath string, settings PublishSettings, receiver partitionCountReceiver) *partitionCountWatcher {
	ctx := context.Background()
	adminClient, err := NewAdminClient(ctx, "ignored", testClientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	return newPartitionCountWatcher(ctx, adminClient, testPublishSettings(), topicPath, receiver)
}

func TestPartitionCountWatcherRetries(t *testing.T) {
	const topic = "projects/123456/locations/us-central1-b/topics/my-topic"
	wantPartitionCount := 2

	verifiers := test.NewVerifiers(t)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), nil, status.Error(codes.Unavailable, "retryable"))
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), nil, status.Error(codes.ResourceExhausted, "retryable"))
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), topicPartitionsResp(wantPartitionCount), nil)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	receiveCallCount := 0
	receiver := func(gotPartitionCount int, err error) {
		receiveCallCount++
		if err != nil {
			t.Errorf("UpdatePartitionCount() got err: %v", err)
		}
		if gotPartitionCount != wantPartitionCount {
			t.Errorf("UpdatePartitionCount() got count: %d, want: %d", gotPartitionCount, wantPartitionCount)
		}
	}

	partitionWatcher := newTestPartitionCountWatcher(t, topic, testPublishSettings(), receiver)
	partitionWatcher.UpdatePartitionCount()

	if got, want := receiveCallCount, 1; got != want {
		t.Errorf("receiveCallCount: got %d, want %d", got, want)
	}
}
