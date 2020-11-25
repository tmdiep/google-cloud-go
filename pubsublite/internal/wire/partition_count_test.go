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

type testCountReceiver struct {
	callCount      int
	partitionCount int
	err            error
}

func newTestCountReceiver() *testCountReceiver {
	return &testCountReceiver{}
}

func (r *testCountReceiver) onCountChanged(partitionCount int, err error) {
	r.callCount++
	r.partitionCount = partitionCount
	r.err = err
}

func (r *testCountReceiver) VerifyCalled(t *testing.T, wantCallCount int) {
	if r.callCount != wantCallCount {
		t.Errorf("testCountReceiver.callCount: got %d, want %d", r.callCount, wantCallCount)
	}
}

func (r *testCountReceiver) VerifyCount(t *testing.T, wantPartitionCount int) {
	if r.err != nil {
		t.Errorf("testCountReceiver.err: got %d, want <nil>", r.err)
	}
	if r.partitionCount != wantPartitionCount {
		t.Errorf("testCountReceiver.partitionCount: got %d, want %d", r.partitionCount, wantPartitionCount)
	}
}

func (r *testCountReceiver) VerifyErrorMsg(t *testing.T, wantMsg string) {
	if !test.ErrorHasMsg(r.err, wantMsg) {
		t.Errorf("testCountReceiver.err: got %d, want msg %q", r.err, wantMsg)
	}
}

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

	receiver := newTestCountReceiver()
	partitionWatcher := newTestPartitionCountWatcher(t, topic, testPublishSettings(), receiver.onCountChanged)
	partitionWatcher.UpdatePartitionCount()

	receiver.VerifyCalled(t, 1)
	receiver.VerifyCount(t, wantPartitionCount)
}

func TestPartitionCountWatcherZeroPartitionCountFails(t *testing.T) {
	const topic = "projects/123456/locations/us-central1-b/topics/my-topic"

	verifiers := test.NewVerifiers(t)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), topicPartitionsResp(0), nil)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	receiver := newTestCountReceiver()
	partitionWatcher := newTestPartitionCountWatcher(t, topic, testPublishSettings(), receiver.onCountChanged)
	partitionWatcher.UpdatePartitionCount()

	receiver.VerifyCalled(t, 1)
	receiver.VerifyErrorMsg(t, "invalid number of partitions 0")
}

func TestPartitionCountWatcherPartitionCountUnchanged(t *testing.T) {
	const topic = "projects/123456/locations/us-central1-b/topics/my-topic"
	wantPartitionCount := 6

	verifiers := test.NewVerifiers(t)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), topicPartitionsResp(wantPartitionCount), nil)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), topicPartitionsResp(wantPartitionCount), nil)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	receiver := newTestCountReceiver()
	partitionWatcher := newTestPartitionCountWatcher(t, topic, testPublishSettings(), receiver.onCountChanged)
	partitionWatcher.UpdatePartitionCount()
	partitionWatcher.UpdatePartitionCount()

	receiver.VerifyCalled(t, 1)
	receiver.VerifyCount(t, wantPartitionCount)
}
