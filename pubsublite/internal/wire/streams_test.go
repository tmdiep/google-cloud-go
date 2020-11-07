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
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/pubsublite/internal/test"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

const defaultStreamConnectTimeout = 30 * time.Second

var invalidInitialResponseErr = errors.New("invalid initial response")

// testStreamHandler is a mock implementation of a parent service that owns a
// retryableStream instance.
type testStreamHandler struct {
	Topic      topicPartition
	InitialReq *pb.PublishRequest
	Stream     *retryableStream

	t         *testing.T
	statuses  chan streamStatus
	pubClient *vkit.PublisherClient
}

func newTestStreamHandler(t *testing.T, timeout time.Duration) *testStreamHandler {
	ctx := context.Background()
	pubClient, err := newPublisherClient(ctx, "ignored", testClientOpts...)
	if err != nil {
		t.Fatal(err)
	}

	topic := topicPartition{Path: "path/to/topic", Partition: 1}
	sh := &testStreamHandler{
		Topic:      topic,
		InitialReq: initPubReq(topic),
		t:          t,
		statuses:   make(chan streamStatus, 3),
		pubClient:  pubClient,
	}
	sh.Stream = newRetryableStream(ctx, sh, timeout, reflect.TypeOf(pb.PublishResponse{}))
	return sh
}

func (sh *testStreamHandler) NextStatus() streamStatus {
	select {
	case status := <-sh.statuses:
		return status
	case <-time.After(defaultStreamConnectTimeout):
		sh.t.Errorf("Stream did not change state within %v", defaultStreamConnectTimeout)
		return streamUninitialized
	}
}

func (sh *testStreamHandler) newStream(ctx context.Context) (grpc.ClientStream, error) {
	return sh.pubClient.Publish(ctx)
}

func (sh *testStreamHandler) validateInitialResponse(response interface{}) error {
	pubResponse, _ := response.(*pb.PublishResponse)
	if pubResponse.GetInitialResponse() == nil {
		return invalidInitialResponseErr
	}
	return nil
}

func (sh *testStreamHandler) initialRequest() interface{} {
	return sh.InitialReq
}

func (sh *testStreamHandler) onStreamStatusChange(status streamStatus) {
	sh.statuses <- status
}

func (sh *testStreamHandler) onResponse(_ interface{}) {}

func TestRetryableStreamStartOnce(t *testing.T) {
	parent := newTestStreamHandler(t, defaultStreamConnectTimeout)

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(parent.InitialReq, initPubResp(), nil)
	mockServer.AddPublishStream(parent.Topic.Path, parent.Topic.Partition, stream)

	// Ensure that new streams are not opened if the publisher is started twice.
	// Note: only 1 stream verifier was added to the mock server above.
	parent.Stream.Start()
	parent.Stream.Start()
	if got, want := parent.NextStatus(), streamReconnecting; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}
	if got, want := parent.NextStatus(), streamConnected; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}

	parent.Stream.Stop()
	if got, want := parent.NextStatus(), streamTerminated; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}
	if gotErr := parent.Stream.Error(); gotErr != nil {
		t.Errorf("Stream final err: (%v), want: <nil>", gotErr)
	}
}

func TestRetryableStreamStopWhileConnecting(t *testing.T) {
	parent := newTestStreamHandler(t, defaultStreamConnectTimeout)

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	block := stream.PushWithBlock(parent.InitialReq, initPubResp(), nil)
	mockServer.AddPublishStream(parent.Topic.Path, parent.Topic.Partition, stream)

	parent.Stream.Start()
	if got, want := parent.NextStatus(), streamReconnecting; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}

	block.Release()
	parent.Stream.Stop()

	// The stream should transition to terminated and the client stream should be
	// discarded.
	if got, want := parent.NextStatus(), streamTerminated; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}
	if parent.Stream.currentStream() != nil {
		t.Error("Client stream should be nil")
	}
	if gotErr := parent.Stream.Error(); gotErr != nil {
		t.Errorf("Stream final err: (%v), want: <nil>", gotErr)
	}
}

func TestRetryableStreamStopAbortsRetries(t *testing.T) {
	parent := newTestStreamHandler(t, defaultStreamConnectTimeout)

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// Aborted is a retryable error, but the stream should not be retried because
	// the publisher is stopped.
	stream := test.NewRPCVerifier(t)
	block := stream.PushWithBlock(parent.InitialReq, nil, status.Error(codes.Aborted, "abort retry"))
	mockServer.AddPublishStream(parent.Topic.Path, parent.Topic.Partition, stream)

	parent.Stream.Start()
	if got, want := parent.NextStatus(), streamReconnecting; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}

	block.Release()
	parent.Stream.Stop()

	// The stream should transition to terminated and the client stream should be
	// discarded.
	if got, want := parent.NextStatus(), streamTerminated; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}
	if parent.Stream.currentStream() != nil {
		t.Error("Client stream should be nil")
	}
	if gotErr := parent.Stream.Error(); gotErr != nil {
		t.Errorf("Stream final err: (%v), want: <nil>", gotErr)
	}
}

func TestRetryableStreamConnectRetries(t *testing.T) {
	parent := newTestStreamHandler(t, defaultStreamConnectTimeout)

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// First 2 errors are retryable.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(parent.InitialReq, nil, status.Error(codes.Unavailable, "server unavailable"))
	mockServer.AddPublishStream(parent.Topic.Path, parent.Topic.Partition, stream1)

	stream2 := test.NewRPCVerifier(t)
	stream2.Push(parent.InitialReq, nil, status.Error(codes.Aborted, "aborted"))
	mockServer.AddPublishStream(parent.Topic.Path, parent.Topic.Partition, stream2)

	// Third stream should succeed.
	stream3 := test.NewRPCVerifier(t)
	stream3.Push(parent.InitialReq, initPubResp(), nil)
	mockServer.AddPublishStream(parent.Topic.Path, parent.Topic.Partition, stream3)

	parent.Stream.Start()
	if got, want := parent.NextStatus(), streamReconnecting; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}
	if got, want := parent.NextStatus(), streamConnected; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}

	parent.Stream.Stop()
	if got, want := parent.NextStatus(), streamTerminated; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}
}

func TestRetryableStreamConnectPermanentFailure(t *testing.T) {
	parent := newTestStreamHandler(t, defaultStreamConnectTimeout)
	permErr := status.Error(codes.PermissionDenied, "denied")

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// The stream connection results in a non-retryable error, so the publisher
	// cannot start.
	stream := test.NewRPCVerifier(t)
	stream.Push(parent.InitialReq, nil, permErr)
	mockServer.AddPublishStream(parent.Topic.Path, parent.Topic.Partition, stream)

	parent.Stream.Start()
	if got, want := parent.NextStatus(), streamReconnecting; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}
	if got, want := parent.NextStatus(), streamTerminated; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}
	if parent.Stream.currentStream() != nil {
		t.Error("Client stream should be nil")
	}
	if gotErr := parent.Stream.Error(); !test.ErrorEqual(gotErr, permErr) {
		t.Errorf("Stream final err: (%v), want: (%v)", gotErr, permErr)
	}
}

func TestRetryableStreamConnectTimeout(t *testing.T) {
	// Set a very low timeout to ensure no retries.
	parent := newTestStreamHandler(t, time.Millisecond)
	wantErr := status.Error(codes.DeadlineExceeded, "too slow")

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	block := stream.PushWithBlock(parent.InitialReq, nil, wantErr)
	mockServer.AddPublishStream(parent.Topic.Path, parent.Topic.Partition, stream)

	parent.Stream.Start()
	if got, want := parent.NextStatus(), streamReconnecting; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}

	// Send the initial server response well after the timeout setting.
	time.Sleep(10 * time.Millisecond)
	block.Release()

	if got, want := parent.NextStatus(), streamTerminated; got != want {
		t.Errorf("Stream status change: got %d, want %d", got, want)
	}
	if parent.Stream.currentStream() != nil {
		t.Error("Client stream should be nil")
	}
	if gotErr := parent.Stream.Error(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Stream final err: (%v), want: (%v)", gotErr, wantErr)
	}
}
