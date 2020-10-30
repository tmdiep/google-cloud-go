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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"cloud.google.com/go/pubsublite/internal/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

const (
	publisherWaitTimeout = 30 * time.Second
)

type publishResultReceiver struct {
	done   chan struct{}
	t      *testing.T
	got    *publishMetadata
	gotErr error
}

func newPublishResultReceiver(t *testing.T) *publishResultReceiver {
	return &publishResultReceiver{
		t:    t,
		done: make(chan struct{}),
	}
}

func (r *publishResultReceiver) set(pm *publishMetadata, err error) {
	r.got = pm
	r.gotErr = err
	close(r.done)
}

func (r *publishResultReceiver) wait() bool {
	select {
	case <-time.After(publisherWaitTimeout):
		r.t.Errorf("Publish result not available within %v", publisherWaitTimeout)
		return false
	case <-r.done:
		return true
	}
}

func (r *publishResultReceiver) validateResult(wantPartition int, wantOffset int64) {
	if !r.wait() {
		return
	}
	if r.gotErr != nil {
		r.t.Errorf("Publish() error: (%v), want: partition=%d,offset=%d", r.gotErr, wantPartition, wantOffset)
	} else if r.got.partition != wantPartition || r.got.offset != wantOffset {
		r.t.Errorf("Publish() got: partition=%d,offset=%d, want: partition=%d,offset=%d", r.got.partition, r.got.offset, wantPartition, wantOffset)
	}
}

func (r *publishResultReceiver) validateError(wantErr error) {
	if !r.wait() {
		return
	}
	if !test.ErrorEqual(r.gotErr, wantErr) {
		r.t.Errorf("Publish() error: (%v), want: (%v)", r.gotErr, wantErr)
	}
}

func (r *publishResultReceiver) validateErrorCode(wantCode codes.Code) {
	if !r.wait() {
		return
	}
	if !test.ErrorHasCode(r.gotErr, wantCode) {
		r.t.Errorf("Publish() error: (%v), want code: %v", r.gotErr, wantCode)
	}
}

func (r *publishResultReceiver) validateErrorMsg(wantStr string) {
	if !r.wait() {
		return
	}
	if !test.ErrorHasMsg(r.gotErr, wantStr) {
		r.t.Errorf("Publish() error: (%v), want msg: %q", r.gotErr, wantStr)
	}
}

// testPartitionPublisher wraps a singlePartitionPublisher for ease of testing.
type testPartitionPublisher struct {
	t          *testing.T
	pub        *singlePartitionPublisher
	started    chan struct{}
	terminated chan struct{}
}

func newTestPartitionPublisher(t *testing.T, topic TopicPath, partition int, settings PublishSettings) *testPartitionPublisher {
	ctx := context.Background()
	region, _ := ZoneToRegion(topic.Zone)
	pubClient, err := newPublisherClient(ctx, region, clientOpts...)
	if err != nil {
		t.Fatal(err)
	}

	tp := &testPartitionPublisher{
		t:          t,
		started:    make(chan struct{}),
		terminated: make(chan struct{}),
	}
	pub := newSinglePartitionPublisher(ctx, pubClient, settings, topic, partition)
	pub.addOnStatusChange(tp.onStatusChange)
	pub.Start()
	tp.pub = pub
	return tp
}

func (tp *testPartitionPublisher) publish(msg *pb.PubSubMessage) *publishResultReceiver {
	result := newPublishResultReceiver(tp.t)
	tp.pub.Publish(msg, result.set)
	return result
}

func (tp *testPartitionPublisher) onStatusChange(unused *abstractService, status serviceStatus, err error) {
	if status == serviceActive {
		close(tp.started)
	}
	if status == serviceTerminated {
		close(tp.terminated)
	}
}

func (tp *testPartitionPublisher) startError() error {
	select {
	case <-time.After(publisherWaitTimeout):
		return fmt.Errorf("Publisher did not start within %v", publisherWaitTimeout)
	case <-tp.terminated:
		return tp.pub.Error()
	case <-tp.started:
		return tp.pub.Error()
	}
}

func (tp *testPartitionPublisher) finalError() error {
	select {
	case <-time.After(publisherWaitTimeout):
		return fmt.Errorf("Publisher did not terminate within %v", publisherWaitTimeout)
	case <-tp.terminated:
		if gotStatus, wantStatus := tp.pub.stream.Status(), streamTerminated; gotStatus != wantStatus {
			tp.t.Errorf("Publisher retryableStream status: %v, want: %v", gotStatus, wantStatus)
		}
		if tp.pub.stream.currentStream() != nil {
			tp.t.Error("Publisher gRPC stream should be nil")
		}
		return tp.pub.Error()
	}
}

func (tp *testPartitionPublisher) stopVerifyFinalError() {
	tp.pub.Stop()
	if gotErr := tp.finalError(); gotErr != nil {
		tp.t.Errorf("Publisher final err: (%v), want: <nil>", gotErr)
	}
}

func initPubReq(topic TopicPath, partition int) *pb.PublishRequest {
	return &pb.PublishRequest{
		RequestType: &pb.PublishRequest_InitialRequest{
			InitialRequest: &pb.InitialPublishRequest{
				Topic:     topic.String(),
				Partition: int64(partition),
			},
		},
	}
}

func initPubResp() *pb.PublishResponse {
	return &pb.PublishResponse{
		ResponseType: &pb.PublishResponse_InitialResponse{
			InitialResponse: &pb.InitialPublishResponse{},
		},
	}
}

func msgPubReq(msgs ...*pb.PubSubMessage) *pb.PublishRequest {
	return &pb.PublishRequest{
		RequestType: &pb.PublishRequest_MessagePublishRequest{
			MessagePublishRequest: &pb.MessagePublishRequest{
				Messages: msgs,
			},
		},
	}
}

func msgPubResp(cursor int64) *pb.PublishResponse {
	return &pb.PublishResponse{
		ResponseType: &pb.PublishResponse_MessageResponse{
			MessageResponse: &pb.MessagePublishResponse{
				StartCursor: &pb.Cursor{
					Offset: cursor,
				},
			},
		},
	}
}

func TestPartitionPublisherStartOnce(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	defer pub.stopVerifyFinalError()

	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	// Ensure that new streams are not opened if the publisher is started twice.
	// Note: only 1 stream verifier was added to the mock server above.
	pub.pub.Start()
}

func TestPartitionPublisherStartStop(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	block := stream.PushWithBlock(initPubReq(topic, partition), initPubResp(), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)

	time.Sleep(10 * time.Millisecond)
	pub.pub.Stop()
	time.Sleep(10 * time.Millisecond)
	close(block)

	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v), want: <nil>", gotErr)
	}
	// pubFinalError also verifies that the gRPC stream is nil.
	if gotErr := pub.finalError(); gotErr != nil {
		t.Errorf("Publisher final err: (%v), want: <nil>", gotErr)
	}
	if gotErr := pub.pub.stream.Error(); gotErr != nil {
		t.Errorf("Stream final err: (%v), want: <nil>", gotErr)
	}
}

func TestPartitionPublisherStopAbortsRetries(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	// Unavailable is a retryable error, but the stream should not be retried
	// because the publisher is stopped.
	block := stream.PushWithBlock(initPubReq(topic, partition), initPubResp(), status.Error(codes.Unavailable, ""))
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)

	time.Sleep(10 * time.Millisecond)
	pub.pub.Stop()
	time.Sleep(10 * time.Millisecond)
	close(block)

	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v), want: <nil>", gotErr)
	}
	// pubFinalError also verifies that the gRPC stream is nil.
	if gotErr := pub.finalError(); gotErr != nil {
		t.Errorf("Publisher final err: (%v), want: <nil>", gotErr)
	}
	if gotErr := pub.pub.stream.Error(); gotErr != nil {
		t.Errorf("Stream final err: (%v), want: <nil>", gotErr)
	}
}

func TestPartitionPublisherConnectRetries(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// First 2 errors are retryable.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topic, partition), nil, status.Error(codes.Unavailable, "server unavailable"))
	mockServer.AddPublishStream(topic.String(), partition, stream1)

	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initPubReq(topic, partition), nil, status.Error(codes.Aborted, "aborted"))
	mockServer.AddPublishStream(topic.String(), partition, stream2)

	// Third stream should succeed.
	stream3 := test.NewRPCVerifier(t)
	stream3.Push(initPubReq(topic, partition), initPubResp(), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream3)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	defer pub.stopVerifyFinalError()

	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}
}

func TestPartitionPublisherConnectPermanentFailure(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	permErr := status.Error(codes.PermissionDenied, "denied")

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// The stream connection results in a non-retryable error, so the publisher
	// cannot start.
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), nil, permErr)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)

	if gotErr := pub.startError(); !test.ErrorEqual(gotErr, permErr) {
		t.Errorf("Start() got err: (%v), want: (%v)", gotErr, permErr)
	}
	if gotErr := pub.finalError(); !test.ErrorEqual(gotErr, permErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, permErr)
	}
}

func TestPartitionPublisherConnectTimeout(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	settings := defaultTestPublishSettings
	// Set a very low timeout to ensure no retries.
	settings.Timeout = time.Millisecond
	wantErr := status.Error(codes.DeadlineExceeded, "too slow")

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	block := stream.PushWithBlock(initPubReq(topic, partition), nil, wantErr)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, settings)

	// Send the initial server response well after settings.Timeout to simulate a
	// timeout. The publisher fails to start.
	time.Sleep(50 * time.Millisecond)
	close(block)

	if gotErr := pub.startError(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Start() got err: (%v), want: (%v)", gotErr, wantErr)
	}
	if gotErr := pub.finalError(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestPartitionPublisherInvalidInitialResponse(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// If the server sends an invalid initial response, the client treats this as
	// a permanent failure (bug on the server).
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), msgPubResp(0), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)

	wantErr := errInvalidInitialPubResponse
	if gotErr := pub.startError(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Start() got err: (%v), want: (%v)", gotErr, wantErr)
	}
	if gotErr := pub.finalError(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestPartitionPublisherSpuriousPublishResponse(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	// If the server has sent a MessagePublishResponse when no messages were
	// published, the client treats this as a permanent failure (bug on the
	// server).
	block := stream.PushWithBlock(nil, msgPubResp(0), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	// Send after startup to ensure the test is deterministic.
	close(block)

	if gotErr, wantErr := pub.finalError(), errPublishQueueEmpty; !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestPartitionPublisherBatching(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	settings := defaultTestPublishSettings
	settings.DelayThreshold = time.Minute // Batching delay disabled, tested elsewhere
	settings.CountThreshold = 3
	settings.ByteThreshold = 50

	// Batch 1: count threshold.
	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	// Batch 2: byte thresholds.
	msg4 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'4'}, 60)}

	// Batch 3: remainder.
	msg5 := &pb.PubSubMessage{Data: []byte{'5'}}
	msg6 := &pb.PubSubMessage{Data: []byte{'6'}}
	msg7 := &pb.PubSubMessage{Data: []byte{'7'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	stream.Push(msgPubReq(msg1, msg2, msg3), msgPubResp(0), nil)
	stream.Push(msgPubReq(msg4), msgPubResp(3), nil)
	stream.Push(msgPubReq(msg5, msg6, msg7), msgPubResp(45), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, settings)
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.publish(msg1)
	result2 := pub.publish(msg2)
	result3 := pub.publish(msg3)
	result4 := pub.publish(msg4)
	// Bundler invokes at most 1 handler and may add a message to the end of the
	// last bundle if the hard limits (BundleByteLimit) aren't reached. Pause to
	// handle previous bundles.
	time.Sleep(20 * time.Millisecond)
	result5 := pub.publish(msg5)
	result6 := pub.publish(msg6)
	result7 := pub.publish(msg7)
	// Stop flushes pending messages.
	pub.pub.Stop()

	result1.validateResult(partition, 0)
	result2.validateResult(partition, 1)
	result3.validateResult(partition, 2)
	result4.validateResult(partition, 3)
	result5.validateResult(partition, 45)
	result6.validateResult(partition, 46)
	result7.validateResult(partition, 47)

	if gotErr := pub.finalError(); gotErr != nil {
		t.Errorf("Publisher final err: (%v), want: <nil>", gotErr)
	}
}

func TestPartitionPublisherBatchingDelay(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	settings := defaultTestPublishSettings
	settings.CountThreshold = 100
	settings.DelayThreshold = 5 * time.Millisecond

	// Batch 1.
	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	// Batch 2.
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	stream.Push(msgPubReq(msg1), msgPubResp(0), nil)
	stream.Push(msgPubReq(msg2), msgPubResp(1), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, settings)
	defer pub.stopVerifyFinalError()
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.publish(msg1)
	time.Sleep(settings.DelayThreshold * 2)
	result2 := pub.publish(msg2)

	result1.validateResult(partition, 0)
	result2.validateResult(partition, 1)
}

func TestPartitionPublisherResendMessages(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// Simulate a transient error that results in a reconnect.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topic, partition), initPubResp(), nil)
	stream1.Push(msgPubReq(msg1), nil, status.Error(codes.Aborted, "server aborted"))
	mockServer.AddPublishStream(topic.String(), partition, stream1)

	// The publisher should re-send pending messages to the second stream.
	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initPubReq(topic, partition), initPubResp(), nil)
	stream2.Push(msgPubReq(msg1), msgPubResp(0), nil)
	stream2.Push(msgPubReq(msg2), msgPubResp(1), nil)
	stream2.Push(msgPubReq(msg3), msgPubResp(2), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream2)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	defer pub.stopVerifyFinalError()
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.publish(msg1)
	result2 := pub.publish(msg2)
	result1.validateResult(partition, 0)
	result2.validateResult(partition, 1)

	result3 := pub.publish(msg3)
	result3.validateResult(partition, 2)
}

func TestPartitionPublisherPublishPermanentError(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	permError := status.Error(codes.NotFound, "topic deleted")

	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// Simulate a permanent server error that terminates publishing.
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	stream.Push(msgPubReq(msg1), nil, permError)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.publish(msg1)
	result2 := pub.publish(msg2)
	result1.validateError(permError)
	result2.validateError(permError)

	// This message arrives after the publisher has already stopped, so its error
	// message is ErrServiceStopped.
	result3 := pub.publish(msg3)
	result3.validateError(ErrServiceStopped)

	if gotErr := pub.finalError(); !test.ErrorEqual(gotErr, permError) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, permError)
	}
}

func TestPartitionPublisherBufferOverflow(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	settings := defaultTestPublishSettings
	settings.BufferedByteLimit = 15

	msg1 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'1'}, 10)}
	msg2 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'2'}, 10)}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	block := stream.PushWithBlock(msgPubReq(msg1), msgPubResp(0), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, settings)
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.publish(msg1)
	// Overflow is detected, which terminates the publisher, but previous messages
	// are flushed.
	result2 := pub.publish(msg2)
	// Delay the server response for the first publish to ensure it is allowed to
	// complete.
	close(block)
	// This message arrives after the publisher has already stopped, so its error
	// message is ErrServiceStopped.
	result3 := pub.publish(msg3)

	result1.validateResult(partition, 0)
	result2.validateError(ErrOverflow)
	result3.validateError(ErrServiceStopped)

	if gotErr := pub.finalError(); !test.ErrorEqual(gotErr, ErrOverflow) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, ErrOverflow)
	}
}

func TestPartitionPublisherBufferRefill(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	settings := defaultTestPublishSettings
	settings.BufferedByteLimit = 15

	msg1 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'1'}, 10)}
	msg2 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'2'}, 8)}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	stream.Push(msgPubReq(msg1), msgPubResp(0), nil)
	stream.Push(msgPubReq(msg2), msgPubResp(1), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, settings)
	defer pub.stopVerifyFinalError()
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.publish(msg1)
	result1.validateResult(partition, 0)

	result2 := pub.publish(msg2)
	result2.validateResult(partition, 1)
}

func TestPartitionPublisherValidatesMaxMsgSize(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	msg1 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'1'}, 10)}
	msg2 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'2'}, MaxPublishMessageBytes+1)}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	block := stream.PushWithBlock(msgPubReq(msg1), msgPubResp(0), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.publish(msg1)
	// Fails due to over msg size limit, which terminates the publisher, but
	// pending messages are flushed.
	result2 := pub.publish(msg2)
	// Delay the server response for the first publish to ensure it is allowed to
	// complete.
	close(block)
	// This message arrives after the publisher has already stopped.
	result3 := pub.publish(msg3)

	wantErrMsg := "maximum allowed size is MaxPublishMessageBytes"
	result1.validateResult(partition, 0)
	result2.validateErrorMsg(wantErrMsg)
	result3.validateError(ErrServiceStopped)

	if gotErr := pub.finalError(); !test.ErrorHasMsg(gotErr, wantErrMsg) {
		t.Errorf("Publisher final err: (%v), want msg: %v", gotErr, wantErrMsg)
	}
}

func TestPartitionPublisherInvalidCursorOffsets(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	block := stream.PushWithBlock(msgPubReq(msg1), msgPubResp(4), nil)
	// The server returns an inconsistent cursor offset for msg2, which causes the
	// publisher to fail permanently (bug on the server).
	stream.Push(msgPubReq(msg2), msgPubResp(4), nil)
	stream.Push(msgPubReq(msg3), msgPubResp(5), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.publish(msg1)
	result2 := pub.publish(msg2)
	result3 := pub.publish(msg3)
	close(block)

	result1.validateResult(partition, 4)

	wantMsg := "server returned publish response with inconsistent start offset"
	result2.validateErrorMsg(wantMsg)
	result3.validateErrorMsg(wantMsg)
	if gotErr := pub.finalError(); !test.ErrorHasMsg(gotErr, wantMsg) {
		t.Errorf("Publisher final err: (%v), want msg: %q", gotErr, wantMsg)
	}
}

func TestPartitionPublisherInvalidServerPublishResponse(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	msg := &pb.PubSubMessage{Data: []byte{'1'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	// Server sends duplicate initial publish response, which causes the publisher
	// to fail permanently (bug on the server).
	stream.Push(msgPubReq(msg), initPubResp(), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result := pub.publish(msg)

	wantErr := errInvalidMsgPubResponse
	result.validateError(wantErr)
	if gotErr := pub.finalError(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestPartitionPublisherFlushMessages(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	finalErr := status.Error(codes.FailedPrecondition, "invalid message")

	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}
	msg4 := &pb.PubSubMessage{Data: []byte{'4'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	block := stream.PushWithBlock(msgPubReq(msg1), msgPubResp(5), nil)
	stream.Push(msgPubReq(msg2), msgPubResp(6), nil)
	stream.Push(msgPubReq(msg3), nil, finalErr)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pub.startError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.publish(msg1)
	result2 := pub.publish(msg2)
	result3 := pub.publish(msg3)
	pub.pub.Stop()
	close(block)
	result4 := pub.publish(msg4)

	// First 2 messages should be allowed to complete.
	result1.validateResult(partition, 5)
	result2.validateResult(partition, 6)
	// Third message failed with a server error, which should result in the
	// publisher terminating with an error.
	result3.validateError(finalErr)
	// Fourth message was sent after the user called Stop(), so should fail
	// immediately with ErrServiceStopped.
	result4.validateError(ErrServiceStopped)

	if gotErr := pub.finalError(); !test.ErrorEqual(gotErr, finalErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, finalErr)
	}
}

func newTestRoutingPublisher(t *testing.T, topic TopicPath, settings PublishSettings, fakeSourceVal int64) *routingPublisher {
	ctx := context.Background()
	source := &test.FakeSource{Ret: fakeSourceVal}
	msgRouter := &roundRobinMsgRouter{rng: rand.New(source)}
	pub, err := newRoutingPublisher(ctx, msgRouter, settings, topic, clientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	pub.Start()
	return pub
}

func topicPartitionsReq(topic TopicPath) *pb.GetTopicPartitionsRequest {
	return &pb.GetTopicPartitionsRequest{Name: topic.String()}
}

func topicPartitionsResp(count int) *pb.TopicPartitions {
	return &pb.TopicPartitions{PartitionCount: int64(count)}
}

func TestRoutingPublisherStartOnce(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	numPartitions := 2

	rpc := test.NewRPCVerifier(t)
	rpc.Push(topicPartitionsReq(topic), topicPartitionsResp(numPartitions), nil)

	mockServer.OnTestStart(rpc)
	defer mockServer.OnTestEnd()

	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topic, 0), initPubResp(), nil)
	mockServer.AddPublishStream(topic.String(), 0, stream1)

	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initPubReq(topic, 1), initPubResp(), nil)
	mockServer.AddPublishStream(topic.String(), 1, stream2)

	pub := newTestRoutingPublisher(t, topic, defaultTestPublishSettings, 0)
	defer pub.Stop()

	t.Run("First succeeds", func(t *testing.T) {
		// Note: newTestRoutingPublisher called Start.
		if gotErr := pub.WaitStarted(); gotErr != nil {
			t.Errorf("Start() got err: (%v)", gotErr)
		}
		if gotLen, wantLen := len(pub.publishers), numPartitions; gotLen != wantLen {
			t.Errorf("len(publishers) got %d, want %d", gotLen, wantLen)
		}
	})
	t.Run("Second no-op", func(t *testing.T) {
		// An error is not returned, but no new streams are opened.
		pub.Start()
		if gotErr := pub.WaitStarted(); gotErr != nil {
			t.Errorf("Start() got err: (%v)", gotErr)
		}
	})
}

func TestRoutingPublisherPartitionCountFail(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	wantErr := status.Error(codes.NotFound, "no exist")

	// Retrieving the number of partitions results in an error. Startup cannot
	// proceed.
	rpc := test.NewRPCVerifier(t)
	rpc.Push(topicPartitionsReq(topic), nil, wantErr)

	mockServer.OnTestStart(rpc)
	defer mockServer.OnTestEnd()

	pub := newTestRoutingPublisher(t, topic, defaultTestPublishSettings, 0)

	if gotErr := pub.WaitStarted(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Start() got err: (%v), want err: (%v)", gotErr, wantErr)
	}
	if gotLen, wantLen := len(pub.publishers), 0; gotLen != wantLen {
		t.Errorf("len(publishers) got %d, want %d", gotLen, wantLen)
	}
	// Ensure that the publisher does not attempt to restart. The mock server does
	// not expect more RPCs.
	pub.Start()
}

func TestRoutingPublisherPartitionCountInvalid(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}

	// The number of partitions returned by the server must be valid, otherwise
	// startup cannot proceed.
	rpc := test.NewRPCVerifier(t)
	rpc.Push(topicPartitionsReq(topic), topicPartitionsResp(0), nil)

	mockServer.OnTestStart(rpc)
	defer mockServer.OnTestEnd()

	pub := newTestRoutingPublisher(t, topic, defaultTestPublishSettings, 0)

	wantMsg := "topic has invalid number of partitions"
	if gotErr := pub.WaitStarted(); !test.ErrorHasMsg(gotErr, wantMsg) {
		t.Errorf("Start() got err: (%v), want msg: %q", gotErr, wantMsg)
	}
	if gotLen, wantLen := len(pub.publishers), 0; gotLen != wantLen {
		t.Errorf("len(publishers) got %d, want %d", gotLen, wantLen)
	}
	// Ensure that the publisher does not attempt to restart. The mock server does
	// not expect more RPCs.
	pub.Start()
}

func TestRoutingPublisherMultiPartitionRoundRobin(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	numPartitions := 3
	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}
	msg4 := &pb.PubSubMessage{Data: []byte{'4'}}

	rpc := test.NewRPCVerifier(t)
	rpc.Push(topicPartitionsReq(topic), topicPartitionsResp(numPartitions), nil)

	mockServer.OnTestStart(rpc)
	defer mockServer.OnTestEnd()

	// Partition 0.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topic, 0), initPubResp(), nil)
	stream1.Push(msgPubReq(msg3), msgPubResp(34), nil)
	mockServer.AddPublishStream(topic.String(), 0, stream1)

	// Partition 1.
	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initPubReq(topic, 1), initPubResp(), nil)
	stream2.Push(msgPubReq(msg1), msgPubResp(41), nil)
	stream2.Push(msgPubReq(msg4), msgPubResp(42), nil)
	mockServer.AddPublishStream(topic.String(), 1, stream2)

	// Partition 2.
	stream3 := test.NewRPCVerifier(t)
	stream3.Push(initPubReq(topic, 2), initPubResp(), nil)
	stream3.Push(msgPubReq(msg2), msgPubResp(78), nil)
	mockServer.AddPublishStream(topic.String(), 2, stream3)

	// Note: The fake source is initialized with value=1, so partition-1 publisher
	// will be the first chosen by the round robin message router.
	pub := newTestRoutingPublisher(t, topic, defaultTestPublishSettings, 1)
	if err := pub.WaitStarted(); err != nil {
		t.Errorf("Start() got err: (%v)", err)
	}
	result1 := newPublishResultReceiver(t)
	result2 := newPublishResultReceiver(t)
	result3 := newPublishResultReceiver(t)
	result4 := newPublishResultReceiver(t)

	pub.Publish(msg1, result1.set)
	pub.Publish(msg2, result2.set)
	pub.Publish(msg3, result3.set)
	pub.Publish(msg4, result4.set)
	pub.Stop()

	result1.validateResult(1, 41)
	result2.validateResult(2, 78)
	result3.validateResult(0, 34)
	result4.validateResult(1, 42)

	if err := pub.WaitStopped(); err != nil {
		t.Errorf("routingPublisher.Error() got: (%v), want: <nil>", err)
	}
}

func TestRoutingPublisherShutdown(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	numPartitions := 2
	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	serverErr := status.Error(codes.FailedPrecondition, "failed")

	rpc := test.NewRPCVerifier(t)
	rpc.Push(topicPartitionsReq(topic), topicPartitionsResp(numPartitions), nil)

	mockServer.OnTestStart(rpc)
	defer mockServer.OnTestEnd()

	// Partition 0.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topic, 0), initPubResp(), nil)
	stream1.Push(msgPubReq(msg1), msgPubResp(34), nil)
	mockServer.AddPublishStream(topic.String(), 0, stream1)

	// Partition 1. Fails due to permanent error, which will also shut down
	// partition-0 publisher, but it should be allowed to flush its pending
	// messages.
	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initPubReq(topic, 1), initPubResp(), nil)
	stream2.Push(msgPubReq(msg2), nil, serverErr)
	mockServer.AddPublishStream(topic.String(), 1, stream2)

	pub := newTestRoutingPublisher(t, topic, defaultTestPublishSettings, 0)
	if err := pub.WaitStarted(); err != nil {
		t.Errorf("Start() got err: (%v)", err)
	}

	result1 := newPublishResultReceiver(t)
	result2 := newPublishResultReceiver(t)

	pub.Publish(msg1, result1.set)
	pub.Publish(msg2, result2.set)

	result1.validateResult(0, 34)
	result2.validateError(serverErr)

	if gotErr := pub.WaitStopped(); !test.ErrorEqual(gotErr, serverErr) {
		t.Errorf("routingPublisher.Error() got: (%v), want: (%v)", gotErr, serverErr)
	}
}

func TestRoutingPublisherPublishAfterStop(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	numPartitions := 2
	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}

	rpc := test.NewRPCVerifier(t)
	rpc.Push(topicPartitionsReq(topic), topicPartitionsResp(numPartitions), nil)

	mockServer.OnTestStart(rpc)
	defer mockServer.OnTestEnd()

	// Partition 0.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topic, 0), initPubResp(), nil)
	mockServer.AddPublishStream(topic.String(), 0, stream1)

	// Partition 1.
	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initPubReq(topic, 1), initPubResp(), nil)
	mockServer.AddPublishStream(topic.String(), 1, stream2)

	pub := newTestRoutingPublisher(t, topic, defaultTestPublishSettings, 0)
	if err := pub.WaitStarted(); err != nil {
		t.Errorf("Start() got err: (%v)", err)
	}

	result1 := newPublishResultReceiver(t)
	result2 := newPublishResultReceiver(t)

	pub.Stop()
	pub.Publish(msg1, result1.set)
	pub.Publish(msg2, result2.set)

	result1.validateError(ErrServiceStopped)
	result2.validateError(ErrServiceStopped)

	if err := pub.WaitStopped(); err != nil {
		t.Errorf("routingPublisher.Error() got: (%v), want: <nil>", err)
	}
}
