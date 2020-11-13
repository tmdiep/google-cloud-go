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
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	"cloud.google.com/go/pubsublite/internal/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

func testPublishSettings() PublishSettings {
	settings := DefaultPublishSettings
	// Send 1 message at a time to make tests deterministic.
	settings.CountThreshold = 1
	// Send messages with minimal delay to speed up tests.
	settings.DelayThreshold = time.Millisecond
	settings.Timeout = 5 * time.Second
	return settings
}

// publishResultReceiver provides convenience methods for receiving and
// validating Publish results.
type publishResultReceiver struct {
	done   chan struct{}
	msg    string
	t      *testing.T
	got    *PublishMetadata
	gotErr error
}

func newPublishResultReceiver(t *testing.T, msg string) *publishResultReceiver {
	return &publishResultReceiver{
		t:    t,
		msg:  msg,
		done: make(chan struct{}),
	}
}

func (r *publishResultReceiver) set(pm *PublishMetadata, err error) {
	r.got = pm
	r.gotErr = err
	close(r.done)
}

func (r *publishResultReceiver) wait() bool {
	select {
	case <-time.After(serviceTestWaitTimeout):
		r.t.Errorf("Publish(%q) result not available within %v", r.msg, serviceTestWaitTimeout)
		return false
	case <-r.done:
		return true
	}
}

func (r *publishResultReceiver) ValidateResult(wantPartition int, wantOffset int64) {
	if !r.wait() {
		return
	}
	if r.gotErr != nil {
		r.t.Errorf("Publish(%q) error: (%v), want: partition=%d,offset=%d", r.msg, r.gotErr, wantPartition, wantOffset)
	} else if r.got.Partition != wantPartition || r.got.Offset != wantOffset {
		r.t.Errorf("Publish(%q) got: partition=%d,offset=%d, want: partition=%d,offset=%d", r.msg, r.got.Partition, r.got.Offset, wantPartition, wantOffset)
	}
}

func (r *publishResultReceiver) ValidateError(wantErr error) {
	if !r.wait() {
		return
	}
	if !test.ErrorEqual(r.gotErr, wantErr) {
		r.t.Errorf("Publish(%q) error: (%v), want: (%v)", r.msg, r.gotErr, wantErr)
	}
}

func (r *publishResultReceiver) ValidateErrorCode(wantCode codes.Code) {
	if !r.wait() {
		return
	}
	if !test.ErrorHasCode(r.gotErr, wantCode) {
		r.t.Errorf("Publish(%q) error: (%v), want code: %v", r.msg, r.gotErr, wantCode)
	}
}

func (r *publishResultReceiver) ValidateErrorMsg(wantStr string) {
	if !r.wait() {
		return
	}
	if !test.ErrorHasMsg(r.gotErr, wantStr) {
		r.t.Errorf("Publish(%q) error: (%v), want msg: %q", r.msg, r.gotErr, wantStr)
	}
}

// testPartitionPublisher wraps a singlePartitionPublisher for ease of testing.
type testPartitionPublisher struct {
	pub *singlePartitionPublisher
	serviceTestProxy
}

func newTestSinglePartitionPublisher(t *testing.T, topic topicPartition, settings PublishSettings) *testPartitionPublisher {
	ctx := context.Background()
	pubClient, err := newPublisherClient(ctx, "ignored", testClientOpts...)
	if err != nil {
		t.Fatal(err)
	}

	pubFactory := &singlePartitionPublisherFactory{
		ctx:       ctx,
		pubClient: pubClient,
		settings:  settings,
		topicPath: topic.Path,
	}
	tp := &testPartitionPublisher{
		pub: pubFactory.New(topic.Partition),
	}
	tp.initAndStart(t, tp.pub, "Publisher")
	return tp
}

func (tp *testPartitionPublisher) Publish(msg *pb.PubSubMessage) *publishResultReceiver {
	result := newPublishResultReceiver(tp.t, string(msg.Data))
	tp.pub.Publish(msg, result.set)
	return result
}

func (tp *testPartitionPublisher) FinalError() (err error) {
	err = tp.serviceTestProxy.FinalError()

	// Verify that the stream has terminated.
	if gotStatus, wantStatus := tp.pub.stream.Status(), streamTerminated; gotStatus != wantStatus {
		tp.t.Errorf("%s retryableStream status: %v, want: %v", tp.name, gotStatus, wantStatus)
	}
	if tp.pub.stream.currentStream() != nil {
		tp.t.Errorf("%s client stream should be nil", tp.name)
	}
	return
}

func (tp *testPartitionPublisher) StreamError() error {
	return tp.pub.stream.Error()
}

func TestSinglePartitionPublisherInvalidInitialResponse(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}

	verifiers := test.NewVerifiers(t)
	// If the server sends an invalid initial response, the client treats this as
	// a permanent failure (bug on the server).
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), msgPubResp(0), nil)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, testPublishSettings())

	wantErr := errInvalidInitialPubResponse
	if gotErr := pub.StartError(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Start() got err: (%v), want: (%v)", gotErr, wantErr)
	}
	if gotErr := pub.FinalError(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestSinglePartitionPublisherSpuriousPublishResponse(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), initPubResp(), nil)
	// If the server has sent a MessagePublishResponse when no messages were
	// published, the client treats this as a permanent failure (bug on the
	// server).
	barrier := stream.PushWithBarrier(nil, msgPubResp(0), nil)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, testPublishSettings())
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	// Send after startup to ensure the test is deterministic.
	barrier.Release()

	if gotErr, wantErr := pub.FinalError(), errPublishQueueEmpty; !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestSinglePartitionPublisherBatching(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}
	settings := testPublishSettings()
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

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), initPubResp(), nil)
	stream.Push(msgPubReq(msg1, msg2, msg3), msgPubResp(0), nil)
	stream.Push(msgPubReq(msg4), msgPubResp(3), nil)
	stream.Push(msgPubReq(msg5, msg6, msg7), msgPubResp(45), nil)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, settings)
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	result3 := pub.Publish(msg3)
	result4 := pub.Publish(msg4)
	// Bundler invokes at most 1 handler and may add a message to the end of the
	// last bundle if the hard limits (BundleByteLimit) aren't reached. Pause to
	// handle previous bundles.
	time.Sleep(10 * time.Millisecond)
	result5 := pub.Publish(msg5)
	result6 := pub.Publish(msg6)
	result7 := pub.Publish(msg7)
	// Stop flushes pending messages.
	pub.Stop()

	result1.ValidateResult(topic.Partition, 0)
	result2.ValidateResult(topic.Partition, 1)
	result3.ValidateResult(topic.Partition, 2)
	result4.ValidateResult(topic.Partition, 3)
	result5.ValidateResult(topic.Partition, 45)
	result6.ValidateResult(topic.Partition, 46)
	result7.ValidateResult(topic.Partition, 47)

	if gotErr := pub.FinalError(); gotErr != nil {
		t.Errorf("Publisher final err: (%v), want: <nil>", gotErr)
	}
}

func TestSinglePartitionPublisherBatchingDelay(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}
	settings := testPublishSettings()
	settings.CountThreshold = 100
	settings.DelayThreshold = 5 * time.Millisecond

	// Batch 1.
	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	// Batch 2.
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), initPubResp(), nil)
	stream.Push(msgPubReq(msg1), msgPubResp(0), nil)
	stream.Push(msgPubReq(msg2), msgPubResp(1), nil)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, settings)
	defer pub.StopVerifyNoError()
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	time.Sleep(settings.DelayThreshold * 2)
	result2 := pub.Publish(msg2)

	result1.ValidateResult(topic.Partition, 0)
	result2.ValidateResult(topic.Partition, 1)
}

func TestSinglePartitionPublisherResendMessages(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}

	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	verifiers := test.NewVerifiers(t)

	// Simulate a transient error that results in a reconnect.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topic), initPubResp(), nil)
	stream1.Push(msgPubReq(msg1), nil, status.Error(codes.Aborted, "server aborted"))
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream1)

	// The publisher should re-send pending messages to the second stream.
	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initPubReq(topic), initPubResp(), nil)
	stream2.Push(msgPubReq(msg1), msgPubResp(0), nil)
	stream2.Push(msgPubReq(msg2), msgPubResp(1), nil)
	stream2.Push(msgPubReq(msg3), msgPubResp(2), nil)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream2)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, testPublishSettings())
	defer pub.StopVerifyNoError()
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	result1.ValidateResult(topic.Partition, 0)
	result2.ValidateResult(topic.Partition, 1)

	result3 := pub.Publish(msg3)
	result3.ValidateResult(topic.Partition, 2)
}

func TestSinglePartitionPublisherPublishPermanentError(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}
	permError := status.Error(codes.NotFound, "topic deleted")

	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	verifiers := test.NewVerifiers(t)
	// Simulate a permanent server error that terminates publishing.
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), initPubResp(), nil)
	stream.Push(msgPubReq(msg1), nil, permError)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, testPublishSettings())
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	result1.ValidateError(permError)
	result2.ValidateError(permError)

	// This message arrives after the publisher has already stopped, so its error
	// message is ErrServiceStopped.
	result3 := pub.Publish(msg3)
	result3.ValidateError(ErrServiceStopped)

	if gotErr := pub.FinalError(); !test.ErrorEqual(gotErr, permError) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, permError)
	}
}

func TestSinglePartitionPublisherBufferOverflow(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}
	settings := testPublishSettings()
	settings.BufferedByteLimit = 15

	msg1 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'1'}, 10)}
	msg2 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'2'}, 10)}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), initPubResp(), nil)
	barrier := stream.PushWithBarrier(msgPubReq(msg1), msgPubResp(0), nil)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, settings)
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	// Overflow is detected, which terminates the publisher, but previous messages
	// are flushed.
	result2 := pub.Publish(msg2)
	// Delay the server response for the first Publish to verify that it is
	// allowed to complete.
	barrier.Release()
	// This message arrives after the publisher has already stopped, so its error
	// message is ErrServiceStopped.
	result3 := pub.Publish(msg3)

	result1.ValidateResult(topic.Partition, 0)
	result2.ValidateError(ErrOverflow)
	result3.ValidateError(ErrServiceStopped)

	if gotErr := pub.FinalError(); !test.ErrorEqual(gotErr, ErrOverflow) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, ErrOverflow)
	}
}

func TestSinglePartitionPublisherBufferRefill(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}
	settings := testPublishSettings()
	settings.BufferedByteLimit = 15

	msg1 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'1'}, 10)}
	msg2 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'2'}, 8)}

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), initPubResp(), nil)
	stream.Push(msgPubReq(msg1), msgPubResp(0), nil)
	stream.Push(msgPubReq(msg2), msgPubResp(1), nil)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, settings)
	defer pub.StopVerifyNoError()
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	result1.ValidateResult(topic.Partition, 0)

	result2 := pub.Publish(msg2)
	result2.ValidateResult(topic.Partition, 1)
}

func TestSinglePartitionPublisherValidatesMaxMsgSize(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}

	msg1 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'1'}, 10)}
	msg2 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'2'}, MaxPublishMessageBytes+1)}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), initPubResp(), nil)
	barrier := stream.PushWithBarrier(msgPubReq(msg1), msgPubResp(0), nil)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, testPublishSettings())
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	// Fails due to over msg size limit, which terminates the publisher, but
	// pending messages are flushed.
	result2 := pub.Publish(msg2)
	// Delay the server response for the first Publish to ensure it is allowed to
	// complete.
	barrier.Release()
	// This message arrives after the publisher has already stopped.
	result3 := pub.Publish(msg3)

	wantErrMsg := "maximum allowed size is MaxPublishMessageBytes"
	result1.ValidateResult(topic.Partition, 0)
	result2.ValidateErrorMsg(wantErrMsg)
	result3.ValidateError(ErrServiceStopped)

	if gotErr := pub.FinalError(); !test.ErrorHasMsg(gotErr, wantErrMsg) {
		t.Errorf("Publisher final err: (%v), want msg: %v", gotErr, wantErrMsg)
	}
}

func TestSinglePartitionPublisherInvalidCursorOffsets(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}

	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), initPubResp(), nil)
	barrier := stream.PushWithBarrier(msgPubReq(msg1), msgPubResp(4), nil)
	// The server returns an inconsistent cursor offset for msg2, which causes the
	// publisher client to fail permanently.
	stream.Push(msgPubReq(msg2), msgPubResp(4), nil)
	stream.Push(msgPubReq(msg3), msgPubResp(5), nil)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, testPublishSettings())
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	result3 := pub.Publish(msg3)
	barrier.Release()

	result1.ValidateResult(topic.Partition, 4)

	wantMsg := "server returned publish response with inconsistent start offset"
	result2.ValidateErrorMsg(wantMsg)
	result3.ValidateErrorMsg(wantMsg)
	if gotErr := pub.FinalError(); !test.ErrorHasMsg(gotErr, wantMsg) {
		t.Errorf("Publisher final err: (%v), want msg: %q", gotErr, wantMsg)
	}
}

func TestSinglePartitionPublisherInvalidServerPublishResponse(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}
	msg := &pb.PubSubMessage{Data: []byte{'1'}}

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), initPubResp(), nil)
	// Server sends duplicate initial Publish response, which causes the publisher
	// client to fail permanently.
	stream.Push(msgPubReq(msg), initPubResp(), nil)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, testPublishSettings())
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result := pub.Publish(msg)

	wantErr := errInvalidMsgPubResponse
	result.ValidateError(wantErr)
	if gotErr := pub.FinalError(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestSinglePartitionPublisherStopFlushesMessages(t *testing.T) {
	topic := topicPartition{"projects/123456/locations/us-central1-b/topics/my-topic", 0}
	finalErr := status.Error(codes.FailedPrecondition, "invalid message")

	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}
	msg4 := &pb.PubSubMessage{Data: []byte{'4'}}

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic), initPubResp(), nil)
	barrier := stream.PushWithBarrier(msgPubReq(msg1), msgPubResp(5), nil)
	stream.Push(msgPubReq(msg2), msgPubResp(6), nil)
	stream.Push(msgPubReq(msg3), nil, finalErr)
	verifiers.AddPublishStream(topic.Path, topic.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestSinglePartitionPublisher(t, topic, testPublishSettings())
	if gotErr := pub.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	result3 := pub.Publish(msg3)
	pub.Stop()
	barrier.Release()
	result4 := pub.Publish(msg4)

	// First 2 messages should be allowed to complete.
	result1.ValidateResult(topic.Partition, 5)
	result2.ValidateResult(topic.Partition, 6)
	// Third message failed with a server error, which should result in the
	// publisher terminating with an error.
	result3.ValidateError(finalErr)
	// Fourth message was sent after the user called Stop(), so should fail
	// immediately with ErrServiceStopped.
	result4.ValidateError(ErrServiceStopped)

	if gotErr := pub.FinalError(); !test.ErrorEqual(gotErr, finalErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, finalErr)
	}
}

type testRoutingPublisher struct {
	t   *testing.T
	pub *routingPublisher
}

func newTestRoutingPublisher(t *testing.T, topicPath string, settings PublishSettings, fakeSourceVal int64) *testRoutingPublisher {
	ctx := context.Background()
	pubClient, err := newPublisherClient(ctx, "ignored", testClientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	adminClient, err := NewAdminClient(ctx, "ignored", testClientOpts...)
	if err != nil {
		t.Fatal(err)
	}

	source := &test.FakeSource{Ret: fakeSourceVal}
	msgRouter := newDefaultMessageRouter(rand.New(source))
	pubFactory := &singlePartitionPublisherFactory{
		ctx:       ctx,
		pubClient: pubClient,
		settings:  settings,
		topicPath: topicPath,
	}
	pub := newRoutingPublisher(adminClient, msgRouter, pubFactory)
	pub.Start()
	return &testRoutingPublisher{t: t, pub: pub}
}

func (tp *testRoutingPublisher) Publish(msg *pb.PubSubMessage) *publishResultReceiver {
	result := newPublishResultReceiver(tp.t, string(msg.Data))
	tp.pub.Publish(msg, result.set)
	return result
}

func (tp *testRoutingPublisher) NumPartitionPublishers() int {
	return len(tp.pub.publishers)
}

func (tp *testRoutingPublisher) Start()             { tp.pub.Start() }
func (tp *testRoutingPublisher) Stop()              { tp.pub.Stop() }
func (tp *testRoutingPublisher) WaitStarted() error { return tp.pub.WaitStarted() }
func (tp *testRoutingPublisher) WaitStopped() error { return tp.pub.WaitStopped() }

func TestRoutingPublisherStartOnce(t *testing.T) {
	topic := "projects/123456/locations/us-central1-b/topics/my-topic"
	numPartitions := 2

	verifiers := test.NewVerifiers(t)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), topicPartitionsResp(numPartitions), nil)

	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topicPartition{topic, 0}), initPubResp(), nil)
	verifiers.AddPublishStream(topic, 0, stream1)

	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initPubReq(topicPartition{topic, 1}), initPubResp(), nil)
	verifiers.AddPublishStream(topic, 1, stream2)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestRoutingPublisher(t, topic, testPublishSettings(), 0)
	defer pub.Stop()

	t.Run("First succeeds", func(t *testing.T) {
		// Note: newTestRoutingPublisher called Start.
		if gotErr := pub.WaitStarted(); gotErr != nil {
			t.Errorf("Start() got err: (%v)", gotErr)
		}
		if got, want := pub.NumPartitionPublishers(), numPartitions; got != want {
			t.Errorf("Num partition publishers: got %d, want %d", got, want)
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
	topic := "projects/123456/locations/us-central1-b/topics/my-topic"
	wantErr := status.Error(codes.NotFound, "no exist")

	// Retrieving the number of partitions results in an error. Startup cannot
	// proceed.
	verifiers := test.NewVerifiers(t)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), nil, wantErr)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestRoutingPublisher(t, topic, testPublishSettings(), 0)

	if gotErr := pub.WaitStarted(); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Start() got err: (%v), want err: (%v)", gotErr, wantErr)
	}
	if got, want := pub.NumPartitionPublishers(), 0; got != want {
		t.Errorf("Num partition publishers: got %d, want %d", got, want)
	}

	// Ensure that the publisher does not attempt to restart. The mock server does
	// not expect more RPCs.
	pub.Start()
}

func TestRoutingPublisherPartitionCountInvalid(t *testing.T) {
	topic := "projects/123456/locations/us-central1-b/topics/my-topic"

	// The number of partitions returned by the server must be valid, otherwise
	// startup cannot proceed.
	verifiers := test.NewVerifiers(t)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), topicPartitionsResp(0), nil)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestRoutingPublisher(t, topic, testPublishSettings(), 0)

	wantMsg := "topic has invalid number of partitions"
	if gotErr := pub.WaitStarted(); !test.ErrorHasMsg(gotErr, wantMsg) {
		t.Errorf("Start() got err: (%v), want msg: %q", gotErr, wantMsg)
	}
	if got, want := pub.NumPartitionPublishers(), 0; got != want {
		t.Errorf("Num partition publishers: got %d, want %d", got, want)
	}

	// Ensure that the publisher does not attempt to restart. The mock server does
	// not expect more RPCs.
	pub.Start()
}

func TestRoutingPublisherRoundRobin(t *testing.T) {
	topic := "projects/123456/locations/us-central1-b/topics/my-topic"
	numPartitions := 3

	// Messages have no ordering key, so the roundRobinMsgRouter is used.
	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}
	msg4 := &pb.PubSubMessage{Data: []byte{'4'}}

	verifiers := test.NewVerifiers(t)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), topicPartitionsResp(numPartitions), nil)

	// Partition 0.
	stream0 := test.NewRPCVerifier(t)
	stream0.Push(initPubReq(topicPartition{topic, 0}), initPubResp(), nil)
	stream0.Push(msgPubReq(msg3), msgPubResp(34), nil)
	verifiers.AddPublishStream(topic, 0, stream0)

	// Partition 1.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topicPartition{topic, 1}), initPubResp(), nil)
	stream1.Push(msgPubReq(msg1), msgPubResp(41), nil)
	stream1.Push(msgPubReq(msg4), msgPubResp(42), nil)
	verifiers.AddPublishStream(topic, 1, stream1)

	// Partition 2.
	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initPubReq(topicPartition{topic, 2}), initPubResp(), nil)
	stream2.Push(msgPubReq(msg2), msgPubResp(78), nil)
	verifiers.AddPublishStream(topic, 2, stream2)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	// Note: The fake source is initialized with value=1, so Partition=1 publisher
	// will be the first chosen by the roundRobinMsgRouter.
	pub := newTestRoutingPublisher(t, topic, testPublishSettings(), 1)
	if err := pub.WaitStarted(); err != nil {
		t.Errorf("Start() got err: (%v)", err)
	}

	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	result3 := pub.Publish(msg3)
	result4 := pub.Publish(msg4)
	pub.Stop()

	result1.ValidateResult(1, 41)
	result2.ValidateResult(2, 78)
	result3.ValidateResult(0, 34)
	result4.ValidateResult(1, 42)

	if err := pub.WaitStopped(); err != nil {
		t.Errorf("Stop() got err: (%v)", err)
	}
}

func TestRoutingPublisherHashing(t *testing.T) {
	topic := "projects/123456/locations/us-central1-b/topics/my-topic"
	numPartitions := 3

	key0 := []byte("bar") // hashes to partition 0
	key1 := []byte("baz") // hashes to partition 1
	key2 := []byte("foo") // hashes to partition 2

	// Messages have ordering key, so the hashingMsgRouter is used.
	msg1 := &pb.PubSubMessage{Data: []byte{'1'}, Key: key2}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}, Key: key0}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}, Key: key2}
	msg4 := &pb.PubSubMessage{Data: []byte{'4'}, Key: key1}
	msg5 := &pb.PubSubMessage{Data: []byte{'5'}, Key: key0}

	verifiers := test.NewVerifiers(t)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), topicPartitionsResp(numPartitions), nil)

	// Partition 0.
	stream0 := test.NewRPCVerifier(t)
	stream0.Push(initPubReq(topicPartition{topic, 0}), initPubResp(), nil)
	stream0.Push(msgPubReq(msg2), msgPubResp(20), nil)
	stream0.Push(msgPubReq(msg5), msgPubResp(21), nil)
	verifiers.AddPublishStream(topic, 0, stream0)

	// Partition 1.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topicPartition{topic, 1}), initPubResp(), nil)
	stream1.Push(msgPubReq(msg4), msgPubResp(40), nil)
	verifiers.AddPublishStream(topic, 1, stream1)

	// Partition 2.
	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initPubReq(topicPartition{topic, 2}), initPubResp(), nil)
	stream2.Push(msgPubReq(msg1), msgPubResp(10), nil)
	stream2.Push(msgPubReq(msg3), msgPubResp(11), nil)
	verifiers.AddPublishStream(topic, 2, stream2)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestRoutingPublisher(t, topic, testPublishSettings(), 0)
	if err := pub.WaitStarted(); err != nil {
		t.Errorf("Start() got err: (%v)", err)
	}

	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	result3 := pub.Publish(msg3)
	result4 := pub.Publish(msg4)
	result5 := pub.Publish(msg5)

	result1.ValidateResult(2, 10)
	result2.ValidateResult(0, 20)
	result3.ValidateResult(2, 11)
	result4.ValidateResult(1, 40)
	result5.ValidateResult(0, 21)

	pub.Stop()
	if err := pub.WaitStopped(); err != nil {
		t.Errorf("Stop() got err: (%v)", err)
	}
}

func TestRoutingPublisherPermanentError(t *testing.T) {
	topic := "projects/123456/locations/us-central1-b/topics/my-topic"
	numPartitions := 2
	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	serverErr := status.Error(codes.FailedPrecondition, "failed")

	verifiers := test.NewVerifiers(t)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), topicPartitionsResp(numPartitions), nil)

	// Partition 0.
	stream0 := test.NewRPCVerifier(t)
	stream0.Push(initPubReq(topicPartition{topic, 0}), initPubResp(), nil)
	stream0.Push(msgPubReq(msg1), msgPubResp(34), nil)
	verifiers.AddPublishStream(topic, 0, stream0)

	// Partition 1. Fails due to permanent error, which will also shut down
	// partition-0 publisher, but it should be allowed to flush its pending
	// messages.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topicPartition{topic, 1}), initPubResp(), nil)
	stream1.Push(msgPubReq(msg2), nil, serverErr)
	verifiers.AddPublishStream(topic, 1, stream1)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestRoutingPublisher(t, topic, testPublishSettings(), 0)
	if err := pub.WaitStarted(); err != nil {
		t.Errorf("Start() got err: (%v)", err)
	}

	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)

	result1.ValidateResult(0, 34)
	result2.ValidateError(serverErr)

	if gotErr := pub.WaitStopped(); !test.ErrorEqual(gotErr, serverErr) {
		t.Errorf("Final error got: (%v), want: (%v)", gotErr, serverErr)
	}
}

func TestRoutingPublisherPublishAfterStop(t *testing.T) {
	topic := "projects/123456/locations/us-central1-b/topics/my-topic"
	numPartitions := 2
	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}

	verifiers := test.NewVerifiers(t)
	verifiers.GlobalVerifier.Push(topicPartitionsReq(topic), topicPartitionsResp(numPartitions), nil)

	// Partition 0.
	stream0 := test.NewRPCVerifier(t)
	stream0.Push(initPubReq(topicPartition{topic, 0}), initPubResp(), nil)
	verifiers.AddPublishStream(topic, 0, stream0)

	// Partition 1.
	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initPubReq(topicPartition{topic, 1}), initPubResp(), nil)
	verifiers.AddPublishStream(topic, 1, stream1)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	pub := newTestRoutingPublisher(t, topic, testPublishSettings(), 0)
	if err := pub.WaitStarted(); err != nil {
		t.Errorf("Start() got err: (%v)", err)
	}

	pub.Stop()
	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)

	result1.ValidateError(ErrServiceStopped)
	result2.ValidateError(ErrServiceStopped)

	if err := pub.WaitStopped(); err != nil {
		t.Errorf("Stop() got err: (%v)", err)
	}
}
