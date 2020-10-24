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
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsublite/internal/test"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

const (
	publisherWaitTimeout = 30 * time.Second
)

var (
	// Initialized in TestMain.
	testServer *test.Server
	mockServer *test.MockLiteServer
	clientOpts []option.ClientOption

	defaultTestPublishSettings PublishSettings
)

func TestMain(m *testing.M) {
	flag.Parse()

	defaultTestPublishSettings = DefaultPublishSettings
	// Send 1 message at a time to make tests deterministic.
	defaultTestPublishSettings.CountThreshold = 1
	// Send messages with minimal delay to speed up tests.
	defaultTestPublishSettings.DelayThreshold = time.Millisecond

	testServer, err := test.NewServer()
	if err != nil {
		log.Fatal(err)
	}
	mockServer = testServer.LiteServer
	conn, err := grpc.Dial(testServer.Addr(), grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	clientOpts = []option.ClientOption{option.WithGRPCConn(conn)}

	exit := m.Run()
	testServer.Close()
	os.Exit(exit)
}

func newTestPartitionPublisher(t *testing.T, topic TopicPath, partition int, settings PublishSettings) (*partitionPublisher, chan struct{}) {
	ctx := context.Background()
	region, _ := ZoneToRegion(topic.Zone)
	pubClient, err := newPublisherClient(ctx, region, clientOpts...)
	if err != nil {
		t.Fatal(err)
	}

	terminated := make(chan struct{})
	onPubTerminated := func(p *partitionPublisher) {
		close(terminated)
	}
	return newPartitionPublisher(ctx, pubClient, settings, topic, partition, onPubTerminated), terminated
}

func pubStartError(pub *partitionPublisher) error {
	startResults := make(chan error, 1)
	pub.Start(startResults)
	err := <-startResults
	return err
}

func pubFinalError(t *testing.T, pub *partitionPublisher, terminated chan struct{}) error {
	select {
	case <-time.After(publisherWaitTimeout):
		return fmt.Errorf("publisher did not terminate within %v", publisherWaitTimeout)
	case <-terminated:
		if gotStatus, wantStatus := pub.stream.Status(), streamTerminated; gotStatus != wantStatus {
			t.Errorf("Stream status: %v, want: %v", gotStatus, wantStatus)
		}
		if pub.stream.currentStream() != nil {
			t.Error("gRPC stream should be nil")
		}
		return pub.Error()
	}
}

func stopPublisher(t *testing.T, pub *partitionPublisher, terminated chan struct{}) {
	pub.Stop()
	if gotErr := pubFinalError(t, pub, terminated); gotErr != nil {
		t.Errorf("Publisher final err: (%v), want: <nil>", gotErr)
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

func contextWithTimeout() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), publisherWaitTimeout)
	return ctx
}

func validatePubResult(ctx context.Context, t *testing.T, result *publishMetadata, wantID string) {
	gotID, err := result.Get(ctx)
	if err != nil {
		t.Errorf("Publish() error: (%v), want ID: %q", err, wantID)
	} else if gotID != wantID {
		t.Errorf("Publish() got ID: %q, want ID: %q", gotID, wantID)
	}
}

func validatePubError(ctx context.Context, t *testing.T, result *publishMetadata, wantErr error) {
	_, gotErr := result.Get(ctx)
	if !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Publish() error: (%v), want: (%v)", gotErr, wantErr)
	}
}

func validatePubErrorCode(ctx context.Context, t *testing.T, result *publishMetadata, wantCode codes.Code) {
	_, gotErr := result.Get(ctx)
	if !test.ErrorHasCode(gotErr, wantCode) {
		t.Errorf("Publish() error: (%v), want code: %v", gotErr, wantCode)
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

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	defer stopPublisher(t, pub, terminated)

	t.Run("First succeeds", func(t *testing.T) {
		if gotErr := pubStartError(pub); gotErr != nil {
			t.Errorf("Start() got err: (%v)", gotErr)
		}
	})
	// Ensure that the publisher can only be started once.
	t.Run("Second fails", func(t *testing.T) {
		if gotErr, wantErr := pubStartError(pub), errDuplicateStreamStart; !test.ErrorEqual(gotErr, wantErr) {
			t.Errorf("Start() got err: (%v), want: (%v)", gotErr, wantErr)
		}
	})
}

func TestPartitionPublisherStartStop(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	block := stream.PushWithBlock(initPubReq(topic, partition), initPubResp(), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	startResults := make(chan error, 1)
	pub.Start(startResults)
	time.Sleep(10 * time.Millisecond)
	pub.Stop()
	time.Sleep(10 * time.Millisecond)
	close(block)

	// Ensure that the stream is not connected if the publisher stops immediately
	// after close.
	gotErr := <-startResults
	wantCode := codes.Canceled
	if !test.ErrorHasCode(gotErr, wantCode) {
		t.Errorf("Start() got err: (%v), want code: %v", gotErr, wantCode)
	}
	if gotErr := pubFinalError(t, pub, terminated); gotErr != nil {
		t.Errorf("Publisher final err: (%v), want: <nil>", gotErr)
	}
	if gotErr := pub.stream.Error(); gotErr != nil {
		t.Errorf("Stream final err: (%v), want: <nil>", gotErr)
	}
}

func TestPartitionPublisherStopAbortsRetries(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	// Unavailable is a retryable error, but the stream should not be retried.
	block := stream.PushWithBlock(initPubReq(topic, partition), initPubResp(), status.Error(codes.Unavailable, ""))
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	startResults := make(chan error, 1)
	pub.Start(startResults)
	time.Sleep(10 * time.Millisecond)
	pub.Stop()
	time.Sleep(10 * time.Millisecond)
	close(block)

	// Ensure that the stream is not connected if the publisher stops immediately
	// after close.
	gotErr := <-startResults
	wantCode := codes.Canceled
	if !test.ErrorHasCode(gotErr, wantCode) {
		t.Errorf("Start() got err: (%v), want code: %v", gotErr, wantCode)
	}
	if gotErr := pubFinalError(t, pub, terminated); gotErr != nil {
		t.Errorf("Publisher final err: (%v), want: <nil>", gotErr)
	}
	if gotErr := pub.stream.Error(); gotErr != nil {
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

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	defer stopPublisher(t, pub, terminated)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}
}

func TestPartitionPublisherConnectPermanentFailure(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	permErr := status.Error(codes.PermissionDenied, "denied")

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), nil, permErr)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pubStartError(pub); !test.ErrorEqual(gotErr, permErr) {
		t.Errorf("Start() got err: (%v), want: (%v)", gotErr, permErr)
	}
	if gotErr := pubFinalError(t, pub, terminated); !test.ErrorEqual(gotErr, permErr) {
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

	pub, terminated := newTestPartitionPublisher(t, topic, partition, settings)
	startResults := make(chan error, 1)
	pub.Start(startResults)

	// Send the server response after settings.Timeout to simulate a timeout.
	time.Sleep(50 * time.Millisecond)
	close(block)

	gotErr := <-startResults
	if !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Start() got err: (%v), want: (%v)", gotErr, wantErr)
	}
	if gotErr := pubFinalError(t, pub, terminated); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestPartitionPublisherInvalidInitialResponse(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// The server sends a MessagePublishResponse for the initial response, which
	// the client treats as a permanent failure (bug on the server).
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), msgPubResp(0), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	wantErr := errInvalidInitalPubResponse
	if gotErr := pubStartError(pub); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Start() got err: (%v), want: (%v)", gotErr, wantErr)
	}
	if gotErr := pubFinalError(t, pub, terminated); !test.ErrorEqual(gotErr, wantErr) {
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

	pub, terminated := newTestPartitionPublisher(t, topic, partition, settings)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	result3 := pub.Publish(msg3)
	result4 := pub.Publish(msg4)
	// Bundler invokes at most 1 handler and may add a message to the end of the
	// last bundle if the hard limits (BundleByteLimit) aren't reached. Pause to
	// handle previous bundles.
	time.Sleep(20 * time.Millisecond)
	result5 := pub.Publish(msg5)
	result6 := pub.Publish(msg6)
	result7 := pub.Publish(msg7)
	// Stop flushes pending messages.
	pub.Stop()

	ctx := contextWithTimeout()
	validatePubResult(ctx, t, result1, "0:0")
	validatePubResult(ctx, t, result2, "0:1")
	validatePubResult(ctx, t, result3, "0:2")
	validatePubResult(ctx, t, result4, "0:3")
	validatePubResult(ctx, t, result5, "0:45")
	validatePubResult(ctx, t, result6, "0:46")
	validatePubResult(ctx, t, result7, "0:47")

	if gotErr := pubFinalError(t, pub, terminated); gotErr != nil {
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

	pub, terminated := newTestPartitionPublisher(t, topic, partition, settings)
	defer stopPublisher(t, pub, terminated)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	time.Sleep(settings.DelayThreshold * 2)
	result2 := pub.Publish(msg2)

	ctx := contextWithTimeout()
	validatePubResult(ctx, t, result1, "0:0")
	validatePubResult(ctx, t, result2, "0:1")
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

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	defer stopPublisher(t, pub, terminated)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	ctx := contextWithTimeout()
	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	validatePubResult(ctx, t, result1, "0:0")
	validatePubResult(ctx, t, result2, "0:1")

	result3 := pub.Publish(msg3)
	validatePubResult(ctx, t, result3, "0:2")
}

func TestPartitionPublisherPublishPermanentError(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	permanentErr := status.Error(codes.NotFound, "topic deleted")

	msg1 := &pb.PubSubMessage{Data: []byte{'1'}}
	msg2 := &pb.PubSubMessage{Data: []byte{'2'}}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	// Simulate a permanent server error that terminates publishing.
	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	stream.Push(msgPubReq(msg1), nil, permanentErr)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	ctx := contextWithTimeout()
	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	validatePubError(ctx, t, result1, permanentErr)
	validatePubError(ctx, t, result2, permanentErr)

	// This message arrives after the publisher has already stopped, so its error
	// message is ErrServiceStopped.
	result3 := pub.Publish(msg3)
	validatePubError(ctx, t, result3, ErrServiceStopped)

	if gotErr := pubFinalError(t, pub, terminated); !test.ErrorEqual(gotErr, permanentErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, permanentErr)
	}
}

func TestPartitionPublisherBufferOverflow(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	settings := defaultTestPublishSettings
	settings.BufferedByteLimit = 20

	msg1 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'1'}, 10)}
	msg2 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'2'}, 10)}
	msg3 := &pb.PubSubMessage{Data: []byte{'3'}}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	block := stream.PushWithBlock(msgPubReq(msg1), msgPubResp(0), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub, terminated := newTestPartitionPublisher(t, topic, partition, settings)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	ctx := contextWithTimeout()
	result1 := pub.Publish(msg1)
	// Overflow is detected, which terminates the publisher, but previous messages
	// are flushed.
	result2 := pub.Publish(msg2)
	// Delay the server response of the first publish to ensure it is allowed to
	// complete.
	close(block)
	// This message arrives after the publisher has already stopped, so its error
	// message is ErrServiceStopped.
	result3 := pub.Publish(msg3)

	validatePubResult(ctx, t, result1, "0:0")
	validatePubError(ctx, t, result2, ErrOverflow)
	validatePubError(ctx, t, result3, ErrServiceStopped)

	if gotErr := pubFinalError(t, pub, terminated); !test.ErrorEqual(gotErr, ErrOverflow) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, ErrOverflow)
	}
}

func TestPartitionPublisherBufferRefill(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0
	settings := defaultTestPublishSettings
	settings.BufferedByteLimit = 20

	msg1 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'1'}, 10)}
	msg2 := &pb.PubSubMessage{Data: bytes.Repeat([]byte{'2'}, 10)}

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initPubReq(topic, partition), initPubResp(), nil)
	stream.Push(msgPubReq(msg1), msgPubResp(0), nil)
	stream.Push(msgPubReq(msg2), msgPubResp(1), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub, terminated := newTestPartitionPublisher(t, topic, partition, settings)
	defer stopPublisher(t, pub, terminated)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	ctx := contextWithTimeout()
	result1 := pub.Publish(msg1)
	validatePubResult(ctx, t, result1, "0:0")

	// The second message is sent after the response for the first has been
	// received. `availableBufferBytes` should be refilled.
	result2 := pub.Publish(msg2)
	validatePubResult(ctx, t, result2, "0:1")
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

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	ctx := contextWithTimeout()
	result1 := pub.Publish(msg1)
	// Fails, which terminates the publisher, but pending messages are flushed.
	result2 := pub.Publish(msg2)
	// Delay the response of the first publish to ensure it is allowed to
	// complete.
	close(block)
	// This message arrives after the publisher has already stopped.
	result3 := pub.Publish(msg3)

	validatePubResult(ctx, t, result1, "0:0")
	validatePubErrorCode(ctx, t, result2, codes.FailedPrecondition)
	validatePubError(ctx, t, result3, ErrServiceStopped)

	if gotErr := pubFinalError(t, pub, terminated); !test.ErrorHasCode(gotErr, codes.FailedPrecondition) {
		t.Errorf("Publisher final err: (%v), want code: %v", gotErr, codes.FailedPrecondition)
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
	// The server returns an inconsistent cursor offset, which causes the
	// publisher to fail permanently.
	stream.Push(msgPubReq(msg2), msgPubResp(4), nil)
	stream.Push(msgPubReq(msg3), msgPubResp(5), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	ctx := contextWithTimeout()
	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	result3 := pub.Publish(msg3)
	close(block)

	validatePubResult(ctx, t, result1, "0:4")
	validatePubErrorCode(ctx, t, result2, codes.Internal)
	validatePubErrorCode(ctx, t, result3, codes.Internal)

	if gotErr := pubFinalError(t, pub, terminated); !test.ErrorHasCode(gotErr, codes.Internal) {
		t.Errorf("Publisher final err: (%v), want code: %v", gotErr, codes.Internal)
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
	// to fail permanently.
	stream.Push(msgPubReq(msg), initPubResp(), nil)
	mockServer.AddPublishStream(topic.String(), partition, stream)

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result := pub.Publish(msg)

	wantErr := errInvalidMsgPubResponse
	validatePubError(contextWithTimeout(), t, result, wantErr)
	if gotErr := pubFinalError(t, pub, terminated); !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

// TODO:
// perm error happens during flush

func TestPartitionPublisherPublishBeforeStart(t *testing.T) {
	topic := TopicPath{Project: "123456", Zone: "us-central1-b", TopicID: "my-topic"}
	partition := 0

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)

	result := pub.Publish(&pb.PubSubMessage{Data: []byte{'1'}})

	wantErr := ErrServiceUninitialized
	validatePubError(contextWithTimeout(), t, result, wantErr)
	if gotErr := pubFinalError(t, pub, terminated); !test.ErrorEqual(gotErr, wantErr) {
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

	pub, terminated := newTestPartitionPublisher(t, topic, partition, defaultTestPublishSettings)
	if gotErr := pubStartError(pub); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	result1 := pub.Publish(msg1)
	result2 := pub.Publish(msg2)
	result3 := pub.Publish(msg3)
	pub.Stop()
	close(block)
	result4 := pub.Publish(msg4)

	ctx := contextWithTimeout()
	// First 2 messages should be allowed to complete.
	validatePubResult(ctx, t, result1, "0:5")
	validatePubResult(ctx, t, result2, "0:6")
	// Third message failed with a server error, which should result in the
	// publisher terminating with an error.
	validatePubError(ctx, t, result3, finalErr)
	// Fourth message was sent after the user called Stop(), so should fail
	// immediately.
	validatePubError(ctx, t, result4, ErrServiceStopped)

	if gotErr := pubFinalError(t, pub, terminated); !test.ErrorEqual(gotErr, finalErr) {
		t.Errorf("Publisher final err: (%v), want: (%v)", gotErr, finalErr)
	}
}
