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
	"time"

	"cloud.google.com/go/pubsublite/internal/test"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

func testSubscriberSettings() ReceiveSettings {
	settings := testReceiveSettings()
	settings.MaxOutstandingMessages = 10
	settings.MaxOutstandingBytes = 1000
	return settings
}

// initFlowControlReq returns the first expected flow control request when
// testSubscriberSettings are used.
func initFlowControlReq() *pb.SubscribeRequest {
	return flowControlSubReq(flowControlTokens{Bytes: 1000, Messages: 10})
}

type testMessageHolder struct {
	Msg *pb.SequencedMessage
	Ack AckConsumer
}

type testMessageReceiver struct {
	t        *testing.T
	received chan *testMessageHolder
}

func newTestMessageReceiver(t *testing.T) *testMessageReceiver {
	return &testMessageReceiver{
		t:        t,
		received: make(chan *testMessageHolder, 5),
	}
}

func (tr *testMessageReceiver) onMessages(msg *pb.SequencedMessage, ack AckConsumer) {
	tr.received <- &testMessageHolder{Msg: msg, Ack: ack}
}

func (tr *testMessageReceiver) ValidateMsg(want *pb.SequencedMessage) AckConsumer {
	select {
	case <-time.After(serviceTestWaitTimeout):
		tr.t.Errorf("Message (%v) not received within %v", want, serviceTestWaitTimeout)
		return nil
	case got := <-tr.received:
		if !proto.Equal(got.Msg, want) {
			tr.t.Errorf("Received message: got (%v), want (%v)", got.Msg, want)
		}
		return got.Ack
	}
}

// testWireSubscriber wraps a wireSubscriber for ease of testing.
type testWireSubscriber struct {
	Receiver *testMessageReceiver
	t        *testing.T
	subs     *wireSubscriber
	serviceTestProxy
}

func newTestWireSubscriber(t *testing.T, subscription subscriptionPartition, settings ReceiveSettings, acks *ackTracker) *testWireSubscriber {
	ctx := context.Background()
	subsClient, err := newSubscriberClient(ctx, "ignored", testClientOpts...)
	if err != nil {
		t.Fatal(err)
	}

	ts := &testWireSubscriber{
		Receiver: newTestMessageReceiver(t),
		t:        t,
	}
	ts.subs = newWireSubscriber(ctx, subsClient, settings, ts.Receiver.onMessages, subscription, acks, true)
	ts.initAndStart(t, ts.subs, "Subscriber")
	return ts
}

// SendBatchFlowControl invokes the periodic background batch flow control. Note
// that the periodic task is disabled in tests.
func (ts *testWireSubscriber) SendBatchFlowControl() {
	ts.subs.sendBatchFlowControl()
}

func TestWireSubscriberReconnect(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	acks := newAckTracker()
	wantMsg1 := seqMsgWithOffsetAndSize(67, 200)
	wantMsg2 := seqMsgWithOffsetAndSize(68, 100)
	permanentErr := status.Error(codes.FailedPrecondition, "permanent failure")

	verifiers := test.NewVerifiers(t)

	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initSubReq(subscription), initSubResp(), nil)
	stream1.Push(initFlowControlReq(), msgSubResp(wantMsg1), nil)
	stream1.Push(nil, nil, status.Error(codes.Unavailable, "server unavailable"))
	verifiers.AddSubscribeStream(subscription.Path, subscription.Partition, stream1)

	// When reconnected, the wireSubscriber should seek to msg2 and have
	// subtracted flow control tokens.
	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initSubReq(subscription), initSubResp(), nil)
	stream2.Push(seekReq(68), seekResp(68), nil)
	stream2.Push(flowControlSubReq(flowControlTokens{Bytes: 800, Messages: 9}), msgSubResp(wantMsg2), nil)
	// Terminates on permanent error.
	stream2.Push(nil, nil, permanentErr)
	verifiers.AddSubscribeStream(subscription.Path, subscription.Partition, stream2)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	subs := newTestWireSubscriber(t, subscription, testSubscriberSettings(), acks)

	if gotErr := subs.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}
	subs.Receiver.ValidateMsg(wantMsg1)
	subs.Receiver.ValidateMsg(wantMsg2)
	if gotErr, wantErr := subs.FinalError(), permanentErr; !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestWireSubscriberInvalidInitialResponse(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	acks := newAckTracker()

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initSubReq(subscription), seekResp(0), nil)
	verifiers.AddSubscribeStream(subscription.Path, subscription.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	subs := newTestWireSubscriber(t, subscription, testSubscriberSettings(), acks)
	if gotErr, wantErr := subs.StartError(), errInvalidInitialSubscribeResponse; !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Start got err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestWireSubscriberDuplicateInitialResponse(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	acks := newAckTracker()

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initSubReq(subscription), initSubResp(), nil)
	stream.Push(initFlowControlReq(), initSubResp(), nil) // Second initial response
	verifiers.AddSubscribeStream(subscription.Path, subscription.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	subs := newTestWireSubscriber(t, subscription, testSubscriberSettings(), acks)
	if gotErr := subs.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}
	if gotErr, wantErr := subs.FinalError(), errInvalidSubscribeResponse; !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestWireSubscriberSpuriousSeekResponse(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	acks := newAckTracker()

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initSubReq(subscription), initSubResp(), nil)
	stream.Push(initFlowControlReq(), seekResp(1), nil) // No seek requested
	verifiers.AddSubscribeStream(subscription.Path, subscription.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	subs := newTestWireSubscriber(t, subscription, testSubscriberSettings(), acks)
	if gotErr := subs.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}
	if gotErr, wantErr := subs.FinalError(), errNoInFlightSeek; !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestWireSubscriberNoMessages(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	acks := newAckTracker()

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initSubReq(subscription), initSubResp(), nil)
	barrier := stream.PushWithBarrier(initFlowControlReq(), msgSubResp(), nil) // No messages in response
	verifiers.AddSubscribeStream(subscription.Path, subscription.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	subs := newTestWireSubscriber(t, subscription, testSubscriberSettings(), acks)
	if gotErr := subs.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}
	barrier.Release() // To ensure tests are deterministic
	if gotErr, wantErr := subs.FinalError(), errServerNoMessages; !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func TestWireSubscriberMessagesOutOfOrder(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	acks := newAckTracker()
	msg1 := seqMsgWithOffsetAndSize(56, 100)
	msg2 := seqMsgWithOffsetAndSize(55, 100)

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initSubReq(subscription), initSubResp(), nil)
	stream.Push(initFlowControlReq(), msgSubResp(msg1), nil)
	stream.Push(nil, msgSubResp(msg2), nil)
	verifiers.AddSubscribeStream(subscription.Path, subscription.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	subs := newTestWireSubscriber(t, subscription, testSubscriberSettings(), acks)
	if gotErr := subs.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	subs.Receiver.ValidateMsg(msg1)
	if gotErr, wantMsg := subs.FinalError(), "start offset = 55, expected >= 57"; !test.ErrorHasMsg(gotErr, wantMsg) {
		t.Errorf("Final err: (%v), want msg: %q", gotErr, wantMsg)
	}
}

func TestWireSubscriberFlowControlOverflow(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	acks := newAckTracker()
	msg1 := seqMsgWithOffsetAndSize(56, 900)
	msg2 := seqMsgWithOffsetAndSize(57, 101) // Overflows ReceiveSettings.MaxOutstandingBytes = 1000

	verifiers := test.NewVerifiers(t)
	stream := test.NewRPCVerifier(t)
	stream.Push(initSubReq(subscription), initSubResp(), nil)
	stream.Push(initFlowControlReq(), msgSubResp(msg1), nil)
	stream.Push(nil, msgSubResp(msg2), nil)
	verifiers.AddSubscribeStream(subscription.Path, subscription.Partition, stream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	subs := newTestWireSubscriber(t, subscription, testSubscriberSettings(), acks)
	if gotErr := subs.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	subs.Receiver.ValidateMsg(msg1)
	if gotErr, wantErr := subs.FinalError(), errTokenCounterBytesNegative; !test.ErrorEqual(gotErr, wantErr) {
		t.Errorf("Final err: (%v), want: (%v)", gotErr, wantErr)
	}
}

func newTestSinglePartitionSubscriber(t *testing.T, receiver *testMessageReceiver, subscription subscriptionPartition) *singlePartitionSubscriber {
	ctx := context.Background()
	subsClient, err := newSubscriberClient(ctx, "ignored", testClientOpts...)
	if err != nil {
		t.Fatal(err)
	}
	cursorClient, err := newCursorClient(ctx, "ignored", testClientOpts...)
	if err != nil {
		t.Fatal(err)
	}

	f := &singlePartitionSubscriberFactory{
		ctx:              ctx,
		subsClient:       subsClient,
		cursorClient:     cursorClient,
		settings:         testSubscriberSettings(),
		subscriptionPath: subscription.Path,
		receiver:         receiver.onMessages,
		disableTasks:     true,
	}
	subs := f.New(subscription.Partition)
	subs.Start()
	return subs
}

func TestSinglePartitionSubscriberStartStop(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	receiver := newTestMessageReceiver(t)

	verifiers := test.NewVerifiers(t)

	// Verifies the behavior of the wireSubscriber and committer when they are
	// stopped before any messages are received.
	subStream := test.NewRPCVerifier(t)
	subStream.Push(initSubReq(subscription), initSubResp(), nil)
	barrier := subStream.PushWithBarrier(initFlowControlReq(), nil, nil)
	verifiers.AddSubscribeStream(subscription.Path, subscription.Partition, subStream)

	cmtStream := test.NewRPCVerifier(t)
	cmtStream.Push(initCommitReq(subscription), initCommitResp(), nil)
	verifiers.AddCommitStream(subscription.Path, subscription.Partition, cmtStream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	subs := newTestSinglePartitionSubscriber(t, receiver, subscription)
	if gotErr := subs.WaitStarted(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	barrier.Release() // To ensure the test is deterministic (i.e. flow control req received)
	subs.Stop()
	if gotErr := subs.WaitStopped(); gotErr != nil {
		t.Errorf("Stop() got err: (%v)", gotErr)
	}
}

func TestSinglePartitionSubscriberSimpleMsgAck(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	receiver := newTestMessageReceiver(t)
	msg1 := seqMsgWithOffsetAndSize(22, 100)
	msg2 := seqMsgWithOffsetAndSize(23, 200)

	verifiers := test.NewVerifiers(t)

	subStream := test.NewRPCVerifier(t)
	subStream.Push(initSubReq(subscription), initSubResp(), nil)
	subStream.Push(initFlowControlReq(), msgSubResp(msg1, msg2), nil)
	verifiers.AddSubscribeStream(subscription.Path, subscription.Partition, subStream)

	cmtStream := test.NewRPCVerifier(t)
	cmtStream.Push(initCommitReq(subscription), initCommitResp(), nil)
	cmtStream.Push(commitReq(24), commitResp(1), nil)
	verifiers.AddCommitStream(subscription.Path, subscription.Partition, cmtStream)

	mockServer.OnTestStart(verifiers)
	defer mockServer.OnTestEnd()

	subs := newTestSinglePartitionSubscriber(t, receiver, subscription)
	if gotErr := subs.WaitStarted(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	receiver.ValidateMsg(msg1).Ack()
	receiver.ValidateMsg(msg2).Ack()
	subs.Stop()
	if gotErr := subs.WaitStopped(); gotErr != nil {
		t.Errorf("Stop() got err: (%v)", gotErr)
	}
}
