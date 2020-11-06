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

// testWireSubscriber wraps a commiter for ease of testing.
type testWireSubscriber struct {
	t        *testing.T
	subs     *wireSubscriber
	received chan *testMessageHolder
	serviceTestProxy
}

type testMessageHolder struct {
	Msg *pb.SequencedMessage
	Ack AckConsumer
}

func newTestWireSubscriber(t *testing.T, subscription subscriptionPartition, settings ReceiveSettings, acks *ackTracker) *testWireSubscriber {
	ctx := context.Background()
	subsClient, err := newSubscriberClient(ctx, "ignored", testClientOpts...)
	if err != nil {
		t.Fatal(err)
	}

	ts := &testWireSubscriber{
		t:        t,
		received: make(chan *testMessageHolder, 1),
	}
	ts.subs = newWireSubscriber(ctx, subsClient, new(disabledPeriodicTaskFactory), settings, ts.onMessages, subscription, acks)
	ts.initAndStart(t, ts.subs, "Subscriber")
	return ts
}

func (ts *testWireSubscriber) onMessages(msg *pb.SequencedMessage, ack AckConsumer) {
	ts.received <- &testMessageHolder{Msg: msg, Ack: ack}
}

func (ts *testWireSubscriber) NextMsg() *testMessageHolder {
	select {
	case <-time.After(serviceTestProxyWaitTimeout):
		ts.t.Errorf("%s message not received within %v", ts.name, serviceTestProxyWaitTimeout)
		return nil
	case next := <-ts.received:
		return next
	}
}

// SendBatchFlowControl invokes the periodic background batch flow control. Note
// that the periodic task is disabled in tests.
func (ts *testWireSubscriber) SendBatchFlowControl() {
	ts.subs.sendPendingFlowControl()
}

func TestWireSubscriberReconnect(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	acks := newAckTracker()

	// Settings determine the initial flow control request.
	settings := defaultTestReceiveSettings
	settings.MaxOutstandingMessages = 16
	settings.MaxOutstandingBytes = 1000

	wantMsg1 := seqMsgWithOffsetAndSize(67, 200)
	msgResp1 := msgSubResp(wantMsg1)
	wantMsg2 := seqMsgWithOffsetAndSize(68, 100)
	msgResp2 := msgSubResp(wantMsg2)

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream1 := test.NewRPCVerifier(t)
	stream1.Push(initSubReq(subscription), initSubResp(), nil)
	stream1.Push(flowControlSubReq(flowControlTokens{Bytes: 1000, Messages: 16}), msgResp1, nil)
	stream1.Push(nil, nil, status.Error(codes.Unavailable, "server unavailable"))
	mockServer.AddSubscribeStream(subscription.Path, subscription.Partition, stream1)

	// When reconnected, the wireSubscriber should seek to msg2 and have
	// subtracted flow control tokens.
	stream2 := test.NewRPCVerifier(t)
	stream2.Push(initSubReq(subscription), initSubResp(), nil)
	stream2.Push(seekReq(68), seekResp(68), nil)
	stream2.Push(flowControlSubReq(flowControlTokens{Bytes: 800, Messages: 15}), msgResp2, nil)
	mockServer.AddSubscribeStream(subscription.Path, subscription.Partition, stream2)

	cmt := newTestWireSubscriber(t, subscription, settings, acks)
	defer cmt.StopVerifyNoError()
	if gotErr := cmt.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}

	got := cmt.NextMsg()
	if got != nil && !proto.Equal(got.Msg, wantMsg1) {
		t.Errorf("Message 1 got: (%v), want: (%v)", got.Msg, wantMsg1)
	}
	got = cmt.NextMsg()
	if got != nil && !proto.Equal(got.Msg, wantMsg2) {
		t.Errorf("Message 2 got: (%v), want: (%v)", got.Msg, wantMsg2)
	}
}

/*
func TestSinglePartitionSubscriberStartStop(t *testing.T) {
	subscription := subscriptionPartition{"projects/123456/locations/us-central1-b/subscriptions/my-subs", 0}
	acks := newAckTracker()

	mockServer.OnTestStart(nil)
	defer mockServer.OnTestEnd()

	stream := test.NewRPCVerifier(t)
	stream.Push(initCommitReq(subscription), initCommitResp(), nil)
	mockServer.AddCommitStream(subscription.Path, subscription.Partition, stream)

	cmt := newTestCommitter(t, subscription, acks)
	if gotErr := cmt.StartError(); gotErr != nil {
		t.Errorf("Start() got err: (%v)", gotErr)
	}
	cmt.StopVerifyNoError()
}
*/
