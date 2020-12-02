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

package ps

import (
	"context"
	"testing"

	pubsub "cloud.google.com/go/pubsublite/internal/pubsub"
	"cloud.google.com/go/pubsublite/internal/wire"
)

type messageReceiverAction int

const (
	actionIgnore messageReceiverAction = 0
	actionAck    messageReceiverAction = 1
	actionNack   messageReceiverAction = 2
)

type testMessageReceiver struct {
	Action   messageReceiverAction
	t        *testing.T
	Received []*pubsub.Message
}

func newTestMessageReceiver(t *testing.T) *testMessageReceiver {
	return &testMessageReceiver{
		Action: actionAck,
		t:      t,
	}
}

func (tr *testMessageReceiver) onMessage(ctx context.Context, msg *pubsub.Message) {
	tr.Received = append(tr.Received, msg)

	switch tr.Action {
	case actionAck:
		msg.Ack()
	case actionNack:
		msg.Nack()
	}
}

// mockWireSubscriber is a mock implementation of the wire.Subscriber interface.
type mockWireSubscriber struct {
	Receiver wire.MessageReceiverFunc
	FakeErr  error
	stop     chan struct{}
}

func (mp *mockWireSubscriber) Start()             {}
func (mp *mockWireSubscriber) Stop()              { close(mp.stop) }
func (mp *mockWireSubscriber) WaitStarted() error { return mp.FakeErr }
func (mp *mockWireSubscriber) WaitStopped() error {
	<-mp.stop // Wait until stopped
	return mp.FakeErr
}

type mockWireSubscriberFactory struct{}

func (f *mockWireSubscriberFactory) New(receiver wire.MessageReceiverFunc) (wire.Subscriber, error) {
	return &mockWireSubscriber{
		Receiver: receiver,
		stop:     make(chan struct{}),
	}, nil
}

func newTestSubscriberInstance(ctx context.Context, settings ReceiveSettings, receiver MessageReceiverFunc) *subscriberInstance {
	sub, _ := newSubscriberInstance(ctx, new(mockWireSubscriberFactory), settings, receiver)
	return sub
}
