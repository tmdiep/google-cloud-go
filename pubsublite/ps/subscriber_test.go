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
	"cloud.google.com/go/pubsublite/internal/wire"
)

// mockWireSubscriber is a mock implementation of the wire.Subscriber interface.
type mockWireSubscriber struct {
	Receiver wire.MessageReceiverFunc
	FakeErr  error
	Stopped  bool
}

func (mp *mockWireSubscriber) Start()             {}
func (mp *mockWireSubscriber) Stop()              { mp.Stopped = true }
func (mp *mockWireSubscriber) WaitStarted() error { return mp.FakeErr }
func (mp *mockWireSubscriber) WaitStopped() error { return mp.FakeErr }

type mockWireSubscriberFactory struct {}

func (f *mockWireSubscriberFactory) New(receiver wire.MessageReceiverFunc) (wire.Subscriber, error) {
	return &mockWireSubscriber{Receiver: receiver}, nil
}

func newTestSubscriberClient(settings ReceiveSettings) *SubscriberClient {
	return newSubscriberClient(new(mockWireSubscriberFactory), settings)
}

