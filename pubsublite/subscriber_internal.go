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
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errInvalidInitialSubscribeResponse = errors.New("pubsublite: first response from server was not an initial response for subscribe")
)

type subscriber struct {
	// Immutable after creation.
	subsClient   *vkit.SubscriberClient
	subscription SubscriptionPath
	partition    int
	initialReq   *pb.SubscribeRequest

	// Fields below must be guarded with mutex.
	stream *retryableStream

	abstractService
}

func newSubscriber(ctx context.Context, subsClient *vkit.SubscriberClient, subs SubscriptionPath, partition int) *subscriber {
	subscriber := &subscriber{
		subsClient:   subsClient,
		subscription: subs,
		partition:    partition,
		initialReq: &pb.SubscribeRequest{
			Request: &pb.SubscribeRequest_Initial{
				Initial: &pb.InitialSubscribeRequest{
					Subscription: subs.String(),
					Partition:    int64(partition),
				},
			},
		},
	}
	// TODO: the timeout should be from ReceiveSettings.
	subscriber.stream = newRetryableStream(ctx, subscriber, time.Minute, reflect.TypeOf(pb.SubscribeResponse{}))
	return subscriber
}

func (s *subscriber) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stream.Start()
}

func (s *subscriber) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsafeInitiateShutdown(serviceTerminating, nil)
}

func (s *subscriber) newStream(ctx context.Context) (grpc.ClientStream, error) {
	return s.subsClient.Subscribe(addSubscriptionRoutingMetadata(ctx, s.subscription, s.partition))
}

func (s *subscriber) initialRequest() interface{} {
	return s.initialReq
}

func (s *subscriber) validateInitialResponse(response interface{}) error {
	subscribeResponse, _ := response.(*pb.SubscribeResponse)
	if subscribeResponse.GetInitial() == nil {
		return errInvalidInitialSubscribeResponse
	}
	return nil
}

func (s *subscriber) onStreamStatusChange(status streamStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch status {
	case streamConnected:
		s.unsafeUpdateStatus(serviceActive, nil)

	case streamReconnecting:

	case streamTerminated:
		s.unsafeInitiateShutdown(serviceTerminated, s.stream.Error())
	}
}

func (s *subscriber) onResponse(response interface{}) {
	fmt.Printf("Response: %v\n", response)
	s.mu.Lock()
	defer s.mu.Unlock()

	processResponse := func() error {
		return nil
	}
	if err := processResponse(); err != nil {
		s.unsafeInitiateShutdown(serviceTerminated, err)
	}
}

func (s *subscriber) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	if !s.unsafeUpdateStatus(targetStatus, err) {
		return
	}

	// Otherwise immediately terminate the stream.
	s.stream.Stop()
}

type partitionSubscriber struct {
	// Have their own mutexes.
	subscriber *subscriber
	committer  *committer

	acks *ackTracker

	compositeService
}

func newPartitionSubscriber(ctx context.Context, subsClient *vkit.SubscriberClient, cursorClient *vkit.CursorClient, subscription SubscriptionPath, partition int) *partitionSubscriber {
	acks := newAckTracker()
	subs := newSubscriber(ctx, subsClient, subscription, partition)
	commit := newCommitter(ctx, cursorClient, subscription, partition, acks)
	partitionSubs := &partitionSubscriber{
		subscriber: subs,
		committer:  commit,
		acks:       acks,
	}
	partitionSubs.init()
	partitionSubs.unsafeAddService(subs)
	partitionSubs.unsafeAddService(commit)
	return partitionSubs
}

// When message received from stream:
// - create ack receivers
// - add ack recv to ack tracker
// - forward msg, ack receiver to next layer up

// When acked
// - pop the ack tracker,
// - refill flow controller

// Subscriber
// - Owns flow controller
// - Allow flow, batch and periodically send flow control tokers
// - Manage stream reconnection
// - Owns receive offset
