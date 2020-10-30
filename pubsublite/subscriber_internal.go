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
	"reflect"

	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errInvalidInitialSubscribeResponse = errors.New("pubsublite: first response from server was not an initial response for subscribe")
	errDuplicateReceive                = errors.New("pubsublite: already called Receive for subscriber")
)

type wireMessagesReceiverFunc func([]*pb.SequencedMessage)

// wireSubscriber directly wraps the subscribe stream.
type wireSubscriber struct {
	// Immutable after creation.
	subsClient   *vkit.SubscriberClient
	subscription SubscriptionPath
	partition    int
	initialReq   *pb.SubscribeRequest
	receiver     wireMessagesReceiverFunc

	// Fields below must be guarded with mutex.
	stream        *retryableStream
	offsetTracker *subscriberOffsetTracker

	abstractService
}

func newWireSubscriber(ctx context.Context, subsClient *vkit.SubscriberClient, settings ReceiveSettings, subs SubscriptionPath, partition int, receiver wireMessagesReceiverFunc) *wireSubscriber {
	subscriber := &wireSubscriber{
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
		receiver:      receiver,
		offsetTracker: newSubscriberOffsetTracker(),
	}
	subscriber.stream = newRetryableStream(ctx, subscriber, settings.Timeout, reflect.TypeOf(pb.SubscribeResponse{}))
	return subscriber
}

func (s *wireSubscriber) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stream.Start()
}

func (s *wireSubscriber) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsafeInitiateShutdown(serviceTerminating, nil)
}

func (s *wireSubscriber) newStream(ctx context.Context) (grpc.ClientStream, error) {
	return s.subsClient.Subscribe(addSubscriptionRoutingMetadata(ctx, s.subscription, s.partition))
}

func (s *wireSubscriber) initialRequest() interface{} {
	return s.initialReq
}

func (s *wireSubscriber) validateInitialResponse(response interface{}) error {
	subscribeResponse, _ := response.(*pb.SubscribeResponse)
	if subscribeResponse.GetInitial() == nil {
		return errInvalidInitialSubscribeResponse
	}
	return nil
}

func (s *wireSubscriber) AllowFlow(allowMessages, allowBytes int64) {
	// TODO: Batch flow control tokens.
	req := &pb.SubscribeRequest{
		Request: &pb.SubscribeRequest_FlowControl{
			FlowControl: &pb.FlowControlRequest{
				AllowedMessages: allowMessages,
				AllowedBytes:    allowBytes,
			},
		},
	}
	s.stream.Send(req)
}

func (s *wireSubscriber) onStreamStatusChange(status streamStatus) {
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

func (s *wireSubscriber) onResponse(response interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	processResponse := func() error {
		subscribeResponse, _ := response.(*pb.SubscribeResponse)
		if subscribeResponse.GetMessages() != nil {
			// TODO: goroutine? Needs to be sync
			// Handle zero messages
			// Subtract from flow control
			// Update current msg offset
			msgs := subscribeResponse.GetMessages().Messages
			if len(msgs) > 0 {
				s.receiver(msgs)
			}
		}
		return nil
	}
	if err := processResponse(); err != nil {
		s.unsafeInitiateShutdown(serviceTerminated, err)
	}
}

func (s *wireSubscriber) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	if !s.unsafeUpdateStatus(targetStatus, err) {
		return
	}

	// Otherwise immediately terminate the stream.
	s.stream.Stop()
}

type messageReceiverFunc func(*pb.SequencedMessage, *ackReplyConsumer)

type partitionSubscriber struct {
	settings ReceiveSettings

	// Have their own mutexes.
	subscriber *wireSubscriber
	committer  *committer
	acks       *ackTracker

	receiver messageReceiverFunc

	compositeService
}

func newPartitionSubscriber(ctx context.Context, subsClient *vkit.SubscriberClient, cursorClient *vkit.CursorClient, settings ReceiveSettings, subscription SubscriptionPath, partition int) *partitionSubscriber {
	acks := newAckTracker()
	commit := newCommitter(ctx, cursorClient, settings, subscription, partition, acks)
	ps := &partitionSubscriber{
		settings:  settings,
		committer: commit,
		acks:      acks,
	}
	subs := newWireSubscriber(ctx, subsClient, settings, subscription, partition, ps.onMessages)
	ps.subscriber = subs
	ps.init()
	ps.unsafeAddServices(subs, commit)
	return ps
}

func (ps *partitionSubscriber) Receive(receiver messageReceiverFunc) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if err := ps.unsafeCheckServiceStatus(); err != nil {
		return err
	}
	if ps.receiver != nil {
		return errDuplicateReceive
	}

	ps.receiver = receiver
	ps.subscriber.AllowFlow(int64(ps.settings.MaxOutstandingMessages), int64(ps.settings.MaxOutstandingBytes))
	return nil
}

func (ps *partitionSubscriber) onMessages(messages []*pb.SequencedMessage) {
	// Package the message to send upstream
	for _, msg := range messages {
		ack := newAckReplyConsumer(msg.GetCursor().GetOffset(), msg.GetSizeBytes(), ps.onAck)
		ps.acks.Push(ack)
		ps.receiver(msg, ack)
	}
}

func (ps *partitionSubscriber) onAck(ar *ackReplyConsumer) {
	ps.acks.Pop()
	ps.subscriber.AllowFlow(1, ar.MsgBytes)
}

// TODO: pause receiving

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
