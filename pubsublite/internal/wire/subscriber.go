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

	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errServerNoMessages                = errors.New("pubsublite: server delivered no messages")
	errInvalidInitialSubscribeResponse = errors.New("pubsublite: first response from server was not an initial response for subscribe")
	errInvalidSubscribeResponse        = errors.New("pubsublite: received invalid subscribe response from server")
	errDuplicateReceive                = errors.New("pubsublite: already called Receive for subscriber")
)

type wireMessagesReceiverFunc func([]*pb.SequencedMessage)

// wireSubscriber directly wraps the subscribe stream.
type wireSubscriber struct {
	// Immutable after creation.
	subsClient   *vkit.SubscriberClient
	subscription subscriptionPartition
	partition    int
	initialReq   *pb.SubscribeRequest
	receiver     wireMessagesReceiverFunc

	// Fields below must be guarded with mutex.
	stream        *retryableStream
	offsetTracker *subscriberOffsetTracker

	abstractService
}

func newWireSubscriber(ctx context.Context, subsClient *vkit.SubscriberClient, settings ReceiveSettings, subscription subscriptionPartition, receiver wireMessagesReceiverFunc) *wireSubscriber {
	subscriber := &wireSubscriber{
		subsClient:   subsClient,
		subscription: subscription,
		initialReq: &pb.SubscribeRequest{
			Request: &pb.SubscribeRequest_Initial{
				Initial: &pb.InitialSubscribeRequest{
					Subscription: subscription.path,
					Partition:    int64(subscription.partition),
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
	return s.subsClient.Subscribe(addSubscriptionRoutingMetadata(ctx, s.subscription))
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
		if seekReq := s.offsetTracker.RequestForRestart(); seekReq != nil {
			req := &pb.SubscribeRequest{
				Request: &pb.SubscribeRequest_Seek{Seek: seekReq},
			}
			s.stream.Send(req)
		}
		// Else allow flow?? Needs to go through flow controller

	case streamReconnecting:

	case streamTerminated:
		s.unsafeInitiateShutdown(serviceTerminated, s.stream.Error())
	}
}

// Subscriber
// - Owns flow controller
// - Allow flow, batch and periodically send flow control tokern
// - Manage stream reconnection - seek when reconnecting

func (s *wireSubscriber) onResponse(response interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	processResponse := func() error {
		subscribeResponse, _ := response.(*pb.SubscribeResponse)
		switch {
		case subscribeResponse.GetMessages() != nil:
			return s.onMessageResponse(subscribeResponse.GetMessages())
		case subscribeResponse.GetSeek() != nil:
			return s.onSeekResponse(subscribeResponse.GetSeek())
		default:
			return errInvalidSubscribeResponse
		}
	}
	if err := processResponse(); err != nil {
		s.unsafeInitiateShutdown(serviceTerminated, err)
	}
}

func (s *wireSubscriber) onMessageResponse(response *pb.MessageResponse) error {
	// TODO: Subtract from flow control
	if len(response.Messages) == 0 {
		return errServerNoMessages
	}
	if err := s.offsetTracker.Update(response.Messages); err != nil {
		return err
	}
	s.receiver(response.Messages)
	return nil
}

func (s *wireSubscriber) onSeekResponse(response *pb.SeekResponse) error {
	// TODO: Send flow control for restart
	return nil
}

func (s *wireSubscriber) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	if !s.unsafeUpdateStatus(targetStatus, err) {
		return
	}
	// No data to send. Immediately terminate the stream.
	s.stream.Stop()
}

type messageReceiverFunc func(*pb.SequencedMessage, *ackReplyConsumer)

type singlePartitionSubscriber struct {
	settings ReceiveSettings

	// Have their own mutexes.
	subscriber *wireSubscriber
	committer  *committer
	acks       *ackTracker

	receiver messageReceiverFunc

	compositeService
}

func newSinglePartitionSubscriber(ctx context.Context, subsClient *vkit.SubscriberClient, cursorClient *vkit.CursorClient, settings ReceiveSettings, subscription subscriptionPartition) *singlePartitionSubscriber {
	acks := newAckTracker()
	commit := newCommitter(ctx, cursorClient, settings, subscription, acks)
	ps := &singlePartitionSubscriber{
		settings:  settings,
		committer: commit,
		acks:      acks,
	}
	subs := newWireSubscriber(ctx, subsClient, settings, subscription, ps.onMessages)
	ps.subscriber = subs
	ps.init()
	ps.unsafeAddServices(subs, commit)
	return ps
}

func (ps *singlePartitionSubscriber) Receive(receiver messageReceiverFunc) error {
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

func (ps *singlePartitionSubscriber) onMessages(messages []*pb.SequencedMessage) {
	// Package the message to send upstream
	for _, msg := range messages {
		ack := newAckReplyConsumer(msg.GetCursor().GetOffset(), msg.GetSizeBytes(), ps.onAck)
		ps.acks.Push(ack) // TODO: handle error
		ps.receiver(msg, ack)
	}
}

func (ps *singlePartitionSubscriber) onAck(ar *ackReplyConsumer) {
	ps.acks.Pop()
	ps.subscriber.AllowFlow(1, ar.MsgBytes)
}

// TODO: cancel receiving
