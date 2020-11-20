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
	"errors"
	"sync"

	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/internal/wire"
	"google.golang.org/api/option"

	pubsub "cloud.google.com/go/pubsublite/internal/pubsub"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	ErrNackCalled       = errors.New("pubsublite: SubscriberClient does not support nack. See NackHandler for how to customize nack handling")
	ErrDuplicateReceive = errors.New("pubsublite: an existing SubscriberClient.Receive() is still active")
)

// handleNack is the default NackHandler implementation.
func handleNack(_ *pubsub.Message) error {
	return ErrNackCalled
}

// transformReceivedMessage is the default ReceiveMessageTransformerFunc
// implementation.
func transformReceivedMessage(from *pb.SequencedMessage, to *pubsub.Message) error {
	// TODO
	return nil
}

type pslAckHandler struct {
	ackh  wire.AckConsumer
	msg   *pubsub.Message
	nackh func(*pubsub.Message) error
}

func (ah *pslAckHandler) OnAck() {
	ah.ackh.Ack()
}

func (ah *pslAckHandler) OnNack() {
	if err := ah.nackh(ah.msg); err == nil {
		// If the NackHandler succeeds, ack the message.
		ah.ackh.Ack()
	}
}

// MessageReceiverFunc handles messages sent by the Cloud Pub/Sub Lite system.
// Only one call from any connected partition will be outstanding at a time, and
// blocking in this receiver callback will block forward progress.
type MessageReceiverFunc func(context.Context, *pubsub.Message)

// SubscriberClient is a Cloud Pub/Sub Lite client to receive messages for a
// given subscription.
type SubscriberClient struct {
	settings     ReceiveSettings
	region       string
	subscription pubsublite.SubscriptionPath
	options      []option.ClientOption

	mu      sync.Mutex
	wireSub wire.Subscriber
	err     error
}

// NewSubscriberClient creates a new Cloud Pub/Sub Lite client to messages for a
// given subscription.
// See https://cloud.google.com/pubsub/lite/docs/subscribing for more
// information about receiving messages.
func NewSubscriberClient(ctx context.Context, settings ReceiveSettings, subscription pubsublite.SubscriptionPath, opts ...option.ClientOption) (*SubscriberClient, error) {
	region, err := pubsublite.ZoneToRegion(subscription.Zone)
	if err != nil {
		return nil, err
	}
	subClient := &SubscriberClient{
		settings:     settings,
		region:       region,
		subscription: subscription,
		options:      opts,
	}
	if subClient.settings.MessageTransformer == nil {
		subClient.settings.MessageTransformer = transformReceivedMessage
	}
	if subClient.settings.NackHandler == nil {
		subClient.settings.NackHandler = handleNack
	}
	return subClient, nil
}

func (s *SubscriberClient) Receive(ctx context.Context, receiver MessageReceiverFunc) error {
	wireSub, err := s.initReceive(ctx, receiver)
	if err != nil {
		return err
	}
	// TODO: stop subscriber on ctx done.
	err = wireSub.WaitStopped()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	return err
}

func (s *SubscriberClient) initReceive(ctx context.Context, receiver MessageReceiverFunc) (wire.Subscriber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.wireSub != nil {
		return nil, ErrDuplicateReceive
	}

	onMessage := func(msgs []*wire.ReceivedMessage) {
		for _, m := range msgs {
			pslAckh := &pslAckHandler{
				ackh:  m.Ack,
				nackh: s.handleNack,
			}
			psMsg := pubsub.NewMessage(pslAckh)
			pslAckh.msg = psMsg
			if err := s.settings.MessageTransformer(m.Msg, psMsg); err != nil {
				s.terminate(err)
				return
			}
			receiver(ctx, psMsg)

			// TODO: Check whether the subscriber has stopped. If it has, cancel the
			// acks in the rest of the msg batch
		}
	}

	wireSub, err := wire.NewSubscriber(ctx, s.settings.toWireSettings(), onMessage, s.region, s.subscription.String(), s.options...)
	if err != nil {
		return nil, err
	}
	wireSub.Start()
	if err := wireSub.WaitStarted(); err != nil {
		return nil, err
	}
	s.wireSub = wireSub
	s.err = nil
	return wireSub, nil
}

func (s *SubscriberClient) handleNack(msg *pubsub.Message) error {
	err := s.settings.NackHandler(msg)
	if err != nil {
		s.terminate(err)
	}
	return err
}

func (s *SubscriberClient) terminate(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.err = err
	if s.wireSub != nil {
		s.wireSub.Stop()
	}
}
