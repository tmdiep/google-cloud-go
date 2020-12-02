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
)

var (
	errNackCalled       = errors.New("pubsublite: subscriber client does not support nack. See NackHandler for how to customize nack handling")
	errDuplicateReceive = errors.New("pubsublite: receive is already in progress for this subscriber client")
)

// handleNack is the default NackHandler implementation.
func handleNack(_ *pubsub.Message) error {
	return errNackCalled
}

// pslAckHandler is the AckHandler for Pub/Sub Lite.
type pslAckHandler struct {
	ackh        wire.AckConsumer
	msg         *pubsub.Message
	nackh       NackHandler
	subInstance *subscriberInstance
}

func (ah *pslAckHandler) OnAck() {
	if ah.subInstance == nil {
		return
	}

	ah.ackh.Ack()
	ah.subInstance.OnAckHandled(ah.ackh)
	ah.subInstance = nil
}

func (ah *pslAckHandler) OnNack() {
	if ah.subInstance == nil {
		return
	}

	err := ah.nackh(ah.msg)
	if err != nil {
		// If the NackHandler returns an error, shut down the subscriber client.
		ah.subInstance.Terminate(err)
	} else {
		// If the NackHandler succeeds, just ack the message.
		ah.ackh.Ack()
	}
	ah.subInstance.OnAckHandled(ah.ackh)
	ah.subInstance = nil
}

// subscriberInstance wraps an instance of a wire.Subscriber and handles the
// translation of Pub/Sub Lite message protos to pubsub.Messages, as well as
// ack/nack handling.
type subscriberInstance struct {
	settings   ReceiveSettings
	receiver   MessageReceiverFunc
	recvCtx    context.Context    // Context passed to the receiver
	recvCancel context.CancelFunc // Corresponding cancel func for recvCtx
	wireSub    wire.Subscriber

	// Fields below must be guarded with mu.
	mu              sync.Mutex
	err             error
	shutdown        bool
	activeReceivers sync.WaitGroup
	outstandingAcks sync.WaitGroup
	acks            map[wire.AckConsumer]struct{}
}

func (si *subscriberInstance) onMessages(msgs []*wire.ReceivedMessage) {
	for _, m := range msgs {
		pslAckh := &pslAckHandler{
			ackh:        m.Ack,
			nackh:       si.settings.NackHandler,
			subInstance: si,
		}
		psMsg := pubsub.NewMessage(pslAckh)
		pslAckh.msg = psMsg
		if err := si.settings.MessageTransformer(m.Msg, psMsg); err != nil {
			si.Terminate(err)
			return
		}

		if !si.canDeliverMsg(m.Ack) {
			// The subscriber client is shutting down.
			return
		}
		si.receiver(si.recvCtx, psMsg)
		si.activeReceivers.Done()
	}
}

func (si *subscriberInstance) canDeliverMsg(ack wire.AckConsumer) bool {
	si.mu.Lock()
	defer si.mu.Unlock()

	if si.shutdown {
		return false
	}
	si.activeReceivers.Add(1)
	si.outstandingAcks.Add(1)
	var void struct{}
	si.acks[ack] = void
	return true
}

// waitForReceivers is a soft shutdown, which waits for all outstanding
// calls to the user-provided message receiver func to finish. No further
// messages will be delivered.
func (si *subscriberInstance) waitForReceivers() {
	si.mu.Lock()
	si.shutdown = true
	si.mu.Unlock()

	si.recvCancel() // Cancels recvCtx to notify message receiver funcs of shutdown
	si.activeReceivers.Wait()
}

// Terminate is a hard shutdown that will immediately stop the wire subscriber.
func (si *subscriberInstance) Terminate(err error) {
	si.mu.Lock()
	defer si.mu.Unlock()

	// Don't clobber original error.
	if si.err == nil {
		si.err = err
	}
	si.shutdown = true
	si.wireSub.Stop()
	si.recvCancel()

	// Release all outstanding acks so that we don't wait for them.
	for ack, _ := range si.acks {
		si.outstandingAcks.Done()
		// Safe to delete map entry during range loop:
		// https://golang.org/ref/spec#For_statements
		delete(si.acks, ack)
	}
}

func (si *subscriberInstance) OnAckHandled(ack wire.AckConsumer) {
	si.mu.Lock()
	defer si.mu.Unlock()

	if _, exists := si.acks[ack]; exists {
		si.outstandingAcks.Done()
		delete(si.acks, ack)
	}
}

// Wait for the subscriber to stop, or the context is done, whichever occurs
// first.
func (si *subscriberInstance) Wait(ctx context.Context) error {
	si.wireSub.Start()
	if err := si.wireSub.WaitStarted(); err != nil {
		return err
	}

	// Start a goroutine to monitor when the context is done.
	subscriberStopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			si.waitForReceivers()
			// Note: if Terminate() is called while waiting for receivers, the
			// outstanding acks will be released.
			si.outstandingAcks.Wait()
			si.wireSub.Stop()
		case <-subscriberStopped:
		}
	}()
	err := si.wireSub.WaitStopped()

	// End goroutine above if the wire subscriber terminated due to fatal error
	// and ctx is not done.
	close(subscriberStopped)
	// And also wait for all the receivers to finish.
	si.waitForReceivers()

	si.mu.Lock()
	defer si.mu.Unlock()

	if si.err != nil {
		return si.err
	}
	return err
}

// wireSubscriberFactory is a factory for creating wire subscribers, which can
// be overridden with a mock in unit tests.
type wireSubscriberFactory interface {
	New(wire.MessageReceiverFunc) (wire.Subscriber, error)
}

type wireSubscriberFactoryImpl struct {
	settings     wire.ReceiveSettings
	region       string
	subscription pubsublite.SubscriptionPath
	options      []option.ClientOption
}

func (f *wireSubscriberFactoryImpl) New(receiver wire.MessageReceiverFunc) (wire.Subscriber, error) {
	return wire.NewSubscriber(context.Background(), f.settings, receiver, f.region, f.subscription.String(), f.options...)
}

// MessageReceiverFunc handles messages sent by the Cloud Pub/Sub Lite service.
// The implementation must arrange for pubsub.Message.Ack() to be called after
// processing the message.
//
// The receiver func will be called from multiple goroutines if the subscriber
// is connected to multiple partitions. Only one call from any connected
// partition will be outstanding at a time, and blocking in this receiver
// callback will block the delivery of subsequent messages for the partition.
type MessageReceiverFunc func(context.Context, *pubsub.Message)

// SubscriberClient is a Cloud Pub/Sub Lite client to receive messages for a
// given subscription.
//
// See https://cloud.google.com/pubsub/lite/docs/subscribing for more
// information about receiving messages.
type SubscriberClient struct {
	settings       ReceiveSettings
	wireSubFactory wireSubscriberFactory

	// Fields below must be guarded with mu.
	mu            sync.Mutex
	receiveActive bool
}

// NewSubscriberClient creates a new Cloud Pub/Sub Lite client to receive
// messages for a given subscription.
//
// See https://cloud.google.com/pubsub/lite/docs/subscribing for more
// information about receiving messages.
func NewSubscriberClient(ctx context.Context, settings ReceiveSettings, subscription pubsublite.SubscriptionPath, opts ...option.ClientOption) (*SubscriberClient, error) {
	region, err := pubsublite.ZoneToRegion(subscription.Zone)
	if err != nil {
		return nil, err
	}
	factory := &wireSubscriberFactoryImpl{
		settings:     settings.toWireSettings(),
		region:       region,
		subscription: subscription,
		options:      opts,
	}
	return newSubscriberClient(factory, settings), nil
}

func newSubscriberClient(factory wireSubscriberFactory, settings ReceiveSettings) *SubscriberClient {
	subClient := &SubscriberClient{
		settings:       settings,
		wireSubFactory: factory,
	}
	if subClient.settings.MessageTransformer == nil {
		subClient.settings.MessageTransformer = transformReceivedMessage
	}
	if subClient.settings.NackHandler == nil {
		subClient.settings.NackHandler = handleNack
	}
	return subClient
}

// Receive calls f with the messages from the subscription. It blocks until ctx
// is done, or the service returns a non-retryable error.
//
// The standard way to terminate a Receive is to cancel its context:
//
//   cctx, cancel := context.WithCancel(ctx)
//   err := sub.Receive(cctx, callback)
//   // Call cancel from callback, or another goroutine.
//
// If there is a fatal service error, Receive returns that error after all of
// the outstanding calls to f have returned. If ctx is done, Receive returns nil
// after all of the outstanding calls to f have returned and all messages have
// been acknowledged.
//
// Receive calls f concurrently from multiple goroutines if the SubscriberClient
// is connected to multiple partitions. It is encouraged to process messages
// synchronously in f, even if that processing is relatively time-consuming.
//
// The context passed to f will be canceled when ctx is Done or there is a fatal
// service error.
//
// Each SubscriberClient may have only one invocation of Receive active at a
// time.
func (s *SubscriberClient) Receive(ctx context.Context, f MessageReceiverFunc) error {
	// Initialize a subscriber instance.
	subInstance, err := func() (*subscriberInstance, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.receiveActive {
			return nil, errDuplicateReceive
		}

		recvCtx, recvCancel := context.WithCancel(ctx)
		subInstance := &subscriberInstance{
			settings:   s.settings,
			recvCtx:    recvCtx,
			recvCancel: recvCancel,
			receiver:   f,
			acks:       make(map[wire.AckConsumer]struct{}),
		}

		// Note: ctx is not used to create the wire subscriber, because if it is
		// cancelled, the subscriber will not be able to perform graceful shutdown
		// (e.g. process acks and commit the final cursor offset).
		wireSub, err := s.wireSubFactory.New(subInstance.onMessages)
		if err != nil {
			return nil, err
		}

		subInstance.wireSub = wireSub
		s.receiveActive = true
		return subInstance, nil
	}()
	if err != nil {
		return err
	}

	defer func() {
		s.mu.Lock()
		s.receiveActive = false
		s.mu.Unlock()
	}()

	// Wait for the subscriber without mutex held. Overlapping Receive invocations
	// will return an error.
	return subInstance.Wait(ctx)
}
