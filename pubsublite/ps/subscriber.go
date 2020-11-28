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
	ackh     wire.AckConsumer
	msg      *pubsub.Message
	nackh    NackHandler
	called   bool
	subProxy *subscriberProxy
}

func (ah *pslAckHandler) OnAck() {
	if ah.called {
		return
	}
	ah.called = true
	ah.ackh.Ack()
}

func (ah *pslAckHandler) OnNack() {
	if ah.called {
		return
	}
	ah.called = true

	err := ah.nackh(ah.msg)
	if err != nil {
		ah.subProxy.Terminate(err)
		return
	}

	// If the NackHandler succeeds, ack the message.
	ah.ackh.Ack()
}

// subscriberProxy wraps a reference to a subscriberInstance that can be cleared
// when it terminates. pslAckHandlers (attached to messages) will no longer hold
// references to the subscriberInstance, allowing it to be GC'ed.
type subscriberProxy struct {
	mu          sync.Mutex
	subInstance *subscriberInstance
}

func (sp *subscriberProxy) Clear() {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.subInstance = nil
}

func (sp *subscriberProxy) Terminate(err error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if sp.subInstance != nil {
		sp.subInstance.Terminate(err)
	}
}

// subscriberInstance wraps an instance of a wire.Subscriber and handles the
// translation of Pub/Sub Lite message protos to pubsub.Messages, as well as
// ack handling.
type subscriberInstance struct {
	settings ReceiveSettings
	receiver MessageReceiverFunc
	rctx     context.Context // Context passed to the receiver
	cancel   context.CancelFunc
	wireSub  wire.Subscriber
	subProxy *subscriberProxy

	// Fields below must be guarded with mutex.
	mu              sync.Mutex
	err             error
	shutdown        bool
	activeReceivers sync.WaitGroup
}

func (si *subscriberInstance) onMessages(msgs []*wire.ReceivedMessage) {
	for _, m := range msgs {
		pslAckh := &pslAckHandler{
			ackh:     m.Ack,
			nackh:    si.settings.NackHandler,
			subProxy: si.subProxy,
		}
		psMsg := pubsub.NewMessage(pslAckh)
		pslAckh.msg = psMsg
		if err := si.settings.MessageTransformer(m.Msg, psMsg); err != nil {
			si.Terminate(err)
			return
		}

		if !si.canDeliverMsg() {
			return
		}
		si.receiver(si.rctx, psMsg)
		si.activeReceivers.Done()
	}
}

func (si *subscriberInstance) canDeliverMsg() bool {
	si.mu.Lock()
	defer si.mu.Unlock()

	if si.shutdown {
		return false
	}
	si.activeReceivers.Add(1)
	return true
}

// stopReceivers is a soft shutdown, which waits for all outstanding calls to
// the user-provided message receiver func to finish. This allows the last
// delivered message(s) to be acked. No further messages will be delivered.
func (si *subscriberInstance) stopReceiversAndWait() {
	si.mu.Lock()
	si.shutdown = true
	si.mu.Unlock()

	si.cancel()
	si.activeReceivers.Wait()
}

// Terminate is a hard shutdown that will immediately stop the wire subscriber.
func (si *subscriberInstance) Terminate(err error) {
	si.cancel()
	si.wireSub.Stop()

	si.mu.Lock()
	defer si.mu.Unlock()

	// Don't clobber original error.
	if si.err == nil {
		si.err = err
	}
	si.shutdown = true
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
			si.stopReceiversAndWait()
			si.Terminate(nil)
		case <-subscriberStopped:
		}
	}()
	err := si.wireSub.WaitStopped()
	close(subscriberStopped) // Ends goroutine above
	si.subProxy.Clear()      // Clear references to self in message ack handlers

	// If the wire subscriber terminated due to a server error, wait for all the
	// receivers to finish.
	si.stopReceiversAndWait()

	si.mu.Lock()
	defer si.mu.Unlock()

	if si.err != nil {
		return si.err
	}
	return err
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
	// SubscriberClient behaves more like a factory that creates
	// subscriberInstances.
	settings     ReceiveSettings
	region       string
	subscription pubsublite.SubscriptionPath
	options      []option.ClientOption

	// Fields below must be guarded with mutex.
	mu            sync.Mutex
	receiveActive bool
}

// NewSubscriberClient creates a new Cloud Pub/Sub Lite client to messages for a
// given subscription.
//
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
// after all of the outstanding calls to f have returned. SubscriberClient does
// not wait for all messages to be acknowledged before Receive returns (i.e.
// messages that are acknowledged outside f).
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

		rctx, cancel := context.WithCancel(ctx)
		subInstance := &subscriberInstance{
			settings: s.settings,
			rctx:     rctx,
			cancel:   cancel,
			receiver: f,
		}

		// Note: ctx is not used to create the wire subscriber, because if it is
		// cancelled, the subscriber will not be able to perform graceful shutdown
		// (e.g. process acks and commit the final cursor offset).
		wireSub, err := wire.NewSubscriber(context.Background(), s.settings.toWireSettings(), subInstance.onMessages, s.region, s.subscription.String(), s.options...)
		if err != nil {
			return nil, err
		}

		subInstance.wireSub = wireSub
		subInstance.subProxy = &subscriberProxy{subInstance: subInstance}
		s.receiveActive = true
		return subInstance, nil
	}()
	if err != nil {
		return err
	}

	// Ensure receiveActive is reset when Receive ends.
	defer func() {
		s.mu.Lock()
		s.receiveActive = false
		s.mu.Unlock()
	}()

	// Wait for the subscriber without mutex held. Overlapping calls to Receive
	// will return an error.
	return subInstance.Wait(ctx)
}
