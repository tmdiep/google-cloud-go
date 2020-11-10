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
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errServerNoMessages                = errors.New("pubsublite: server delivered no messages")
	errInvalidInitialSubscribeResponse = errors.New("pubsublite: first response from server was not an initial response for subscribe")
	errInvalidSubscribeResponse        = errors.New("pubsublite: received invalid subscribe response from server")
	errNoInFlightSeek                  = errors.New("pubsublite: received seek response for no in-flight seek")
)

// MessageReceiverFunc receives a Pub/Sub message from a topic partition and an
// AckConsumer for acknowledging the message.
type MessageReceiverFunc func(*pb.SequencedMessage, AckConsumer)

// The frequency of sending batch flow control requests.
const batchFlowControlPeriod = 100 * time.Millisecond

// wireSubscriber directly wraps the subscribe client stream. It passes messages
// to the message receiver and manages flow control. Flow control tokens are
// batched and sent to the stream via a periodic background task, although it
// can be expedited if the user is rapidly acking messages.
//
// Client-initiated seek unsupported.
type wireSubscriber struct {
	// Immutable after creation.
	subsClient   *vkit.SubscriberClient
	settings     ReceiveSettings
	subscription subscriptionPartition
	initialReq   *pb.SubscribeRequest
	receiver     MessageReceiverFunc

	// Fields below must be guarded with mutex.
	stream          *retryableStream
	acks            *ackTracker
	offsetTracker   subscriberOffsetTracker
	flowControl     flowControlBatcher
	pollFlowControl *periodicTask
	seekInFlight    bool

	abstractService
}

func newWireSubscriber(ctx context.Context, subsClient *vkit.SubscriberClient, settings ReceiveSettings,
	receiver MessageReceiverFunc, subscription subscriptionPartition, acks *ackTracker, disableTasks bool) *wireSubscriber {

	s := &wireSubscriber{
		subsClient:   subsClient,
		settings:     settings,
		subscription: subscription,
		initialReq: &pb.SubscribeRequest{
			Request: &pb.SubscribeRequest_Initial{
				Initial: &pb.InitialSubscribeRequest{
					Subscription: subscription.Path,
					Partition:    int64(subscription.Partition),
				},
			},
		},
		receiver: receiver,
		acks:     acks,
	}
	s.stream = newRetryableStream(ctx, s, settings.Timeout, reflect.TypeOf(pb.SubscribeResponse{}))

	backgroundTask := s.sendBatchFlowControl
	if disableTasks {
		backgroundTask = func() {}
	}
	s.pollFlowControl = newPeriodicTask(batchFlowControlPeriod, backgroundTask)
	return s
}

// Start establishes a subscribe stream connection and initializes flow control
// tokens from ReceiveSettings.
func (s *wireSubscriber) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unsafeUpdateStatus(serviceStarting, nil) {
		s.stream.Start()
		s.pollFlowControl.Start()

		s.flowControl.OnClientFlow(flowControlTokens{
			Bytes:    int64(s.settings.MaxOutstandingBytes),
			Messages: int64(s.settings.MaxOutstandingMessages),
		})
	}
}

// Stop immediately terminates the subscribe stream.
func (s *wireSubscriber) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsafeInitiateShutdown(serviceTerminating, nil)
}

func (s *wireSubscriber) newStream(ctx context.Context) (grpc.ClientStream, error) {
	return s.subsClient.Subscribe(addSubscriptionRoutingMetadata(ctx, s.subscription))
}

func (s *wireSubscriber) initialRequest() (interface{}, bool) {
	return s.initialReq, true
}

func (s *wireSubscriber) validateInitialResponse(response interface{}) error {
	subscribeResponse, _ := response.(*pb.SubscribeResponse)
	if subscribeResponse.GetInitial() == nil {
		return errInvalidInitialSubscribeResponse
	}
	return nil
}

func (s *wireSubscriber) onStreamStatusChange(status streamStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch status {
	case streamConnected:
		s.unsafeUpdateStatus(serviceActive, nil)

		// Reinitialize when a new subscribe stream instance is connected.
		if seekReq := s.offsetTracker.RequestForRestart(); seekReq != nil {
			if s.stream.Send(&pb.SubscribeRequest{
				Request: &pb.SubscribeRequest_Seek{Seek: seekReq},
			}) {
				s.seekInFlight = true
			}
		}
		s.unsafeSendFlowControl(s.flowControl.RequestForRestart())
		s.pollFlowControl.Start()

	case streamReconnecting:
		s.seekInFlight = false
		s.pollFlowControl.Stop()

	case streamTerminated:
		s.unsafeInitiateShutdown(serviceTerminated, s.stream.Error())
	}
}

func (s *wireSubscriber) onResponse(response interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	processResponse := func() error {
		subscribeResponse, _ := response.(*pb.SubscribeResponse)
		switch {
		case subscribeResponse.GetMessages() != nil:
			return s.unsafeHandleMessageResponse(subscribeResponse.GetMessages())
		case subscribeResponse.GetSeek() != nil:
			return s.unsafeHandleSeekResponse(subscribeResponse.GetSeek())
		default:
			return errInvalidSubscribeResponse
		}
	}
	if err := processResponse(); err != nil {
		s.unsafeInitiateShutdown(serviceTerminated, err)
	}
}

func (s *wireSubscriber) unsafeHandleSeekResponse(response *pb.SeekResponse) error {
	if !s.seekInFlight {
		return errNoInFlightSeek
	}
	s.seekInFlight = false
	return nil
}

func (s *wireSubscriber) unsafeHandleMessageResponse(response *pb.MessageResponse) error {
	if len(response.Messages) == 0 {
		return errServerNoMessages
	}
	if err := s.offsetTracker.OnMessages(response.Messages); err != nil {
		return err
	}
	if err := s.flowControl.OnMessages(response.Messages); err != nil {
		return err
	}
	for _, msg := range response.Messages {
		// Register outstanding acks, which are primarily handled by the
		// `committer`.
		ack := newAckConsumer(msg.GetCursor().GetOffset(), msg.GetSizeBytes(), s.onAck)
		if err := s.acks.Push(ack); err != nil {
			return err
		}
		s.receiver(msg, ack)
	}
	return nil
}

func (s *wireSubscriber) onAck(ac *ackConsumer) {
	// Don't block the user's goroutine with potentially expensive ack processing.
	go s.onAckAsync(ac.MsgBytes)
}

func (s *wireSubscriber) onAckAsync(msgBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsafeAllowFlow(flowControlTokens{Bytes: msgBytes, Messages: 1})
}

// sendBatchFlowControl is called by the periodic background task.
func (s *wireSubscriber) sendBatchFlowControl() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsafeSendFlowControl(s.flowControl.ReleasePendingRequest())
}

func (s *wireSubscriber) unsafeAllowFlow(allow flowControlTokens) {
	s.flowControl.OnClientFlow(allow)
	if s.flowControl.ShouldExpediteBatchRequest() {
		s.unsafeSendFlowControl(s.flowControl.ReleasePendingRequest())
	}
}

func (s *wireSubscriber) unsafeSendFlowControl(req *pb.FlowControlRequest) {
	if req == nil {
		return
	}
	// Note: if Send() fails, the stream will be reconnected and
	// flowControlBatcher.RequestForRestart() will be sent when the stream
	// reconnects.
	s.stream.Send(&pb.SubscribeRequest{
		Request: &pb.SubscribeRequest_FlowControl{FlowControl: req},
	})
}

func (s *wireSubscriber) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	if !s.unsafeUpdateStatus(targetStatus, err) {
		return
	}
	// No data to send. Immediately terminate the stream.
	s.pollFlowControl.Stop()
	s.stream.Stop()
}

// singlePartitionSubscriber receives messages from a single topic partition.
// It requires 2 child services:
// - wireSubscriber to receive messages from the subscribe stream.
// - committer to commit cursor offsets to the streaming commit cursor stream.
type singlePartitionSubscriber struct {
	// These have their own mutexes.
	subscriber *wireSubscriber
	committer  *committer

	compositeService
}

type singlePartitionSubscriberFactory struct {
	ctx              context.Context
	subsClient       *vkit.SubscriberClient
	cursorClient     *vkit.CursorClient
	settings         ReceiveSettings
	subscriptionPath string
	receiver         MessageReceiverFunc
	disableTasks     bool
}

func (f *singlePartitionSubscriberFactory) New(partition int) *singlePartitionSubscriber {
	subscription := subscriptionPartition{Path: f.subscriptionPath, Partition: partition}
	acks := newAckTracker()
	commit := newCommitter(f.ctx, f.cursorClient, f.settings, subscription, acks, f.disableTasks)
	subs := newWireSubscriber(f.ctx, f.subsClient, f.settings, f.receiver, subscription, acks, f.disableTasks)
	ps := &singlePartitionSubscriber{
		committer:  commit,
		subscriber: subs,
	}
	ps.init()
	ps.unsafeAddServices(subs, commit)
	return ps
}

// multiPartitionSubscriber receives messages from a fixed set of topic
// partitions.
type multiPartitionSubscriber struct {
	// Immutable after creation.
	subscribers []*singlePartitionSubscriber

	compositeService
}

func newMultiPartitionSubscriber(subsFactory *singlePartitionSubscriberFactory) *multiPartitionSubscriber {
	ms := &multiPartitionSubscriber{}
	ms.init()

	for _, partition := range subsFactory.settings.Partitions {
		subscriber := subsFactory.New(partition)
		ms.subscribers = append(ms.subscribers, subscriber)
		ms.unsafeAddServices(subscriber)
	}
	return ms
}

// assigningSubscriber uses the Pub/Sub Lite partition assignment service to
// listen to its assigned partition numbers and dynamically add/remove
// singlePartitionSubscribers.
type assigningSubscriber struct {
	// Immutable after creation.
	subsFactory *singlePartitionSubscriberFactory
	assigner    *assigner

	// Fields below must be guarded with mutex.
	// Subscribers keyed by partition number. Updated as assignments change.
	subscribers map[int]*singlePartitionSubscriber

	compositeService
}

func newAssigningSubscriber(partitionClient *vkit.PartitionAssignmentClient, subsFactory *singlePartitionSubscriberFactory) (*assigningSubscriber, error) {
	as := &assigningSubscriber{
		subsFactory: subsFactory,
		subscribers: make(map[int]*singlePartitionSubscriber),
	}
	as.init()

	assigner, err := newAssigner(subsFactory.ctx, partitionClient, uuid.NewRandom, subsFactory.settings, subsFactory.subscriptionPath, as.handleAssignment)
	if err != nil {
		return nil, err
	}
	as.assigner = assigner
	as.unsafeAddServices(assigner)
	return as, nil
}

func (as *assigningSubscriber) handleAssignment(assignment *partitionAssignment) error {
	as.mu.Lock()
	defer as.mu.Unlock()

	// Handle new partitions.
	for _, partition := range assignment.Partitions() {
		if _, exists := as.subscribers[partition]; !exists {
			subscriber := as.subsFactory.New(partition)
			if err := as.unsafeAddServices(subscriber); err != nil {
				// Service is stopping/stopped.
				return err
			}
			as.subscribers[partition] = subscriber
			log.Printf("assigningSubscriber: added subscriber for partition %d\n", partition)
		}
	}

	// Handle removed partitions.
	for partition, subscriber := range as.subscribers {
		if !assignment.Contains(partition) {
			as.unsafeRemoveService(subscriber)
			// Safe to delete map entry during range loop:
			// https://golang.org/ref/spec#For_statements
			delete(as.subscribers, partition)
			log.Printf("assigningSubscriber: removed subscriber for partition %d\n", partition)
		}
	}
	return nil
}

// Subscriber is the client interface exported from this package for receiving
// messages.
type Subscriber interface {
	Start()
	WaitStarted() error
	Stop()
	WaitStopped() error
}

// NewSubscriber creates a new client for receiving messages.
func NewSubscriber(ctx context.Context, settings ReceiveSettings, receiver MessageReceiverFunc, region, subscriptionPath string, opts ...option.ClientOption) (Subscriber, error) {
	if err := ValidateRegion(region); err != nil {
		return nil, err
	}
	// TODO: validate settings
	subsClient, err := newSubscriberClient(ctx, region, opts...)
	if err != nil {
		return nil, err
	}
	cursorClient, err := newCursorClient(ctx, region, opts...)
	if err != nil {
		return nil, err
	}

	subsFactory := &singlePartitionSubscriberFactory{
		ctx:              ctx,
		subsClient:       subsClient,
		cursorClient:     cursorClient,
		settings:         settings,
		subscriptionPath: subscriptionPath,
		receiver:         receiver,
	}

	if len(settings.Partitions) > 0 {
		if err := validatePartitions(settings.Partitions); err != nil {
			return nil, err
		}
		return newMultiPartitionSubscriber(subsFactory), nil
	}

	partitionClient, err := newPartitionAssignmentClient(ctx, region, opts...)
	if err != nil {
		return nil, err
	}
	return newAssigningSubscriber(partitionClient, subsFactory)
}

func validatePartitions(partitions []int) error {
	partitionMap := make(map[int]struct{})
	for _, p := range partitions {
		if p < 0 {
			return fmt.Errorf("pubsublite: partition numbers are zero-indexed; invalid partition %d", p)
		}
		if _, exists := partitionMap[p]; exists {
			return fmt.Errorf("pubsublite: duplicate partition number %d", p)
		}
	}
	return nil
}
