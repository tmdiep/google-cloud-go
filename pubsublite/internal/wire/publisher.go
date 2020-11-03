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
	"container/list"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/api/option"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errInvalidInitialPubResponse = errors.New("pubsublite: first response from server was not an initial response for publish")
	errInvalidMsgPubResponse     = errors.New("pubsublite: received invalid publish response from server")
	errPublishQueueEmpty         = errors.New("pubsublite: received publish response from server with no batches in flight")
)

// PublishMetadata holds the results of a published message.
type PublishMetadata struct {
	Partition int
	Offset    int64
}

func (pm *PublishMetadata) String() string {
	return fmt.Sprintf("%d:%d", pm.Partition, pm.Offset)
}

// PublishResultFunc receives the result of a publish.
type PublishResultFunc func(*PublishMetadata, error)

// Publisher is the client interface exported from this package for publishing
// messages.
type Publisher interface {
	Publish(*pb.PubSubMessage, PublishResultFunc)

	Start()
	WaitStarted() error
	Stop()
	WaitStopped() error
}

// NewPublisher creates a new client for publishing messages.
func NewPublisher(ctx context.Context, settings PublishSettings, region, topicPath string, opts ...option.ClientOption) (Publisher, error) {
	pubClient, err := newPublisherClient(ctx, region, opts...)
	if err != nil {
		return nil, err
	}
	adminClient, err := NewAdminClient(ctx, region, opts...)
	if err != nil {
		return nil, err
	}

	msgRouter := newDefaultMessageRouter(rand.New(rand.NewSource(time.Now().UnixNano())))
	pubFactory := &singlePartitionPublisherFactory{
		ctx:       ctx,
		pubClient: pubClient,
		settings:  settings,
		topicPath: topicPath,
	}
	return newRoutingPublisher(adminClient, msgRouter, pubFactory), nil
}

// messageHolder stores a message to be published, with associated metadata.
type messageHolder struct {
	msg      *pb.PubSubMessage
	size     int
	onResult PublishResultFunc
}

// publishBatch holds messages that are published in the same
// MessagePublishRequest.
type publishBatch struct {
	msgHolders []*messageHolder
}

func (b *publishBatch) ToPublishRequest() *pb.PublishRequest {
	msgs := make([]*pb.PubSubMessage, len(b.msgHolders))
	for i, holder := range b.msgHolders {
		msgs[i] = holder.msg
	}

	return &pb.PublishRequest{
		RequestType: &pb.PublishRequest_MessagePublishRequest{
			MessagePublishRequest: &pb.MessagePublishRequest{
				Messages: msgs,
			},
		},
	}
}

// singlePartitionPublisher publishes messages to a single topic partition.
//
// The life of a successfully published message is as follows:
// - Publish() receives the message from the user.
// - It is added to `msgBundler`, which performs batching in accordance with
//   user-configured PublishSettings.
// - handleBatch() receives new message batches from the bundler. The batch is
//   added to `publishQueue` and sent to the gRPC stream, if connected. If the
//   stream is currently reconnecting, the entire `publishQueue` is resent to
//   the stream immediately after it has reconnected within
//   onStreamStatusChange().
// - onResponse() receives the first cursor offset for the front batch in
//   `publishQueue`. It assigns the cursor offsets for each message and
//   releases the publish result to the user.
//
// See comments for unsafeInitiateShutdown() for error scenarios.
type singlePartitionPublisher struct {
	// Immutable after creation.
	pubClient  *vkit.PublisherClient
	topic      topicPartition
	initialReq *pb.PublishRequest

	// Fields below must be guarded with mutex.
	stream *retryableStream

	// Used to batch messages.
	msgBundler *bundler.Bundler
	// FIFO queue of in-flight batches of published messages. Results have not yet
	// been received from the server.
	publishQueue *list.List // Value = *publishBatch
	// Used for error checking, to ensure the server returns increasing offsets
	// for published messages.
	minExpectedNextOffset int64
	// The buffer size is managed by this publisher rather than the bundler due to
	// the in-flight publish queue.
	availableBufferBytes int
	enableSendToStream   bool

	abstractService
}

// singlePartitionPublisherFactory creates instances of singlePartitionPublisher
// for given partition numbers.
type singlePartitionPublisherFactory struct {
	ctx       context.Context
	pubClient *vkit.PublisherClient
	settings  PublishSettings
	topicPath string
}

func (f *singlePartitionPublisherFactory) New(partition int) *singlePartitionPublisher {
	pp := &singlePartitionPublisher{
		pubClient: f.pubClient,
		topic:     topicPartition{Path: f.topicPath, Partition: partition},
		initialReq: &pb.PublishRequest{
			RequestType: &pb.PublishRequest_InitialRequest{
				InitialRequest: &pb.InitialPublishRequest{
					Topic:     f.topicPath,
					Partition: int64(partition),
				},
			},
		},
		publishQueue:         list.New(),
		availableBufferBytes: f.settings.BufferedByteLimit,
	}

	msgBundler := bundler.NewBundler(&messageHolder{}, func(item interface{}) {
		msgs, _ := item.([]*messageHolder)
		pp.handleBatch(msgs)
	})
	msgBundler.DelayThreshold = f.settings.DelayThreshold
	msgBundler.BundleCountThreshold = f.settings.CountThreshold
	msgBundler.BundleByteThreshold = f.settings.ByteThreshold
	msgBundler.BundleByteLimit = MaxPublishRequestBytes
	msgBundler.HandlerLimit = 1                                      // Handle batches serially for ordering
	msgBundler.BufferedByteLimit = f.settings.BufferedByteLimit * 10 // Handled in the publisher

	pp.msgBundler = msgBundler
	pp.stream = newRetryableStream(f.ctx, pp, f.settings.Timeout, reflect.TypeOf(pb.PublishResponse{}))
	return pp
}

// Start attempts to establish a publish stream connection.
func (pp *singlePartitionPublisher) Start() {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	if pp.unsafeUpdateStatus(serviceStarting, nil) {
		pp.stream.Start()
	}
}

// Stop initiates shutdown of the publisher. All pending messages are flushed.
func (pp *singlePartitionPublisher) Stop() {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.unsafeInitiateShutdown(serviceTerminating, nil)
}

// Publish a pub/sub message.
func (pp *singlePartitionPublisher) Publish(msg *pb.PubSubMessage, onResult PublishResultFunc) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	processMessage := func() error {
		msgSize := proto.Size(msg)
		err := pp.unsafeCheckServiceStatus()
		switch {
		case err != nil:
			return err
		case msgSize > MaxPublishMessageBytes:
			return fmt.Errorf("pubsublite: serialized message size is %d bytes, maximum allowed size is MaxPublishMessageBytes (%d)", msgSize, MaxPublishMessageBytes)
		case msgSize > pp.availableBufferBytes:
			return ErrOverflow
		}

		holder := &messageHolder{msg: msg, size: msgSize, onResult: onResult}
		if err := pp.msgBundler.Add(holder, msgSize); err != nil {
			// As we've already checked the size of the message and overflow, the
			// bundler should not return an error.
			return fmt.Errorf("pubsublite: failed to batch message: %v", err)
		}
		pp.availableBufferBytes -= msgSize
		return nil
	}

	// If the new message cannot be published, flush pending messages and then
	// terminate the stream once results are received.
	if err := processMessage(); err != nil {
		pp.unsafeInitiateShutdown(serviceTerminating, err)
		onResult(nil, err)
	}
	return
}

func (pp *singlePartitionPublisher) newStream(ctx context.Context) (grpc.ClientStream, error) {
	return pp.pubClient.Publish(addTopicRoutingMetadata(ctx, pp.topic))
}

func (pp *singlePartitionPublisher) initialRequest() interface{} {
	return pp.initialReq
}

func (pp *singlePartitionPublisher) validateInitialResponse(response interface{}) error {
	pubResponse, _ := response.(*pb.PublishResponse)
	if pubResponse.GetInitialResponse() == nil {
		return errInvalidInitialPubResponse
	}
	return nil
}

func (pp *singlePartitionPublisher) onStreamStatusChange(status streamStatus) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	switch status {
	case streamReconnecting:
		// This prevents handleBatch() from sending any new batches to the stream
		// before we've had a chance to send the queued batches below.
		pp.enableSendToStream = false

	case streamConnected:
		pp.unsafeUpdateStatus(serviceActive, nil)

		// To ensure messages are sent in order, we should send everything in
		// publishQueue to the stream immediately after reconnecting, before any new
		// batches.
		reenableSend := true
		for elem := pp.publishQueue.Front(); elem != nil; elem = elem.Next() {
			if batch, ok := elem.Value.(*publishBatch); ok {
				// If an error occurs during send, the gRPC stream will close and the
				// retryableStream will transition to `streamReconnecting` or
				// `streamTerminated`.
				if !pp.stream.Send(batch.ToPublishRequest()) {
					reenableSend = false
					break
				}
			}
		}
		pp.enableSendToStream = reenableSend

	case streamTerminated:
		pp.unsafeInitiateShutdown(serviceTerminated, pp.stream.Error())
	}
}

// handleBatch is the bundler's handler func.
func (pp *singlePartitionPublisher) handleBatch(messages []*messageHolder) {
	if len(messages) == 0 {
		// This should not occur.
		return
	}

	pp.mu.Lock()
	defer pp.mu.Unlock()

	batch := &publishBatch{msgHolders: messages}
	pp.publishQueue.PushBack(batch)

	if pp.enableSendToStream {
		// Note: if the underlying stream is reconnecting or Send() fails, the
		// entire publish queue will be sent to the stream in order once the
		// connection has been established. Thus the return value is ignored.
		pp.stream.Send(batch.ToPublishRequest())
	}
}

func (pp *singlePartitionPublisher) onResponse(response interface{}) {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	processResponse := func() error {
		pubResponse, _ := response.(*pb.PublishResponse)
		if pubResponse.GetMessageResponse() == nil {
			return errInvalidMsgPubResponse
		}
		frontElem := pp.publishQueue.Front()
		if frontElem == nil {
			return errPublishQueueEmpty
		}
		firstOffset := pubResponse.GetMessageResponse().GetStartCursor().GetOffset()
		if firstOffset < pp.minExpectedNextOffset {
			return fmt.Errorf("pubsublite: server returned publish response with inconsistent start offset = %d, expected >= %d", firstOffset, pp.minExpectedNextOffset)
		}

		batch, _ := frontElem.Value.(*publishBatch)
		for i, msgHolder := range batch.msgHolders {
			// Messages are ordered, so the offset of each message is firstOffset + i.
			pm := &PublishMetadata{Partition: pp.topic.Partition, Offset: firstOffset + int64(i)}
			msgHolder.onResult(pm, nil)
			pp.availableBufferBytes += msgHolder.size
		}

		pp.minExpectedNextOffset = firstOffset + int64(len(batch.msgHolders))
		pp.publishQueue.Remove(frontElem)
		pp.unsafeCheckDone()
		return nil
	}
	if err := processResponse(); err != nil {
		pp.unsafeInitiateShutdown(serviceTerminated, err)
	}
}

// unsafeInitiateShutdown must be provided a target serviceStatus, which must be
// one of:
// * serviceTerminating: attempts to successfully publish all pending messages
//   before terminating the publisher. Occurs when:
//   - The user calls Stop().
//   - A new message fails preconditions. This should block the publish of
//     subsequent messages to ensure ordering, but ideally all prior messages
//     should be flushed to avoid forcing the user to republish them, as
//     this may result in duplicates if there were in-flight batches with
//     pending results.
// * serviceTerminated: immediately terminates the publisher and errors all
//   in-flight batches and pending messages in the bundler. Occurs when:
//   - The publish stream terminates with a non-retryable error.
//   - An inconsistency is detected in the server's publish responses. Assume
//     there is a bug on the server and terminate the publisher, as correct
//     processing of messages cannot be guaranteed.
func (pp *singlePartitionPublisher) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	if !pp.unsafeUpdateStatus(targetStatus, err) {
		return
	}

	// Close the stream if this is an immediate shutdown. Otherwise leave it open
	// to send pending messages.
	if targetStatus == serviceTerminated {
		pp.enableSendToStream = false
		pp.stream.Stop()
	}

	// msgBundler.Flush invokes handleBatch() in a goroutine and blocks.
	// handleBatch() also acquires the mutex, so it cannot be held here.
	// Updating the publisher status above prevents any new messages from being
	// added to the bundler after flush.
	pp.mu.Unlock()
	pp.msgBundler.Flush()
	pp.mu.Lock()

	// If flushing pending messages, close the stream if there's nothing left to
	// publish.
	if targetStatus == serviceTerminating {
		pp.unsafeCheckDone()
		return
	}

	// Otherwise set the error message for all pending messages and clear the
	// publish queue.
	for elem := pp.publishQueue.Front(); elem != nil; elem = elem.Next() {
		if batch, ok := elem.Value.(*publishBatch); ok {
			for _, msgHolder := range batch.msgHolders {
				msgHolder.onResult(nil, err)
			}
		}
	}
	pp.publishQueue.Init()
}

// unsafeCheckDone must be called with singlePartitionPublisher.mu held.
func (pp *singlePartitionPublisher) unsafeCheckDone() {
	// If a shutdown was in progress, close the stream once all queued messages
	// have been published.
	if pp.status == serviceTerminating && pp.publishQueue.Len() == 0 {
		pp.stream.Stop()
	}
}

type routingPublisher struct {
	// Immutable after creation.
	ctx         context.Context
	adminClient *vkit.AdminClient
	msgRouter   messageRouter
	topicPath   string
	pubFactory  *singlePartitionPublisherFactory

	// Fields below must be guarded with mutex.
	publishers map[int]*singlePartitionPublisher

	compositeService
}

func newRoutingPublisher(adminClient *vkit.AdminClient, msgRouter messageRouter, pubFactory *singlePartitionPublisherFactory) *routingPublisher {
	pub := &routingPublisher{
		ctx:         pubFactory.ctx,
		adminClient: adminClient,
		msgRouter:   msgRouter,
		topicPath:   pubFactory.topicPath,
		pubFactory:  pubFactory,
		publishers:  make(map[int]*singlePartitionPublisher),
	}
	pub.init()
	return pub
}

// No-op if already successfully started.
func (rp *routingPublisher) Start() {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if !rp.unsafeUpdateStatus(serviceStarting, nil) {
		// Already started.
		return
	}

	partitionCount, err := rp.partitionCount()
	if err != nil {
		rp.unsafeUpdateStatus(serviceTerminated, err)
		return
	}
	if partitionCount <= 0 {
		err := fmt.Errorf("pubsublite: topic has invalid number of partitions %d", partitionCount)
		rp.unsafeUpdateStatus(serviceTerminated, err)
		return
	}

	for i := 0; i < partitionCount; i++ {
		pub := rp.pubFactory.New(i)
		rp.publishers[i] = pub
		rp.unsafeAddServices(pub)
	}
	rp.msgRouter.SetPartitionCount(partitionCount)
}

func (rp *routingPublisher) Publish(msg *pb.PubSubMessage, onResult PublishResultFunc) {
	pub, err := rp.routeToPublisher(msg)
	if err != nil {
		onResult(nil, err)
		return
	}
	pub.Publish(msg, onResult)
}

func (rp *routingPublisher) partitionCount() (int, error) {
	partitions, err := rp.adminClient.GetTopicPartitions(rp.ctx, &pb.GetTopicPartitionsRequest{Name: rp.topicPath})
	if err != nil {
		return 0, err
	}
	return int(partitions.GetPartitionCount()), nil
}

func (rp *routingPublisher) routeToPublisher(msg *pb.PubSubMessage) (*singlePartitionPublisher, error) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if err := rp.unsafeCheckServiceStatus(); err != nil {
		return nil, err
	}

	partition := rp.msgRouter.Route(msg.GetKey())
	pub, ok := rp.publishers[partition]
	if !ok {
		// Should not occur. This indicates a bug in the client.
		err := fmt.Errorf("pubsublite: publisher not found for partition %d", partition)
		rp.unsafeInitiateShutdown(serviceTerminating, err)
		return nil, err
	}
	return pub, nil
}
