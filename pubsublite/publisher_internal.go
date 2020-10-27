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
	"container/list"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/golang/protobuf/proto"
	"google.golang.org/api/option"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	errInvalidInitalPubResponse = errors.New("pubsublite: first response from server was not an initial response")
	errInvalidMsgPubResponse    = errors.New("pubsublite: received invalid publish response from server")
	errPublishQueueEmpty        = errors.New("pubsublite: received publish response from server with no batches in flight")
)

// publishMetadata holds the results of a published message. It implements the
// public PublishResult interface.
type publishMetadata struct {
	ready     chan struct{}
	partition int
	offset    int64
	err       error
}

func newPublishMetadata() *publishMetadata {
	return &publishMetadata{ready: make(chan struct{})}
}

func newPublishMetadataWithError(err error) *publishMetadata {
	pm := newPublishMetadata()
	pm.error(err)
	return pm
}

func (pm *publishMetadata) Ready() <-chan struct{} { return pm.ready }

func (pm *publishMetadata) Get(ctx context.Context) (serverID string, err error) {
	// If the result is already ready, return it even if the context is done.
	select {
	case <-pm.ready:
		return pm.id(), pm.err
	default:
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-pm.ready:
		return pm.id(), pm.err
	}
}

func (pm *publishMetadata) id() string {
	if pm.err != nil {
		return ""
	}
	return fmt.Sprintf("%d:%d", pm.partition, pm.offset)
}

func (pm *publishMetadata) set(partition int, offset int64) {
	pm.partition = partition
	pm.offset = offset
	close(pm.ready)
}

func (pm *publishMetadata) error(err error) {
	pm.err = err
	close(pm.ready)
}

// messageHolder stores a message to be published, with associated metadata.
type messageHolder struct {
	msg    *pb.PubSubMessage
	result *publishMetadata
	size   int
}

// publishBatch holds messages that are published in the same
// MessagePublishRequest.
type publishBatch struct {
	msgHolders []*messageHolder
}

func (b *publishBatch) toPublishRequest() *pb.PublishRequest {
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

// publisherStatus captures the lifecycle of a publisher, in order. Note that
// some statuses may be skipped.
type publisherStatus int

const (
	// Publisher has not been started.
	publisherUninitialized publisherStatus = 0
	// Publisher is active and accepting messages. Note that the underlying stream
	// may be reconnecting due to retryable errors.
	publisherActive publisherStatus = 1
	// Publisher is gracefully shutting down by flushing all pending messages. No
	// new messages are accepted.
	publisherFlushing publisherStatus = 2
	// Publisher has terminated. No new messages are accepted.
	publisherTerminated publisherStatus = 3
)

// publisherStatusChangeFunc notifies the parent routingPublisher of status
// changes. `publisherFlushing` and `publisherTerminated` have an associated
// error. This error may be nil if the user called Stop().
type publisherStatusChangeFunc func(*partitionPublisher, publisherStatus, error)

// partitionPublisher publishes messages to a single topic partition.
//
// The life of a successfully published message is as follows:
// - Publish() receives the message from the user.
// - If a series of preconditions pass, it is added to `msgBundler`, which
//   performs batching as per the user-configured PublishSettings.
// - handleBatch() receives message batches from the bundler.
// - The batch is added to `publishQueue` and sent to the gRPC stream, if
//   connected. If the stream is currently reconnecting, the entire
//   `publishQueue` is resent to the stream immediately after it has
//   reconnected.
// - onResponse() receives the cursor offset of the first message of the front
//   batch of `publishQueue`. It assigns the cursor offsets for each message and
//   releases the publish result to the user.
//
// See comments for initiateShutdown() for error scenarios.
//
// Capitalized methods are safe to call from multiple goroutines. All other
// methods are private implementation.
type partitionPublisher struct {
	// Immutable after creation.
	ctx            context.Context
	pubClient      *vkit.PublisherClient
	topic          TopicPath
	partition      int
	header         string
	initialReq     *pb.PublishRequest
	onStatusChange publisherStatusChangeFunc

	// Guards access to fields below.
	mu sync.Mutex

	stream   *retryableStream
	status   publisherStatus
	finalErr error

	// Used to batch messages.
	msgBundler *bundler.Bundler
	// Ordered list of in-flight batches of published messages. Results have not
	// yet been received from the server.
	publishQueue          *list.List // Value = *publishBatch
	minExpectedNextOffset int64
	enableSendToStream    bool
	availableBufferBytes  int
}

func newPartitionPublisher(ctx context.Context, pubClient *vkit.PublisherClient, settings PublishSettings, topic TopicPath, partition int, onStatusChange publisherStatusChangeFunc) *partitionPublisher {
	publisher := &partitionPublisher{
		ctx:       ctx,
		pubClient: pubClient,
		topic:     topic,
		partition: partition,
		initialReq: &pb.PublishRequest{
			RequestType: &pb.PublishRequest_InitialRequest{
				InitialRequest: &pb.InitialPublishRequest{
					Topic:     topic.String(),
					Partition: int64(partition),
				},
			},
		},
		onStatusChange:       onStatusChange,
		publishQueue:         list.New(),
		availableBufferBytes: settings.BufferedByteLimit,
	}

	msgBundler := bundler.NewBundler(&messageHolder{}, func(item interface{}) {
		msgs, _ := item.([]*messageHolder)
		publisher.handleBatch(msgs)
	})
	msgBundler.DelayThreshold = settings.DelayThreshold
	msgBundler.BundleCountThreshold = settings.CountThreshold
	msgBundler.BundleByteThreshold = settings.ByteThreshold
	msgBundler.BundleByteLimit = MaxPublishRequestBytes
	msgBundler.HandlerLimit = 1 // Handle batches serially for ordering
	// The buffer size is managed by this publisher due to the in-flight publish
	// queue.
	msgBundler.BufferedByteLimit = settings.BufferedByteLimit * 10

	publisher.msgBundler = msgBundler
	publisher.stream = newRetryableStream(ctx, publisher, settings.Timeout, reflect.TypeOf(pb.PublishResponse{}))
	return publisher
}

// Start attempts to establish a publish stream connection.
func (p *partitionPublisher) Start() {
	p.stream.Start()
}

// Stop initiates shutdown of the publisher. All pending messages are flushed.
func (p *partitionPublisher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.initiateShutdown(publisherFlushing, nil)
}

// Error returns the error that the publisher terminated with (may be nil).
func (p *partitionPublisher) Error() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.finalErr
}

// Publish publishes a pub/sub message.
func (p *partitionPublisher) Publish(msg *pb.PubSubMessage) (result *publishMetadata) {
	p.mu.Lock()
	defer p.mu.Unlock()

	result = newPublishMetadata()

	processMessage := func() error {
		msgSize := proto.Size(msg)
		switch {
		case p.status == publisherUninitialized:
			return ErrServiceUninitialized
		case p.status > publisherActive:
			return ErrServiceStopped
		case msgSize > MaxPublishMessageBytes:
			return fmt.Errorf("pubsublite: serialized message size is %d bytes, maximum allowed size is MaxPublishMessageBytes (%d)", msgSize, MaxPublishMessageBytes)
		case msgSize > p.availableBufferBytes:
			return ErrOverflow
		}

		holder := &messageHolder{msg: msg, result: result, size: msgSize}
		if err := p.msgBundler.Add(holder, msgSize); err != nil {
			// As we've already checked the size of the message and overflow, the
			// bundler should not return an error.
			return fmt.Errorf("pubsublite: failed to batch message: %v", err)
		}
		p.availableBufferBytes -= msgSize
		return nil
	}

	// If the new message cannot be published, flush pending messages and then
	// terminate the stream.
	if err := processMessage(); err != nil {
		result.error(err)
		p.initiateShutdown(publisherFlushing, err)
	}
	return
}

func (p *partitionPublisher) newStream(ctx context.Context) (grpc.ClientStream, error) {
	return p.pubClient.Publish(addRoutingMetadataToContext(ctx, p.topic, p.partition))
}

func (p *partitionPublisher) initialRequest() interface{} {
	return p.initialReq
}

func (p *partitionPublisher) validateInitialResponse(response interface{}) error {
	pubResponse, _ := response.(*pb.PublishResponse)
	if pubResponse.GetInitialResponse() == nil {
		return errInvalidInitalPubResponse
	}
	return nil
}

// updateStatus must be called with partitionPublisher.mu held.
func (p *partitionPublisher) updateStatus(targetStatus publisherStatus, err error) bool {
	if p.status >= targetStatus {
		// Already at the same or later stage of the publisher lifecycle.
		return false
	}

	p.status = targetStatus
	if err != nil {
		// Prevent nil clobbering an original error.
		p.finalErr = err
	}
	if p.onStatusChange != nil {
		// Call back in a goroutine to prevent deadlocks if the parent is holding a
		// locked mutex.
		go p.onStatusChange(p, p.status, p.finalErr)
	}
	return true
}

func (p *partitionPublisher) onStreamStatusChange(status streamStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch status {
	case streamReconnecting:
		// This prevents handleBatch() from sending any new batches to the stream
		// before we've had a chance to send the queued batches below.
		p.enableSendToStream = false

	case streamConnected:
		p.updateStatus(publisherActive, nil)

		// To ensure messages are sent in order, we should send everything in
		// publishQueue to the stream immediately after reconnecting, before any new
		// batches.
		reenableSend := true
		for elem := p.publishQueue.Front(); elem != nil; elem = elem.Next() {
			if batch, ok := elem.Value.(*publishBatch); ok {
				// If an error occurs during send, the gRPC stream will close and the
				// retryableStream will transition to `streamReconnecting` or
				// `streamTerminated`.
				if !p.stream.Send(batch.toPublishRequest()) {
					reenableSend = false
					break
				}
			}
		}
		p.enableSendToStream = reenableSend

	case streamTerminated:
		p.initiateShutdown(publisherTerminated, p.stream.Error())
	}
}

// handleBatch is the bundler's handler func.
func (p *partitionPublisher) handleBatch(messages []*messageHolder) {
	if len(messages) == 0 {
		// This should not occur.
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	batch := &publishBatch{msgHolders: messages}
	p.publishQueue.PushBack(batch)

	if p.enableSendToStream {
		// Note: if the stream is reconnecting, the entire publish queue will be
		// sent to the stream in order once the connection has been established.
		p.stream.Send(batch.toPublishRequest())
	}
}

// onResponse receives a server response from the stream.
func (p *partitionPublisher) onResponse(response interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()

	processResponse := func() error {
		pubResponse, _ := response.(*pb.PublishResponse)
		if pubResponse.GetMessageResponse() == nil {
			return errInvalidMsgPubResponse
		}
		frontElem := p.publishQueue.Front()
		if frontElem == nil {
			return errPublishQueueEmpty
		}
		firstOffset := pubResponse.GetMessageResponse().GetStartCursor().GetOffset()
		if firstOffset < p.minExpectedNextOffset {
			return fmt.Errorf("pubsublite: server returned publish response with inconsistent start offset = %d, expected >= %d", firstOffset, p.minExpectedNextOffset)
		}

		batch, _ := frontElem.Value.(*publishBatch)
		for i, msgHolder := range batch.msgHolders {
			msgHolder.result.set(p.partition, firstOffset+int64(i))
			p.availableBufferBytes += msgHolder.size
		}

		p.minExpectedNextOffset = firstOffset + int64(len(batch.msgHolders))
		p.publishQueue.Remove(frontElem)
		p.checkDone()
		return nil
	}

	if err := processResponse(); err != nil {
		p.initiateShutdown(publisherTerminated, err)
	}
}

// initiateShutdown must be provided a target publisher status, which must be
// one of:
// * publisherFlushing: attempts to successfully publish all pending messages
//   before terminating the publisher. Occurs when:
//   - The user calls Stop().
//   - A new message fails preconditions. This should block the publish of
//     subsequent messages to ensure ordering, but ideally all prior messages
//     should be flushed to avoid forcing the user to republish them, as
//     this may result in duplicates if there were in-flight batches with
//     pending results.
// * publisherTerminated: immediately terminates the publisher and errors all
//   in-flight batches and pending messages in the bundler. Occurs when:
//   - The publish stream terminates with a non-retryable error.
//   - An inconsistency is detected in the server's publish responses. Assume
//     there is a bug on the server and terminate the publisher, as correct
//     processing of messages cannot be guaranteed.
//
// initiateShutdown must be called with partitionPublisher.mu held.
func (p *partitionPublisher) initiateShutdown(targetStatus publisherStatus, err error) {
	if !p.updateStatus(targetStatus, err) {
		return
	}

	// Close the stream if this is an immediate shutdown. Otherwise leave it open
	// to send pending messages.
	if targetStatus == publisherTerminated {
		p.enableSendToStream = false
		p.stream.Stop()
	}

	// msgBundler.Flush invokes handleBatch() in a goroutine and blocks.
	// handleBatch() also acquires the mutex, so it cannot be held here.
	// Updating the publisher status above prevents any new messages from being
	// added to the bundler after flush.
	p.mu.Unlock()
	p.msgBundler.Flush()
	p.mu.Lock()

	// If flushing pending messages, close the stream if there's nothing left to
	// publish.
	if targetStatus == publisherFlushing {
		p.checkDone()
		return
	}

	// Otherwise set the error message for all pending messages and clear the
	// publish queue.
	for elem := p.publishQueue.Front(); elem != nil; elem = elem.Next() {
		if batch, ok := elem.Value.(*publishBatch); ok {
			for _, msgHolder := range batch.msgHolders {
				msgHolder.result.error(err)
			}
		}
	}
	p.publishQueue.Init()
}

// checkDone must be called with partitionPublisher.mu held.
func (p *partitionPublisher) checkDone() {
	// If a shutdown was in progress, close the stream once all queued messages
	// have been published.
	if p.status == publisherFlushing && p.publishQueue.Len() == 0 {
		p.stream.Stop()
	}
}

type publisherHolder struct {
	pub     *partitionPublisher
	stopped bool
}

type routingPublisher struct {
	// Immutable after creation.
	ctx       context.Context
	pubClient *vkit.PublisherClient
	admin     *AdminClient
	topic     TopicPath
	settings  PublishSettings
	// Used to block until all partition publishers have started or terminated.
	waitStarted    sync.WaitGroup
	waitTerminated sync.WaitGroup

	// Guards access to fields below.
	mu sync.Mutex

	msgRouter  messageRouter
	publishers map[int]*publisherHolder
	finalErr   error
}

func newPublisherClient(ctx context.Context, region string, opts ...option.ClientOption) (*vkit.PublisherClient, error) {
	if err := validateRegion(region); err != nil {
		return nil, err
	}
	options := defaultClientOptions(region)
	options = append(options, opts...)
	return vkit.NewPublisherClient(ctx, options...)
}

func newRoutingPublisher(ctx context.Context, msgRouter messageRouter, settings PublishSettings, topic TopicPath, opts ...option.ClientOption) (*routingPublisher, error) {
	region, err := ZoneToRegion(topic.Zone)
	if err != nil {
		return nil, err
	}
	pubClient, err := newPublisherClient(ctx, region, opts...)
	if err != nil {
		return nil, err
	}
	admin, err := NewAdminClient(ctx, region, opts...)
	if err != nil {
		return nil, err
	}

	return &routingPublisher{
		ctx:        ctx,
		pubClient:  pubClient,
		admin:      admin,
		topic:      topic,
		settings:   settings,
		msgRouter:  msgRouter,
		publishers: make(map[int]*publisherHolder),
	}, nil
}

// No-op if already successfully started.
func (rp *routingPublisher) Start() error {
	publishers, err := rp.initPublishers()
	if publishers == nil {
		// Note: error is nil if already started.
		return err
	}

	for _, pub := range publishers {
		pub.Start()
	}

	rp.waitStarted.Wait()
	return rp.Error()
}

// Stop stops all child partitionPublishers and waits for them to terminate.
func (rp *routingPublisher) Stop() {
	rp.mu.Lock()
	for _, p := range rp.publishers {
		p.pub.Stop()
	}
	rp.mu.Unlock()

	rp.waitTerminated.Wait()
}

func (rp *routingPublisher) Error() error {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	return rp.finalErr
}

func (rp *routingPublisher) Publish(msg *pb.PubSubMessage) *publishMetadata {
	pub, err := rp.routeToPublisher(msg)
	if err != nil {
		rp.Stop()
		return newPublishMetadataWithError(err)
	}
	return pub.Publish(msg)
}

func (rp *routingPublisher) initPublishers() ([]*partitionPublisher, error) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if len(rp.publishers) > 0 {
		// Already started.
		return nil, nil
	}

	partitionCount, err := rp.admin.TopicPartitions(rp.ctx, rp.topic)
	if err != nil {
		return nil, err
	}
	if partitionCount <= 0 {
		return nil, fmt.Errorf("pubsublite: topic has invalid number of partitions %d", partitionCount)
	}

	var publishers []*partitionPublisher
	for i := 0; i < partitionCount; i++ {
		pub := newPartitionPublisher(rp.ctx, rp.pubClient, rp.settings, rp.topic, i, rp.onPublisherStatusChange)
		publishers = append(publishers, pub)
		rp.publishers[i] = &publisherHolder{pub: pub}
	}

	rp.msgRouter.SetPartitionCount(partitionCount)
	rp.waitStarted.Add(partitionCount)
	rp.waitTerminated.Add(partitionCount)
	return publishers, nil
}

func (rp *routingPublisher) routeToPublisher(msg *pb.PubSubMessage) (*partitionPublisher, error) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if len(rp.publishers) == 0 {
		return nil, ErrServiceUninitialized
	}

	partition := rp.msgRouter.Route(msg.GetKey())
	p, ok := rp.publishers[partition]
	if !ok {
		// Should not occur. This indicates a bug in the client.
		return nil, fmt.Errorf("pubsublite: publisher not found for partition %d", partition)
	}
	return p.pub, nil
}

func (rp *routingPublisher) onPublisherStatusChange(pub *partitionPublisher, status publisherStatus, err error) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if status > publisherActive && rp.finalErr == nil {
		rp.finalErr = err
	}

	for _, p := range rp.publishers {
		if p.pub == pub {
			if status == publisherActive {
				rp.waitStarted.Done()
			}
			if status == publisherTerminated {
				rp.waitTerminated.Done()
			}
			if status > publisherActive {
				p.stopped = true
			}
		} else if status > publisherActive && !p.stopped {
			// If a single partitionPublisher terminates, stop them all, but allow
			// the others to flush pending messages.
			p.pub.Stop()
		}
	}
}
