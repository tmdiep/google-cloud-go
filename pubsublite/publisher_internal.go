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
	"fmt"
	"net/url"
	"reflect"
	"sync"

	"github.com/golang/protobuf/proto"
	"google.golang.org/api/option"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

type shutdownMode int

const (
	// Publisher is active.
	noShutdown shutdownMode = 0
	// Flush all pending messages before terminating the publisher. This mode
	// should be used when:
	// - The user calls Stop(terminate=false).
	// - A new message fails preconditions. This should block the publish of
	//   subsequent messages to ensure ordering, but ideally all prior messages
	//   should be published to avoid forcing the user to republish them.
	//   Republishing may result in duplicates if there were in-flight batches
	//   sent to the server, but the responses have not yet been received.
	flushPending shutdownMode = 1
	// Immediately terminate the publisher and error all in-flight batches and
	// pending messages in the bundler. This mode should be used when:
	// - The publish stream terminates with a non-retryable error.
	// - An inconsistency is detected in the server's publish responses.
	immediateShutdown shutdownMode = 2
)

// publishMetadata holds the results of a successfully published message.
type publishMetadata struct {
	partition int
	offset    int64
}

func (pm *publishMetadata) String() string {
	return fmt.Sprintf("%d:%d", pm.partition, pm.offset)
}

// publishResultFunc receives the outcome of a message publish.
type publishResultFunc func(pm *publishMetadata, err error)

type messageHolder struct {
	msg    *pb.PubSubMessage
	onDone publishResultFunc
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

// publisherTerminatedFunc is used to notify the parent of a partitionPublisher
// that it has terminated.
type publisherTerminatedFunc func(*partitionPublisher)

// partitionPublisher publishes messages to a single topic partition.
// Safe to call from multiple goroutines.
type partitionPublisher struct {
	// Immutable after creation.
	ctx          context.Context
	pubClient    *vkit.PublisherClient
	partition    int
	header       string
	initialReq   *pb.PublishRequest
	onTerminated publisherTerminatedFunc

	// Guards access to fields below.
	mu sync.Mutex

	stream   *retryableStream
	shutdown shutdownMode
	finalErr error

	// Used to batch messages.
	msgBundler *bundler.Bundler
	// Ordered list of In-flight batches of published messages. Results have not
	// yet been received from the stream.
	publishQueue          *list.List
	minExpectedNextOffset int64
	enableSendToStream    bool
	availableBufferBytes  int
}

func newPartitionPublisher(ctx context.Context, pubClient *vkit.PublisherClient, settings PublishSettings, topic TopicPath, partition int, terminated publisherTerminatedFunc) *partitionPublisher {
	publisher := &partitionPublisher{
		ctx:       ctx,
		pubClient: pubClient,
		partition: partition,
		header:    fmt.Sprintf("partition=%d&topic=%s", partition, url.QueryEscape(topic.String())),
		initialReq: &pb.PublishRequest{
			RequestType: &pb.PublishRequest_InitialRequest{
				InitialRequest: &pb.InitialPublishRequest{
					Topic:     topic.String(),
					Partition: int64(partition),
				},
			},
		},
		onTerminated:         terminated,
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
	msgBundler.HandlerLimit = 1 // Handle batches serially
	// The buffer size is managed by this publisher due to the in-flight publish
	// queue.
	msgBundler.BufferedByteLimit = settings.BufferedByteLimit * 10

	publisher.msgBundler = msgBundler
	publisher.stream = newRetryableStream(ctx, publisher, settings.Timeout, reflect.TypeOf(pb.PublishResponse{}))
	return publisher
}

// Start establishes a publish stream connection.
func (p *partitionPublisher) Start(result chan error) {
	p.stream.Start(result)
}

// Stop initiates shutdown of the publisher. If `immediate` is true, the
// publisher is shutdown immediately and all pending messages are errored.
// Otherwise a graceful shutdown will occur, where all pending messages are
// flushed.
func (p *partitionPublisher) Stop(immediate bool) {
	if immediate {
		p.initiateShutdown(immediateShutdown, status.Errorf(codes.Canceled, "pubsublite: user terminated publisher"))
	} else {
		p.initiateShutdown(flushPending, nil)
	}
}

func (p *partitionPublisher) Publish(msg *pb.PubSubMessage, onDone publishResultFunc) {
	processMessage := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		switch {
		case p.stream.Status() == streamUninitialized:
			return status.Errorf(codes.FailedPrecondition, "pubsublite: publisher has not been started")
		case p.shutdown > 0:
			return status.Errorf(codes.FailedPrecondition, "pubsublite: publisher has terminated or is terminating")
		default:
			break
		}

		msgSize := proto.Size(msg)
		if msgSize > MaxPublishMessageBytes {
			return status.Errorf(codes.FailedPrecondition, "pubsublite: serialized message size is %d bytes, maximum allowed size is MaxPublishMessageBytes (%d)", msgSize, MaxPublishMessageBytes)
		}
		if msgSize > p.availableBufferBytes {
			return ErrOverflow
		}
		if err := p.msgBundler.Add(&messageHolder{msg: msg, onDone: onDone, size: msgSize}, msgSize); err != nil {
			// As we've already checked the size of the message and overflow, the
			// bundler should not return an error.
			return status.Errorf(codes.Internal, "pubsublite: failed to batch message: %v", err)
		}
		p.availableBufferBytes -= msgSize
		return nil
	}

	// If the new message cannot be published, flush pending messages. These
	// operations cannot be called with the mutex held to avoid deadlocks.
	if err := processMessage(); err != nil {
		p.initiateShutdown(flushPending, err)
		onDone(nil, err)
	}
}

func (p *partitionPublisher) newStream(ctx context.Context) (grpc.ClientStream, error) {
	// TODO: Move this to util
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md["x-goog-request-params"] = []string{p.header}

	return p.pubClient.Publish(metadata.NewOutgoingContext(ctx, md))
}

func (p *partitionPublisher) initialRequest() interface{} {
	return p.initialReq
}

func (p *partitionPublisher) validateInitialResponse(response interface{}) error {
	pubResponse, _ := response.(*pb.PublishResponse)
	if pubResponse.GetInitialResponse() == nil {
		return status.Errorf(codes.Internal, "pubsublite: server returned invalid initial publish response")
	}
	return nil
}

func (p *partitionPublisher) onStreamStatusChange(status streamStatus) {
	fmt.Printf("onStreamStatusChange: %v\n", status)

	switch status {
	case streamConnected:
		p.mu.Lock()
		// To ensure messages are sent in order, we should send everything in
		// publishQueue to the stream first after reconnecting, before any new
		// batches.
		for elem := p.publishQueue.Front(); elem != nil; elem = elem.Next() {
			if batch, ok := elem.Value.(*publishBatch); ok {
				p.stream.Send(batch.toPublishRequest())
			}
		}
		p.enableSendToStream = true
		p.mu.Unlock()

	case streamReconnecting:
		p.mu.Lock()
		// This prevents handleBatch() from sending any new batches to the stream
		// before we've had a chance to send the queued batches above.
		p.enableSendToStream = false
		p.mu.Unlock()

	case streamTerminated:
		p.initiateShutdown(immediateShutdown, p.stream.Error())
		if p.onTerminated != nil {
			p.onTerminated(p)
		}

	default:
		// Unexpected state. Should not occur.
		break
	}
}

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
		// sent to the stream once the connection has been established.
		p.stream.Send(batch.toPublishRequest())
	}
}

func (p *partitionPublisher) onResponse(response interface{}) {
	processResponse := func() (*publishBatch, int64, error) {
		p.mu.Lock()
		defer p.mu.Unlock()

		pubResponse, _ := response.(*pb.PublishResponse)
		if pubResponse.GetMessageResponse() == nil {
			return nil, 0, status.Errorf(codes.Internal, "pubsublite: received unexpected publish response from server")
		}

		frontElem := p.publishQueue.Front()
		if frontElem == nil {
			return nil, 0, status.Errorf(codes.Internal, "pubsublite: received unexpected message response from server for no in-flight published messages")
		}

		firstOffset := pubResponse.GetMessageResponse().GetStartCursor().GetOffset()
		if firstOffset < p.minExpectedNextOffset {
			return nil, 0, status.Errorf(codes.Internal, "pubsublite: server returned message start offset = %d, expected >= %d", firstOffset, p.minExpectedNextOffset)
		}

		batch, _ := frontElem.Value.(*publishBatch)
		for _, msgHolder := range batch.msgHolders {
			p.availableBufferBytes += msgHolder.size
		}
		p.minExpectedNextOffset += int64(len(batch.msgHolders))
		p.publishQueue.Remove(frontElem)
		p.checkDone()
		return batch, firstOffset, nil
	}

	batch, firstOffset, err := processResponse()

	// These operations below need to be done without holding the mutex to avoid
	// deadlocks.
	if err != nil {
		p.initiateShutdown(immediateShutdown, err)
		return
	}

	for i, msgHolder := range batch.msgHolders {
		msgHolder.onDone(
			&publishMetadata{partition: p.partition, offset: firstOffset + int64(i)},
			nil)
	}
}

func (p *partitionPublisher) initiateShutdown(mode shutdownMode, err error) {
	p.mu.Lock()

	// Don't re-execute the same shutdown mode. However, allow immediate
	// termination while attempting to flush.
	if p.shutdown >= mode {
		p.mu.Unlock()
		return
	}

	p.shutdown = mode
	p.finalErr = err

	// Close the stream if this is an immediate shutdown. Otherwise leave it open
	// to send pending messages.
	if mode == immediateShutdown {
		p.enableSendToStream = false
		p.stream.Stop()
	}

	// Bundler.Flush invokes handleBatch() in a goroutine and blocks. Setting the
	// shutdown mode prevents any new messages from being added to the bundler.
	// handleBatch() also acquires the mutex, so it cannot be held here.
	p.mu.Unlock()
	p.msgBundler.Flush()

	p.mu.Lock()
	defer p.mu.Unlock()

	// If flushing, wait for the results of queued batches.
	if mode == flushPending {
		p.checkDone()
		return
	}

	// Otherwise set the error message for all pending messages and clear the
	// publish queue.
	for elem := p.publishQueue.Front(); elem != nil; elem = elem.Next() {
		if batch, ok := elem.Value.(*publishBatch); ok {
			for _, msgHolder := range batch.msgHolders {
				msgHolder.onDone(nil, err)
			}
		}
	}
	p.publishQueue.Init()
}

// checkDone must be called with partitionPublisher.mu held.
func (p *partitionPublisher) checkDone() {
	// If a shutdown was in progress, close the stream once all queued messages
	// have been published.
	if p.shutdown > 0 && p.publishQueue.Len() == 0 {
		fmt.Println("Done, shutting down")
		p.stream.Stop()
	}
}

type publisherHolder struct {
	pub        *partitionPublisher
	terminated bool
}

type routingPublisher struct {
	// Immutable after creation.
	ctx       context.Context
	pubClient *vkit.PublisherClient
	admin     *AdminClient
	topic     TopicPath
	settings  PublishSettings

	// Guards access to fields below.
	mu sync.Mutex

	msgRouter  messageRouter
	publishers map[int]*publisherHolder
	// Used to block until all partition publishers have terminated.
	waitTerminated *sync.WaitGroup
}

func newInternalPublisherClient(ctx context.Context, region string, opts ...option.ClientOption) (*vkit.PublisherClient, error) {
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
	pubClient, err := newInternalPublisherClient(ctx, region, opts...)
	if err != nil {
		return nil, err
	}
	admin, err := NewAdminClient(ctx, region, opts...)
	if err != nil {
		return nil, err
	}

	return &routingPublisher{
		ctx:            ctx,
		pubClient:      pubClient,
		admin:          admin,
		topic:          topic,
		settings:       settings,
		msgRouter:      msgRouter,
		publishers:     make(map[int]*publisherHolder),
		waitTerminated: new(sync.WaitGroup),
	}, nil
}

// No-op if already successfully started.
func (rp *routingPublisher) Start() error {
	publishers, err := rp.initPublishers()
	if publishers == nil {
		// Note: error is nil if already started.
		return err
	}

	partitionCount := len(publishers)
	startResults := make(chan error, partitionCount)
	for _, pub := range publishers {
		pub.Start(startResults)
	}
	for numStarted := 0; numStarted < partitionCount; numStarted++ {
		err = <-startResults
		if err != nil {
			break
		}
	}
	return err
}

func (rp *routingPublisher) Stop(immediate bool) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	for _, p := range rp.publishers {
		p.pub.Stop(immediate)
	}
}

func (rp *routingPublisher) Wait() {
	rp.waitTerminated.Wait()
}

func (rp *routingPublisher) Publish(msg *pb.PubSubMessage, onDone publishResultFunc) {
	pub, err := rp.routeToPublisher(msg)
	if err != nil {
		rp.Stop(false)
		onDone(nil, err)
		return
	}
	pub.Publish(msg, onDone)
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
		return nil, status.Errorf(codes.Internal, "pubsublite: topic has unexpected number of partitions %d\n", partitionCount)
	}

	var publishers []*partitionPublisher
	for i := 0; i < partitionCount; i++ {
		pub := newPartitionPublisher(rp.ctx, rp.pubClient, rp.settings, rp.topic, i, rp.onPublisherTerminated)
		publishers = append(publishers, pub)
		rp.publishers[i] = &publisherHolder{pub: pub}
	}

	rp.msgRouter.SetPartitionCount(partitionCount)
	rp.waitTerminated.Add(partitionCount)
	return publishers, nil
}

func (rp *routingPublisher) routeToPublisher(msg *pb.PubSubMessage) (*partitionPublisher, error) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	if len(rp.publishers) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "pubsublite: publisher has not been started")
	}

	partition := rp.msgRouter.Route(msg.GetKey())
	p, ok := rp.publishers[partition]
	if !ok {
		// This indicates a bug.
		return nil, status.Errorf(codes.Internal, "pubsublite: publisher not found for partition %d", partition)
	}
	return p.pub, nil
}

func (rp *routingPublisher) onPublisherTerminated(pub *partitionPublisher) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	for _, p := range rp.publishers {
		if p.pub == pub {
			if !p.terminated {
				p.terminated = true
				rp.waitTerminated.Done()
			}
		} else if !p.terminated {
			// If a publisher terminates due to permanent error, stop them all, but
			// allow them to flush pending messages.
			p.pub.Stop(false)
		}
	}
}
