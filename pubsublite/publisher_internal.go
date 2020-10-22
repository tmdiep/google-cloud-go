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
	noShutdown        shutdownMode = 0
	flushPending      shutdownMode = 1
	immediateShutdown shutdownMode = 2
)

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

// singlePartitionPublisher publishes messages to a single topic partition.
type singlePartitionPublisher struct {
	// Immutable after creation.
	ctx        context.Context
	pubClient  *vkit.PublisherClient
	partition  int
	header     string
	initialReq *pb.PublishRequest

	// Guards access to fields below.
	mu sync.Mutex

	stream   *retryableStream
	shutdown shutdownMode
	finalErr error

	// Used to batch messages.
	msgBundler *bundler.Bundler
	// In-flight batches of published messages. Results have not yet been
	// received from the stream.
	publishQueue          *list.List
	minExpectedNextOffset int64
	enableSendToStream    bool
	availableBufferBytes  int
}

func newSinglePartitionPublisher(ctx context.Context, pubClient *vkit.PublisherClient, settings PublishSettings, topicPath string, partition int) *singlePartitionPublisher {
	publisher := &singlePartitionPublisher{
		ctx:       ctx,
		pubClient: pubClient,
		partition: partition,
		header:    fmt.Sprintf("partition=%d&topic=%s", partition, url.QueryEscape(topicPath)),
		initialReq: &pb.PublishRequest{
			RequestType: &pb.PublishRequest_InitialRequest{
				InitialRequest: &pb.InitialPublishRequest{
					Topic:     topicPath,
					Partition: int64(partition),
				},
			},
		},
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

func (p *singlePartitionPublisher) Start(result chan error) {
	p.stream.Start(result)
}

func (p *singlePartitionPublisher) Publish(msg *pb.PubSubMessage, onDone publishResultFunc) {
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
			// This should not occur.
			return status.Errorf(codes.Internal, "pubsublite: failed to bundle message: %v", err)
		}
		p.availableBufferBytes -= msgSize
		return nil
	}

	// If there the new message cannot be published, flush pending messages to
	// avoid the user having to republish duplicates. These operations cannot be
	// called with the mutex held to avoid deadlocks.
	if err := processMessage(); err != nil {
		p.initiateShutdown(flushPending, err)
		onDone(nil, err)
	}
}

func (p *singlePartitionPublisher) Stop() {
	p.initiateShutdown(flushPending, nil)
}

func (p *singlePartitionPublisher) newStream(ctx context.Context) (grpc.ClientStream, error) {
	// TODO: Move this to util
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md["x-goog-request-params"] = []string{p.header}

	return p.pubClient.Publish(metadata.NewOutgoingContext(ctx, md))
}

func (p *singlePartitionPublisher) initialRequest() interface{} {
	return p.initialReq
}

func (p *singlePartitionPublisher) validateInitialResponse(response interface{}) error {
	pubResponse, _ := response.(*pb.PublishResponse)
	if pubResponse.GetInitialResponse() == nil {
		return status.Errorf(codes.Internal, "pubsublite: server returned invalid initial publish response")
	}
	return nil
}

func (p *singlePartitionPublisher) onStreamStatusChange(status streamStatus) {
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

	default:
		// Unexpected state. Should not occur.
		break
	}
}

func (p *singlePartitionPublisher) onResponse(response interface{}) {
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

func (p *singlePartitionPublisher) handleBatch(messages []*messageHolder) {
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

func (p *singlePartitionPublisher) initiateShutdown(mode shutdownMode, err error) {
	p.mu.Lock()

	if p.shutdown > 0 {
		// Already shutting down.
		p.mu.Unlock()
		return
	}

	// Setting the shutdown mode here will prevent any new messages from being
	// added to the bundler.
	p.shutdown = mode
	p.finalErr = err

	// Close the stream if this is an immediate shutdown. Otherwise leave it open
	// to send pending messages.
	if mode == immediateShutdown {
		p.enableSendToStream = false
		p.stream.Stop()
	}

	// Flush may invoke the bunder handler (in a goroutine), which also acquires
	// the mutex, so it cannot be held here.
	p.mu.Unlock()
	p.msgBundler.Flush()

	p.mu.Lock()
	defer p.mu.Unlock()

	// If there are no pending messages, the stream can be closed.
	if mode == flushPending {
		p.checkDone()
		return
	}

	// Otherwise set the error message for all queued messages and clear the
	// queue.
	for elem := p.publishQueue.Front(); elem != nil; elem = elem.Next() {
		if batch, ok := elem.Value.(*publishBatch); ok {
			for _, msgHolder := range batch.msgHolders {
				msgHolder.onDone(nil, err)
			}
		}
	}
	p.publishQueue.Init()
}

// checkDone must be called with singlePartitionPublisher.mu held.
func (p *singlePartitionPublisher) checkDone() {
	// If a shutdown was in progress, close the stream once all queued messages
	// have been published.
	if p.shutdown > 0 && p.publishQueue.Len() == 0 {
		fmt.Println("Done, shutting down")
		p.stream.Stop()
	}
}
