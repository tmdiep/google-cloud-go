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
	flushShutdown     shutdownMode = 1
	immediateShutdown shutdownMode = 2
)

type messageHolder struct {
	Msg *pb.PubSubMessage
}

type publishBatch struct {
	MsgHolders []*messageHolder
}

func (b *publishBatch) ToPublishRequest() *pb.PublishRequest {
	msgs := make([]*pb.PubSubMessage, len(b.MsgHolders))
	for i, holder := range b.MsgHolders {
		msgs[i] = holder.Msg
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
	header     string
	initialReq *pb.PublishRequest

	// Guards access to fields below.
	mu sync.Mutex

	stream   *retryableStream
	shutdown shutdownMode

	// Used to batch messages.
	msgBundler *bundler.Bundler
	// In-flight batches of published messages. Results have not yet been
	// received.
	publishQueue *list.List
}

func newSinglePartitionPublisher(ctx context.Context, pubClient *vkit.PublisherClient, settings PublishSettings, topicPath string, partition int) *singlePartitionPublisher {
	publisher := &singlePartitionPublisher{
		ctx:       ctx,
		pubClient: pubClient,
		header:    fmt.Sprintf("partition=%d&topic=%s", partition, url.QueryEscape(topicPath)),
		initialReq: &pb.PublishRequest{
			RequestType: &pb.PublishRequest_InitialRequest{
				InitialRequest: &pb.InitialPublishRequest{
					Topic:     topicPath,
					Partition: int64(partition),
				},
			},
		},
		publishQueue: list.New(),
	}

	msgBundler := bundler.NewBundler(&messageHolder{}, func(item interface{}) {
		msgs, _ := item.([]*messageHolder)
		publisher.handleBatch(msgs)
	})
	msgBundler.DelayThreshold = settings.DelayThreshold
	msgBundler.BundleCountThreshold = settings.CountThreshold
	msgBundler.BundleByteThreshold = settings.ByteThreshold
	msgBundler.BundleByteLimit = MaxPublishRequestBytes
	msgBundler.BufferedByteLimit = settings.BufferedByteLimit * 10 // Managed by this publisher
	msgBundler.HandlerLimit = 1                                    // Handle batches serially

	publisher.msgBundler = msgBundler
	publisher.stream = newRetryableStream(ctx, publisher, settings.Timeout, reflect.TypeOf(pb.PublishResponse{}))
	return publisher
}

func (p *singlePartitionPublisher) Start(result chan error) {
	p.stream.Start(result)
}

func (p *singlePartitionPublisher) Publish(msg *pb.PubSubMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// TODO: error handling
	// Bundler error should not occur
	p.msgBundler.Add(&messageHolder{Msg: msg}, 1)
}

func (p *singlePartitionPublisher) Stop() {
	p.mu.Lock()

	if p.shutdown > 0 {
		// Already shutting down.
		p.mu.Unlock()
		return
	}

	// Setting the shutdown mode here will prevent any new messages from being
	// added to the bundler.
	p.shutdown = flushShutdown

	// Flush invokes the bunder handler (in a goroutine), so the mutex cannot be
	// held here.
	p.mu.Unlock()
	p.msgBundler.Flush()
	// TODO: terminate after flush succeeded. There may be no messages to flush.

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.publishQueue.Len() == 0 {
		p.stream.Terminate(nil)
	}
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
	pubResponse, ok := response.(*pb.PublishResponse)
	if !ok {
		// This should not occur.
		return status.Errorf(codes.Internal, "pubsublite: initial publish response has invalid type %s", reflect.TypeOf(response))
	}
	if pubResponse.GetInitialResponse() == nil {
		return status.Errorf(codes.Internal, "pubsublite: server returned invalid initial publish response")
	}
	return nil
}

func (p *singlePartitionPublisher) onStreamStatusChange(status streamStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// TODO
	fmt.Printf("onStreamStatusChange: %v\n", status)
	if status == streamTerminated {
		// TODO
	} else {
		// If graceful shutdown was in progress, terminate the stream once all queued
		// messages have been published.
		if p.shutdown > 0 && p.publishQueue.Len() == 0 {
			fmt.Println("Done, shutting down")
			p.stream.Terminate(nil)
		}
	}
}

func (p *singlePartitionPublisher) onResponse(response interface{}) {
	pubResponse, ok := response.(*pb.PublishResponse)
	if !ok {
		// This should not occur.
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// TODO: error handling.
	msgResponse := pubResponse.GetMessageResponse()
	fmt.Printf("Successfully published messages, got start cursor: %v\n", msgResponse.GetStartCursor())
	frontBatch := p.publishQueue.Front()
	p.publishQueue.Remove(frontBatch)

	// If graceful shutdown was in progress, terminate the stream once all queued
	// messages have been published.
	if p.shutdown > 0 && p.publishQueue.Len() == 0 {
		fmt.Println("Done, shutting down")
		p.stream.Terminate(nil)
	}
}

func (p *singlePartitionPublisher) handleBatch(messages []*messageHolder) {
	if len(messages) == 0 {
		// This should not occur.
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	// TODO: shutdown and error handling.
	batch := &publishBatch{
		MsgHolders: messages,
	}
	p.publishQueue.PushBack(batch)
	// TODO: Prevent send to stream if reconnecting.
	// Note: if the stream is reconnecting, the entire publish queue will be sent
	// to the stream once the connection has been established.
	p.stream.Send(batch.ToPublishRequest())
}
