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
	"context"
	"math/rand"
	"time"

	"google.golang.org/api/option"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

// A PublishResult holds the result from a call to Publish.
type PublishResult interface {
	// Ready returns a channel that is closed when the result is ready. When the
	// Ready channel is closed, Get is guaranteed not to block.
	Ready() <-chan struct{}

	// Get returns the server-generated message ID and/or error result of a
	// Publish call. Get blocks until the Publish call completes or the context is
	// done.
	Get(ctx context.Context) (serverID string, err error)
}

// publisher is an internal implementation of a Pub/Sub Lite publisher.
// PublisherClient provides a facade to match the Google Cloud Pub/Sub (pubsub)
// module.
type publisher interface {
	Start() error
	Stop()
	Publish(msg *pb.PubSubMessage) *publishMetadata
}

// PublisherClient is a Cloud Pub/Sub Lite client to publish messages to a given
// topic.
type PublisherClient struct {
	pub publisher
}

// NewPublisherClient creates a new Cloud Pub/Sub Lite client to publish
// messages to a given topic.
// See https://cloud.google.com/pubsub/lite/docs/publishing for more information
// about publishing.
func NewPublisherClient(ctx context.Context, settings PublishSettings, topic TopicPath, opts ...option.ClientOption) (*PublisherClient, error) {
	msgRouter := newDefaultMessageRouter(rand.New(rand.NewSource(time.Now().UnixNano())))
	pub, err := newRoutingPublisher(ctx, msgRouter, settings, topic, opts...)
	if err != nil {
		return nil, err
	}
	if err := pub.Start(); err != nil {
		pub.Stop()
		return nil, err
	}
	return &PublisherClient{pub: pub}, nil
}

// Publish publishes `msg` to the topic asynchronously. Messages are batched and
// sent according to the client's PublishSettings. Publish never blocks.
//
// Publish returns a non-nil PublishResult which will be ready when the
// message has been sent (or has failed to be sent) to the server.
//
// Once Stop() has been called, future calls to Publish will immediately return
// a PublishResult with an error.
func (p *PublisherClient) Publish(ctx context.Context, msg *Message) PublishResult {
	msgpb, err := msg.toProto()
	if err != nil {
		return newPublishMetadataWithError(err)
	}
	return p.pub.Publish(msgpb)
}

// Stop sends all remaining published messages and closes publish streams.
// Returns once all outstanding messages have been sent or have failed to be
// sent.
func (p *PublisherClient) Stop() {
	p.pub.Stop()
}
