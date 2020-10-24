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
type PublishResult struct {
	ready    chan struct{}
	serverID string
	err      error
}

func newPublishResult() *PublishResult {
	return &PublishResult{ready: make(chan struct{})}
}

// Ready returns a channel that is closed when the result is ready.
// When the Ready channel is closed, Get is guaranteed not to block.
func (r *PublishResult) Ready() <-chan struct{} { return r.ready }

// Get returns the server-generated message ID and/or error result of a Publish
// call. Get blocks until the Publish call completes or the context is done.
func (r *PublishResult) Get(ctx context.Context) (serverID string, err error) {
	// If the result is already ready, return it even if the context is done.
	select {
	case <-r.Ready():
		return r.serverID, r.err
	default:
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-r.Ready():
		return r.serverID, r.err
	}
}

func (r *PublishResult) set(pm *publishMetadata, err error) {
	if pm != nil {
		r.serverID = pm.String()
	}
	r.err = err
	close(r.ready)
}

// publisher is an internal implementation of a Pub/Sub Lite publisher, which
// is closer to the wire protocol. PublisherClient provides a facade to match
// the Google Cloud Pub/Sub (pubsub) module.
type publisher interface {
	Start() error
	Stop(immediate bool)
	Wait()
	Publish(msg *pb.PubSubMessage, onDone publishResultFunc)
}

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
	return &PublisherClient{pub: pub}, nil
}

// Start attempts to establish publish streams and blocks until ready, or a
// permanent error has occurred. Start() needs to be called prior to Publish().
func (p *PublisherClient) Start() error {
	return p.pub.Start()
}

// Publish publishes `msg` to the topic asynchronously. Messages are batched and
// sent according to the client's PublishSettings. Publish never blocks.
//
// Publish returns a non-nil PublishResult which will be ready when the
// message has been sent (or has failed to be sent) to the server.
//
// Once Stop() has been called, future calls to Publish will immediately return
// a PublishResult with an error.
func (p *PublisherClient) Publish(ctx context.Context, msg *Message) (result *PublishResult) {
	result = newPublishResult()
	msgpb, err := msg.toProto()
	if err != nil {
		result.set(nil, err)
		return
	}
	p.pub.Publish(msgpb, result.set)
	return
}

// Stop sends all remaining published messages and closes publish streams.
// Returns once all outstanding messages have been sent or have failed to be
// sent.
func (p *PublisherClient) Stop() {
	p.pub.Stop(false)
	p.pub.Wait()
}
