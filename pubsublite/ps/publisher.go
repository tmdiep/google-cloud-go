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
	"sync"

	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/common"
	pubsub "cloud.google.com/go/pubsublite/internal/pubsub"
	"cloud.google.com/go/pubsublite/internal/wire"
	"golang.org/x/xerrors"
	"google.golang.org/api/option"
	"google.golang.org/api/support/bundler"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	// ErrOverflow is set for a PublishResult when publish buffers overflow.
	ErrOverflow = bundler.ErrOverflow

	// ErrOversizedMessage is set for a PublishResult when a published message
	// exceeds MaxPublishMessageBytes.
	ErrOversizedMessage = bundler.ErrOversizedItem

	// ErrPublisherStopped is set for a PublishResult when a message cannot be
	// published because the publisher client has stopped. PublisherClient.Error()
	// returns the error that caused the publisher client to terminate (if any).
	ErrPublisherStopped = wire.ErrServiceStopped
)

// translateError transforms a subset of errors to what would be returned by the
// pubsub package.
func translateError(err error) error {
	if xerrors.Is(err, wire.ErrOversizedMessage) {
		return ErrOversizedMessage
	}
	if xerrors.Is(err, wire.ErrOverflow) {
		return ErrOverflow
	}
	return err
}

// PublisherClient is a Cloud Pub/Sub Lite client to publish messages to a given
// topic.
// See https://cloud.google.com/pubsub/lite/docs/publishing for more information
// about publishing.
type PublisherClient struct {
	settings PublishSettings
	wirePub  wire.Publisher

	mu  sync.Mutex
	err error
}

// NewPublisherClient creates a new Cloud Pub/Sub Lite client to publish
// messages to a given topic.
// See https://cloud.google.com/pubsub/lite/docs/publishing for more information
// about publishing.
func NewPublisherClient(ctx context.Context, settings PublishSettings, topic pubsublite.TopicPath, opts ...option.ClientOption) (*PublisherClient, error) {
	region, err := pubsublite.ZoneToRegion(topic.Zone)
	if err != nil {
		return nil, err
	}
	wirePub, err := wire.NewPublisher(ctx, settings.toWireSettings(), region, topic.String(), opts...)
	if err != nil {
		return nil, err
	}
	wirePub.Start()
	if err := wirePub.WaitStarted(); err != nil {
		return nil, err
	}
	return &PublisherClient{settings: settings, wirePub: wirePub}, nil
}

// Publish publishes `msg` to the topic asynchronously. Messages are batched and
// sent according to the client's PublishSettings. Publish never blocks.
//
// Publish returns a non-nil PublishResult which will be ready when the
// message has been sent (or has failed to be sent) to the server.
//
// Once Stop() has been called or the publisher has terminated due to an error,
// future calls to Publish will immediately return a PublishResult with error
// ErrPublisherStopped.
//
// The publisher for a single topic partition will terminate upon error to
// ensure ordering of messages. This will trigger publishers for other topic
// partitions to also terminate. Pending published messages will be flushed,
// where possible.
func (p *PublisherClient) Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult {
	result := pubsub.NewPublishResult()
	msgpb, err := p.transformMessage(msg)
	if err != nil {
		pubsub.SetPublishResult(result, "", err)
		p.setError(err)
		p.Stop()
		return result
	}

	p.wirePub.Publish(msgpb, func(pm *common.PublishMetadata, err error) {
		err = translateError(err)
		if pm != nil {
			pubsub.SetPublishResult(result, pm.String(), err)
		} else {
			pubsub.SetPublishResult(result, "", err)
		}
	})
	return result
}

// Stop sends all remaining published messages and closes publish streams.
// Returns once all outstanding messages have been sent or have failed to be
// sent.
func (p *PublisherClient) Stop() {
	p.wirePub.Stop()
	p.wirePub.WaitStopped()
}

// Error returns the error that caused the publisher client to terminate. It
// may be nil if Stop() was called. The error returned here typically contains
// more context than what would be set for PublishResult.
func (p *PublisherClient) Error() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.err != nil {
		return p.err
	}
	return p.wirePub.Error()
}

func (p *PublisherClient) setError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Don't clobber original error.
	if p.err == nil {
		p.err = err
	}
}

func (p *PublisherClient) transformMessage(msg *pubsub.Message) (*pb.PubSubMessage, error) {
	if p.settings.MessageTransformer != nil {
		return p.settings.MessageTransformer(msg)
	}

	keyExtractor := p.settings.KeyExtractor
	if keyExtractor == nil {
		keyExtractor = extractOrderingKey
	}
	return transformPublishedMessage(msg, keyExtractor)
}
