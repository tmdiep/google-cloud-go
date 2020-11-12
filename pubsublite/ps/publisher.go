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

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/internal/wire"
	"google.golang.org/api/option"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

// PublisherClient is a Cloud Pub/Sub Lite client to publish messages to a given
// topic.
type PublisherClient struct {
	settings PublishSettings
	pub      wire.Publisher
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
	pub, err := wire.NewPublisher(ctx, settings.toWireSettings(), region, topic.String(), opts...)
	if err != nil {
		return nil, err
	}
	pub.Start()
	if err := pub.WaitStarted(); err != nil {
		return nil, err
	}
	return &PublisherClient{settings: settings, pub: pub}, nil
}

// Publish publishes `msg` to the topic asynchronously. Messages are batched and
// sent according to the client's PublishSettings. Publish never blocks.
//
// Publish returns a non-nil PublishResult which will be ready when the
// message has been sent (or has failed to be sent) to the server.
//
// Once Stop() has been called, future calls to Publish will immediately return
// a PublishResult with an error.
func (p *PublisherClient) Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult {
	result, setResult := pubsub.NewPublishResult()
	msgpb, err := p.transformMessage(msg)
	if err != nil {
		setResult("", err)
		return result
	}
	p.pub.Publish(msgpb, func(pm *wire.PublishMetadata, err error) {
		if pm != nil {
			setResult(pm.String(), err)
		} else {
			setResult("", err)
		}
	})
	return result
}

// Stop sends all remaining published messages and closes publish streams.
// Returns once all outstanding messages have been sent or have failed to be
// sent.
func (p *PublisherClient) Stop() {
	p.pub.Stop()
	p.pub.WaitStopped()
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

// extractOrderingKey extracts the ordering key from the message for routing
// during publishing. It is the default KeyExtractorFunc implementation.
func extractOrderingKey(msg *pubsub.Message) []byte {
	if len(msg.OrderingKey) == 0 {
		return nil
	}
	return []byte(msg.OrderingKey)
}

// transformPublishedMessage is the default PublishMessageTransformerFunc
// implementation.
func transformPublishedMessage(m *pubsub.Message, extractKey KeyExtractorFunc) (*pb.PubSubMessage, error) {
	msgpb := &pb.PubSubMessage{
		Data: m.Data,
		Key:  extractKey(m),
	}

	if len(m.Attributes) > 0 {
		msgpb.Attributes = make(map[string]*pb.AttributeValues)
		for key, value := range m.Attributes {
			msgpb.Attributes[key] = &pb.AttributeValues{Values: [][]byte{[]byte(value)}}
		}
	}
	return msgpb, nil
}
