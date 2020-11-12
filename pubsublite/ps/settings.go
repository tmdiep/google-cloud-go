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
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/internal/wire"
)

// ErrOverflow occurrs when publish buffers overflow.
var ErrOverflow = wire.ErrOverflow

// KeyExtractorFunc is a function that extracts an ordering key from a Message.
type KeyExtractorFunc func(*pubsub.Message) []byte

// PublishMessageTransformerFunc transforms a Message to a PubSubMessage API
// proto.
type PublishMessageTransformerFunc func(*pubsub.Message) (*pb.PubSubMessage, error)

// PublishSettings control the batching of published messages. These settings
// apply per partition.
type PublishSettings struct {
	// Publish a non-empty batch after this delay has passed. If 0,
	// DefaultPublishSettings.DelayThreshold will be used.
	DelayThreshold time.Duration

	// Publish a batch when it has this many messages. The maximum is
	// wire.MaxPublishRequestCount. If 0, DefaultPublishSettings.CountThreshold
	// will be used.
	CountThreshold int

	// Publish a batch when its size in bytes reaches this value. The maximum is
	// wire.MaxPublishRequestBytes. If 0, DefaultPublishSettings.ByteThreshold
	// will be used.
	ByteThreshold int

	// The maximum time that the client will attempt to establish a publish stream
	// connection to the server. If 0, DefaultPublishSettings.Timeout will be
	// used.
	//
	// The timeout is exceeded, the publisher will terminate with the last error
	// that occurred while trying to reconnect. Note that if the timeout duration
	// is long, ErrOverflow may occur first.
	Timeout time.Duration

	// The maximum number of bytes that the publisher will keep in memory before
	// returning ErrOverflow. If 0, DefaultPublishSettings.BufferedByteLimit will
	// be used.
	//
	// Note that Pub/Sub Lite topics are provisioned a publishing throughput
	// capacity, per partition, shared by all publisher clients. Setting a large
	// buffer size can mitigate transient publish spikes. However, consistently
	// attempting to publish messages at a much higher rate than the publishing
	// throughput capacity can cause the buffers to overflow. For more
	// information, see https://cloud.google.com/pubsub/lite/docs/topics.
	BufferedByteLimit int

	// Optional custom function that extracts an ordering key from a Message. The
	// default implementation extracts the key from Message.OrderingKey.
	KeyExtractor KeyExtractorFunc

	// Optional custom function that transforms a Message to a PubSubMessage API
	// proto.
	MessageTransformer PublishMessageTransformerFunc
}

// DefaultPublishSettings holds the default values for PublishSettings.
var DefaultPublishSettings = PublishSettings{
	DelayThreshold:    wire.DefaultPublishSettings.DelayThreshold,
	CountThreshold:    wire.DefaultPublishSettings.CountThreshold,
	ByteThreshold:     wire.DefaultPublishSettings.ByteThreshold,
	Timeout:           wire.DefaultPublishSettings.Timeout,
	BufferedByteLimit: wire.DefaultPublishSettings.BufferedByteLimit,
}

func (s *PublishSettings) toWireSettings() wire.PublishSettings {
	wireSettings := wire.DefaultPublishSettings // Copy
	if s.DelayThreshold > 0 {
		wireSettings.DelayThreshold = s.DelayThreshold
	}
	if s.CountThreshold > 0 {
		wireSettings.CountThreshold = s.CountThreshold
	}
	if s.ByteThreshold > 0 {
		wireSettings.ByteThreshold = s.ByteThreshold
	}
	if s.Timeout > 0 {
		wireSettings.Timeout = s.Timeout
	}
	if s.BufferedByteLimit > 0 {
		wireSettings.BufferedByteLimit = s.BufferedByteLimit
	}
	return wireSettings
}

// ReceiveSettings configure the Receive method.
type ReceiveSettings struct {
	// MaxOutstandingMessages is the maximum number of unprocessed messages
	// (unacknowledged but not yet expired). If MaxOutstandingMessages is 0, it
	// will be treated as if it were DefaultReceiveSettings.MaxOutstandingMessages.
	// If the value is negative, then there will be no limit on the number of
	// unprocessed messages.
	MaxOutstandingMessages int

	// MaxOutstandingBytes is the maximum size of unprocessed messages
	// (unacknowledged but not yet expired). If MaxOutstandingBytes is 0, it will
	// be treated as if it were DefaultReceiveSettings.MaxOutstandingBytes. If
	// the value is negative, then there will be no limit on the number of bytes
	// for unprocessed messages.
	MaxOutstandingBytes int

	// The maximum time that the client will attempt to establish a subscribe
	// stream connection to the server. If 0, DefaultReceiveSettings.Timeout will
	// be used.
	//
	// The timeout is exceeded, the publisher will terminate with the last error
	// that occurred while trying to reconnect. Note that if the timeout duration
	// is long, ErrOverflow may occur first.
	Timeout time.Duration

	// The topic partition numbers (zero-indexed) to receive messages from.
	// Values must be less than the number of partitions for the topic. If not
	// specified, the client will use the partition assignment service to
	// determine which partitions it should connect to.
	Partitions []int
}

// DefaultReceiveSettings holds the default values for ReceiveSettings.
var DefaultReceiveSettings = ReceiveSettings{
	MaxOutstandingMessages: wire.DefaultReceiveSettings.MaxOutstandingMessages,
	MaxOutstandingBytes:    wire.DefaultReceiveSettings.MaxOutstandingBytes,
	Timeout:                wire.DefaultReceiveSettings.Timeout,
}

func (s *ReceiveSettings) toWireSettings() wire.ReceiveSettings {
	wireSettings := wire.ReceiveSettings // Copy
	if s.MaxOutstandingMessages > 0 {
		wireSettings.MaxOutstandingMessages = s.MaxOutstandingMessages
	}
	if s.MaxOutstandingBytes > 0 {
		wireSettings.MaxOutstandingBytes = s.MaxOutstandingBytes
	}
	if s.Timeout > 0 {
		wireSettings.Timeout = s.Timeout
	}
	return wireSettings
}
