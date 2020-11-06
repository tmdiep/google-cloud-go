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
	"errors"
	"fmt"
	"time"
)

const (
	// MaxPublishRequestCount is the maximum number of messages that can be
	// batched in a single publish request.
	MaxPublishRequestCount = 1000

	// MaxPublishMessageBytes is the maximum allowed serialized size of a single
	// Pub/Sub message in bytes.
	MaxPublishMessageBytes = 1000000

	// MaxPublishRequestBytes is the maximum allowed serialized size of a single
	// publish request (containing a batch of messages) in bytes.
	MaxPublishRequestBytes = 3500000
)

// PublishSettings control the batching of published messages.
type PublishSettings struct {
	// Publish a non-empty batch after this delay has passed. Must be > 0.
	DelayThreshold time.Duration

	// Publish a batch when it has this many messages. Must be > 0. The maximum is
	// MaxPublishRequestCount.
	CountThreshold int

	// Publish a batch when its size in bytes reaches this value. Must be > 0. The
	// maximum is MaxPublishRequestBytes.
	ByteThreshold int

	// The maximum time that the client will attempt to establish a publish stream
	// connection to the server. Must be > 0.
	Timeout time.Duration

	// The maximum number of bytes that the publisher will keep in memory before
	// returning ErrOverflow. Must be > 0.
	BufferedByteLimit int
}

// DefaultPublishSettings holds the default values for PublishSettings.
var DefaultPublishSettings = PublishSettings{
	DelayThreshold: 10 * time.Millisecond,
	CountThreshold: 100,
	ByteThreshold:  1e6,
	Timeout:        10 * time.Minute,
	// By default set to a high limit that is not likely to occur, but prevents
	// OOM errors in clients.
	BufferedByteLimit: 1 << 30, // 1 GiB
}

// ReceiveSettings control the receiving of messages.
type ReceiveSettings struct {
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	Timeout                time.Duration
	Partitions             []int
}

// DefaultReceiveSettings holds the default values for ReceiveSettings.
var DefaultReceiveSettings = ReceiveSettings{
	MaxOutstandingMessages: 1000,
	MaxOutstandingBytes:    1e9,
	Timeout:                10 * time.Minute,
}

func validatePublishSettings(settings PublishSettings) error {
	if settings.DelayThreshold <= 0 {
		return errors.New("pubsublite: invalid publish settings. DelayThreshold duration must be > 0")
	}
	if settings.Timeout <= 0 {
		return errors.New("pubsublite: invalid publish settings. Timeout duration must be > 0")
	}
	if settings.CountThreshold <= 0 {
		return errors.New("pubsublite: invalid publish settings. CountThreshold must be > 0")
	}
	if settings.CountThreshold > MaxPublishRequestCount {
		return fmt.Errorf("pubsublite: invalid publish settings. Maximum CountThreshold is MaxPublishRequestCount (%d)", MaxPublishRequestCount)
	}
	if settings.ByteThreshold <= 0 {
		return errors.New("pubsublite: invalid publish settings. ByteThreshold must be > 0")
	}
	if settings.ByteThreshold > MaxPublishRequestBytes {
		return fmt.Errorf("pubsublite: invalid publish settings. Maximum ByteThreshold is MaxPublishRequestBytes (%d)", MaxPublishRequestBytes)
	}
	if settings.BufferedByteLimit <= 0 {
		return errors.New("pubsublite: invalid publish settings. BufferedByteLimit must be > 0")
	}
	return nil
}
