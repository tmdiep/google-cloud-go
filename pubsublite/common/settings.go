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

package common

import "time"

// PublishSettings control the batching of published messages.
type PublishSettings struct {
	// Publish a non-empty batch after this delay has passed.
	DelayThreshold time.Duration

	// Publish a batch when it has this many messages. The maximum is
	// MaxPublishRequestCount.
	CountThreshold int

	// Publish a batch when its size in bytes reaches this value. The maximum is
	// MaxPublishRequestBytes.
	ByteThreshold int

	// The maximum time that the client will attempt to establish a publish stream
	// connection to the server. The timeout is exceeded, the publisher will
	// terminate with the ErrConnectionTimeout error. Note that if the timeout
	// duration is long, ErrOverflow may occur first.
	Timeout time.Duration

	// The maximum number of bytes that the publisher will keep in memory before
	// returning ErrOverflow.
	//
	// Note that Pub/Sub Lite topics are provisioned a publishing throughput
	// capacity, per partition, shared by all publisher clients. Setting a large
	// buffer size can mitigate transient publish spikes. However, consistently
	// attempting to publish messages at a much higher rate than the publishing
	// throughput capacity can cause the buffers to overflow. For more
	// information, see https://cloud.google.com/pubsub/lite/docs/topics.
	BufferedByteLimit int
}

// DefaultPublishSettings holds the default values for PublishSettings.
var DefaultPublishSettings = PublishSettings{
	DelayThreshold: 10 * time.Millisecond,
	CountThreshold: 100,
	ByteThreshold:  1e6,
	Timeout:        60 * time.Second,
	// By default, limit the buffer 10 times the max publish request size. The
	// number 10 is chosen as a reasonable amount of messages in the worst case
	// whilst still capping the number to a low enough value to not OOM users.
	BufferedByteLimit: 10 * MaxPublishRequestBytes,
}
