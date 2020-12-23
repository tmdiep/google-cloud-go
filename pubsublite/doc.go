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

/*
Package pubsublite provides an easy way to publish and receive messages using
Google Cloud Pub/Sub Lite.

Google Cloud Pub/Sub Lite is designed to provide reliable, many-to-many,
asynchronous messaging between applications. Publisher applications can send
messages to a topic and other applications can subscribe to that topic to
receive the messages. By decoupling senders and receivers, Google Cloud Pub/Sub
allows developers to communicate between independently written applications.

Compared to Google Cloud Pub/Sub, Pub/Sub Lite provides partitioned zonal data
storage with predefined throughput and storage capacity. Guidance on how to
choose between Google Cloud Pub/Sub and Pub/Sub Lite is available at
https://cloud.google.com/pubsub/docs/choosing-pubsub-or-lite.

More information about Google Cloud Pub/Sub Lite is available at
https://cloud.google.com/pubsub/lite.

See https://godoc.org/cloud.google.com/go for authentication, timeouts,
connection pooling and similar aspects of this package.

Note: This library is in BETA. Backwards-incompatible changes may be made before
stable v1.0.0 is released.
*/
package pubsublite // import "cloud.google.com/go/pubsublite"
