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
Package pubsublite provides an AdminClient for managing topics and subscriptions
for Google Cloud Pub/Sub Lite. Subpackage pubsublite/ps provides clients for
publishing and receiving messages, with a similar API to Google Cloud Pub/Sub.

Google Cloud Pub/Sub Lite is a zonal, real-time messaging service that lets you
send and receive messages between independent applications. You can manually
configure the throughput and storage capacity.

More information about Google Cloud Pub/Sub Lite is available at
https://cloud.google.com/pubsub/lite.

Information about choosing between Google Cloud Pub/Sub vs Pub/Sub Lite is
available at https://cloud.google.com/pubsub/docs/choosing-pubsub-or-lite.

See https://godoc.org/cloud.google.com/go for authentication, timeouts,
connection pooling and similar aspects of this package.

Note: This library is in BETA. Backwards-incompatible changes may be made before
stable v1.0.0 is released.
*/
package pubsublite // import "cloud.google.com/go/pubsublite"
