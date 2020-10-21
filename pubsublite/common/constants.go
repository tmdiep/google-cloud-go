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

package common

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
