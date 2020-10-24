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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrOverflow indicates that the publish buffers have overflowed. See
	// comments for PublishSettings.BufferedByteLimit.
	ErrOverflow = status.Error(codes.ResourceExhausted, "pubsublite: client-side publish buffers have overflowed")

	// ErrServiceUninitialized indicates that a service (e.g. publisher or
	// subscriber) cannot perform an operation because it is uninitialized.
	ErrServiceUninitialized = status.Error(codes.FailedPrecondition, "pubsublite: service must be started")

	// ErrServiceTerminated indicates that a service (e.g. publisher or
	// subscriber) cannot perform an operation because it has stoped or is in the
	// process of stopping.
	ErrServiceStopped = status.Error(codes.FailedPrecondition, "pubsublite: service has stopped or is stopping")
)
