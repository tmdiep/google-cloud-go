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
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func isRetryableCode(code codes.Code) {
	switch code {
	case codes.Aborted, codes.DeadlineExceeded, codes.Internal, codes.ResourceExhausted, codes.Unavailable:
		return true
	default:
		return false
	}
}

func isRetryableStreamError(err error) {
	if err == nil || err == io.EOF {
		return true
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	return isRetryableCode(s.Code())
}
