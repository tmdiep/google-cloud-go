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
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// streamStatus is the state of a gRPC client stream. A stream starts off
// uninitialized. While it is active, it can transition between reconnecting and
// connected due to retryable errors. When a permanent error occurs, the stream
// is terminated and cannot be reconnected.
type streamStatus int

const (
	streamUninitialized streamStatus = 0
	streamReconnecting  streamStatus = 1
	streamConnected     streamStatus = 2
	streamTerminated    streamStatus = 3
)

// streamHandler provides hooks for different streaming RPCs (e.g. publish,
// subscribe, streaming cursor, etc) to use retryableStream. All Pub/Sub Lite
// streaming RPCs implement a similar handshaking protocol, where an initial
// request and response must be transmitted  before other requests can be sent
// over the stream.
//
// streamHandler methods must not be called while holding the retryableStream.mu
// in order to prevent the streamHandler calling back into the retryableStream
// and deadlocking.
type streamHandler interface {
	// newStream implementations must create the client stream with a cancellable
	// context. The CancelFunc will be called to close the stream.
	newStream() (grpc.ClientStream, context.CancelFunc, error)
	initialRequest() interface{}
	validateInitialResponse(interface{}) error

	// onStreamStatusChange is used to notify stream handlers when the stream has
	// changed state. In particular, the `streamTerminated` state must be handled.
	// Stream handlers should perform any necessary reset of state upon
	// `streamConnected`.
	onStreamStatusChange(streamStatus)
	// onResponse forwards a response received on the stream to the stream
	// handler.
	onResponse(interface{})
}

// retryableStream is a wrapper around a bidirectional gRPC client stream to
// handle reconnection when the stream breaks due to retryable errors.
type retryableStream struct {
	// Immutable after creation.
	handler      streamHandler
	responseType reflect.Type
	timeout      time.Duration

	// Guards access to fields below.
	mu sync.Mutex

	stream         grpc.ClientStream
	cancelStream   context.CancelFunc
	status         streamStatus
	terminateError error
}

func newRetryableStream(handler streamHandler, connectTimeout time.Duration, responseType reflect.Type) *retryableStream {
	return &retryableStream{
		handler:      handler,
		responseType: responseType,
		timeout:      connectTimeout,
	}
}

func (rs *retryableStream) Start(result chan error) {
	if rs.Status() != streamUninitialized {
		result <- status.Error(codes.Internal, "pubsublite: stream has already been started")
		return
	}
	go rs.reconnect(&result)
}

func (rs *retryableStream) Send(request interface{}) bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.stream != nil {
		fmt.Printf("Sending data to stream: %v\n", request)
		err := rs.stream.SendMsg(request)
		if err == nil {
			return true
		}

		// TODO: handle retries or fail with permanent error
		fmt.Printf("Send to stream failed: %v\n", err)
	}
	return false
}

func (rs *retryableStream) Terminate(err error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.status == streamTerminated {
		return
	}

	fmt.Printf("Stream terminated with error %v\n", err)

	rs.status = streamTerminated
	rs.terminateError = err
	rs.clearStream()

	// Terminate can be called from within a streamHandler method with a lock
	// held. So notify from a goroutine to prevent deadlock.
	go rs.handler.onStreamStatusChange(streamTerminated)
}

// Status returns the current status of the stream.
func (rs *retryableStream) Status() streamStatus {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.status
}

// Error returns the error that caused the stream to terminate.
func (rs *retryableStream) Error() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.terminateError
}

func (rs *retryableStream) currentStream() grpc.ClientStream {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.stream
}

// clearStream must be called with the retryableStream.mu locked.
func (rs *retryableStream) clearStream() {
	if rs.cancelStream != nil {
		rs.cancelStream()
		rs.cancelStream = nil
	}
	if rs.stream != nil {
		rs.stream = nil
	}
}

// Intended to be called in a goroutine.
func (rs *retryableStream) reconnect(result *chan error) {
	rs.mu.Lock()

	// There should not be more than 1 goroutine reconnecting.
	if rs.status == streamReconnecting {
		rs.mu.Unlock()
		return
	}
	rs.status = streamReconnecting
	rs.clearStream()
	rs.mu.Unlock()

	rs.handler.onStreamStatusChange(streamReconnecting)

	newStream, cancelFunc, err := rs.initNewStream()
	if err != nil {
		rs.Terminate(err)
	} else {
		rs.mu.Lock()
		rs.status = streamConnected
		rs.stream = newStream
		rs.cancelStream = cancelFunc
		rs.mu.Unlock()

		rs.handler.onStreamStatusChange(streamConnected)
		go rs.listen()
	}

	if result != nil {
		*result <- err
	}
}

func (rs *retryableStream) initNewStream() (newStream grpc.ClientStream, cancelFunc context.CancelFunc, err error) {
	defer func() {
		if err != nil && cancelFunc != nil {
			cancelFunc()
		}
	}()

	// TODO: handle retries and permanent errors
	newStream, cancelFunc, err = rs.handler.newStream()
	if err != nil {
		return
	}
	fmt.Printf("Sending initial request %v\n", rs.handler.initialRequest())
	if err = newStream.SendMsg(rs.handler.initialRequest()); err != nil {
		return
	}
	response := reflect.New(rs.responseType).Interface()
	if err = newStream.RecvMsg(response); err != nil {
		fmt.Printf("Initial stream recv got err %v\n", err)
		return
	}
	if err = rs.handler.validateInitialResponse(response); err != nil {
		// TODO: this is a permanent error
		return
	}
	return
}

// Intended to be called in a goroutine.
func (rs *retryableStream) listen() {
	recvStream := rs.currentStream()
	if recvStream == nil {
		return
	}

	for {
		response := reflect.New(rs.responseType).Interface()
		fmt.Printf("Waiting for response...\n")
		err := recvStream.RecvMsg(response)
		if err != nil {
			fmt.Printf("Stream recv got err %v\n", err)
		}
		fmt.Printf("Stream recv got response %v\n", response)

		// If the current stream has changed while listening, any errors or messages
		// received now are obsolete. Discard and end the goroutine.
		if rs.currentStream() != recvStream {
			break
		}

		if err != nil {
			// TODO: retry
			rs.Terminate(err)
			break
		}
		rs.handler.onResponse(response)
	}

	fmt.Println("listen goroutine terminated")
}
