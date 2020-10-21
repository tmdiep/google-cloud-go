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
	"io"
	"reflect"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gax "github.com/googleapis/gax-go/v2"
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
	newStream(context.Context) (grpc.ClientStream, error)
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
	ctx          context.Context
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

// newRetryableStream creates a new retryable stream wrapper. `timeout` is the
// maximum duration for reconnection. `responseType` is the type of the response
// proto received on the stream.
func newRetryableStream(ctx context.Context, handler streamHandler, timeout time.Duration, responseType reflect.Type) *retryableStream {
	return &retryableStream{
		ctx:          ctx,
		handler:      handler,
		responseType: responseType,
		timeout:      timeout,
	}
}

// Start establishes a stream connection and returns the result to the given
// channel.
func (rs *retryableStream) Start(result chan error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.status != streamUninitialized {
		result <- status.Error(codes.Internal, "pubsublite: stream has already been started")
		return
	}
	go rs.reconnect(&result)
}

// Send attempts to send the request to the underlying stream and returns true
// if successfully sent. Returns false if an error occurred or a reconnection is
// in progress.
func (rs *retryableStream) Send(request interface{}) (sent bool) {
	rs.mu.Lock()

	if rs.stream != nil {
		fmt.Printf("Sending data to stream: %v\n", request)
		err := rs.stream.SendMsg(request)
		switch {
		case err == nil:
			sent = true
		case err == io.EOF:
			// If SendMsg returns io.EOF, RecvMsg will return the status of the
			// stream. Nothing to do here.
			break
		case isRetryableSendError(err):
			go rs.reconnect(nil)
		default:
			rs.mu.Unlock() // Terminate acquires the mutex.
			rs.Terminate(err)
			return
		}
	}

	rs.mu.Unlock()
	return
}

// Terminate forces the stream to terminate with the given error (can be nil for
// user-initiated shutdown).
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

// reconnect attempts to establish a valid connection with the server. Due to
// the potential high latency, it should not be done while holding
// retryableStream.mu. Hence we need to handle the stream being force terminated
// during reconnection.
//
// Intended to be called in a goroutine. It ends once the connection has been
// established or the stream terminated.
func (rs *retryableStream) reconnect(result *chan error) {
	var outerErr error
	notifyResult := true
	defer func() {
		if result != nil && notifyResult {
			*result <- outerErr
		}
	}()

	canReconnect := func() bool {
		rs.mu.Lock()
		defer rs.mu.Unlock()

		if rs.status == streamReconnecting {
			// There cannot be more than 1 goroutine reconnecting.
			notifyResult = false
			return false
		}
		if rs.status == streamTerminated {
			outerErr = status.Error(codes.Aborted, "pubsublite: stream has stopped")
			return false
		}

		rs.status = streamReconnecting
		rs.clearStream()
		return true
	}
	if !canReconnect() {
		return
	}
	rs.handler.onStreamStatusChange(streamReconnecting)

	newStream, cancelFunc, outerErr := rs.initNewStream()
	if outerErr != nil {
		rs.Terminate(outerErr)
		return
	}

	connected := func() bool {
		rs.mu.Lock()
		defer rs.mu.Unlock()

		if rs.status == streamTerminated {
			outerErr = status.Error(codes.Aborted, "pubsublite: stream has stopped")
			return false
		}

		rs.status = streamConnected
		rs.stream = newStream
		rs.cancelStream = cancelFunc
		go rs.listen(newStream)
		return true
	}
	if !connected() {
		return
	}
	rs.handler.onStreamStatusChange(streamConnected)
}

func (rs *retryableStream) initNewStream() (newStream grpc.ClientStream, cancelFunc context.CancelFunc, err error) {
	r := newStreamRetryer(rs.timeout)
	for {
		backoff, shouldRetry := func() (time.Duration, bool) {
			defer func() {
				if err != nil && cancelFunc != nil {
					cancelFunc()
					cancelFunc = nil
					newStream = nil
				}
			}()

			var cctx context.Context
			cctx, cancelFunc = context.WithCancel(rs.ctx)
			newStream, err = rs.handler.newStream(cctx)
			if err != nil {
				return r.RetryRecv(err)
			}
			fmt.Printf("Sending initial request %v\n", rs.handler.initialRequest())
			if err = newStream.SendMsg(rs.handler.initialRequest()); err != nil {
				return r.RetrySend(err)
			}
			response := reflect.New(rs.responseType).Interface()
			if err = newStream.RecvMsg(response); err != nil {
				fmt.Printf("Initial stream recv got err %v\n", err)
				return r.RetryRecv(err)
			}
			if err = rs.handler.validateInitialResponse(response); err != nil {
				// An unexpected initial response from the server is a permanent error.
				return 0, false
			}

			// We have a valid connection and should break from the outer loop.
			return 0, false
		}()

		if !shouldRetry {
			break
		}
		if err = gax.Sleep(rs.ctx, backoff); err != nil {
			break
		}
	}
	return
}

// listen receives responses from the current stream. It initiates reconnection
// upon retryable errors or terminates the stream upon permanent error.
//
// Intended to be called in a goroutine. It ends when recvStream has closed.
func (rs *retryableStream) listen(recvStream grpc.ClientStream) {
	for {
		response := reflect.New(rs.responseType).Interface()
		fmt.Println("Waiting for response...")
		err := recvStream.RecvMsg(response)
		fmt.Printf("Stream recv got err=%v, response=%v\n", err, response)

		// If the current stream has changed while listening, any errors or messages
		// received now are obsolete. Discard and end the goroutine.
		if rs.currentStream() != recvStream {
			fmt.Println("Discarded stream recv")
			break
		}

		if err != nil {
			if isRetryableRecvError(err) {
				go rs.reconnect(nil)
			} else {
				rs.Terminate(err)
			}
			break
		}

		rs.handler.onResponse(response)
	}

	fmt.Println("listen goroutine terminated")
}
