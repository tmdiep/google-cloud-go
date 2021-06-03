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
	"context"
	"errors"
	"io"
	"log"
	"reflect"
	"sync"
	"time"

	"golang.org/x/xerrors"
	"google.golang.org/grpc"

	gax "github.com/googleapis/gax-go/v2"
)

// streamStatus is the status of a retryableStream. A stream starts off
// uninitialized. While it is active, it can transition between reconnecting and
// connected due to retryable errors. When a permanent error occurs, the stream
// is terminated and cannot be reconnected.
type streamStatus int

const (
	streamUninitialized streamStatus = iota
	streamReconnecting
	streamResetState
	streamConnected
	streamTerminated
)

type initialResponseRequired bool
type notifyReset bool

// Abort a stream initialization attempt after this duration to mitigate server
// delays.
const defaultStreamInitTimeout = 1 * time.Minute

var errStreamInitTimeout = errors.New("pubsublite: stream initialization timed out")

// streamHandler provides hooks for different Pub/Sub Lite streaming APIs
// (e.g. publish, subscribe, streaming cursor, etc.) to use retryableStream.
// All Pub/Sub Lite streaming APIs implement a similar handshaking protocol,
// where an initial request and response must be transmitted before other
// requests can be sent over the stream.
//
// streamHandler methods must not be called while holding retryableStream.mu in
// order to prevent the streamHandler calling back into the retryableStream and
// deadlocking.
//
// If any streamHandler method implementations block, this will block the
// retryableStream.connectStream goroutine processing the underlying stream.
type streamHandler interface {
	// newStream implementations must create the client stream with the given
	// (cancellable) context.
	newStream(context.Context) (grpc.ClientStream, error)
	// initialRequest should return the initial request and whether an initial
	// response is expected.
	initialRequest() (interface{}, initialResponseRequired)
	validateInitialResponse(interface{}) error

	// onStreamStatusChange is used to notify stream handlers when the stream has
	// changed state.
	// - A `streamReconnecting` status change is fired before attempting to
	//   connect a new stream.
	// - A `streamResetState` status change may be fired if the stream should
	//   reset its state (due to receipt of the RESET signal from the server).
	// - A `streamConnected` status change is fired when the stream is
	//   successfully connected.
	// These are followed by onResponse() calls when responses are received from
	// the server. These events are guaranteed to occur in this order.
	//
	// A final `streamTerminated` status change is fired when a permanent error
	// occurs. retryableStream.Error() returns the error that caused the stream to
	// terminate.
	onStreamStatusChange(streamStatus)
	// onResponse forwards a response received on the stream to the stream
	// handler.
	onResponse(interface{})
}

// retryableStream is a wrapper around a bidirectional gRPC client stream to
// handle automatic reconnection when the stream breaks.
//
// The connectStream() goroutine handles each stream connection. terminate() can
// be called at any time, either by the client to force stream closure, or as a
// result of an unretryable error.
//
// Safe to call capitalized methods from multiple goroutines. All other methods
// are private implementation.
type retryableStream struct {
	// Immutable after creation.
	ctx               context.Context
	handler           streamHandler
	responseType      reflect.Type
	timeout           time.Duration
	streamInitTimeout time.Duration
	initReq           interface{}

	// Guards access to fields below.
	mu sync.Mutex

	// The current connected stream.
	stream grpc.ClientStream
	// Function to cancel the current stream (which may be reconnecting).
	cancelStream      context.CancelFunc
	status            streamStatus
	finalErr          error
	lastRecvErr       error
	lastRecvTime      time.Time
	lastSendErr       error
	lastSendTime      time.Time
	lastReconnectTime time.Time
	lastNewStreamTime time.Time
	initStatus        string
}

func (rs *retryableStream) LogState() {
	now := time.Now()
	warning := ""
	if now.Sub(rs.lastReconnectTime) > 6*time.Minute {
		warning = "[OVERDUE] "
	}
	log.Printf(" %slastInitStatus=%s | lastReconnect=%v,lastNewStream=%v | lastRecv=%v,err=%v | lastSend=%v,err=%v",
		warning, rs.initStatus,
		now.Sub(rs.lastReconnectTime),
		now.Sub(rs.lastNewStreamTime),
		now.Sub(rs.lastRecvTime),
		rs.lastRecvErr,
		now.Sub(rs.lastSendTime),
		rs.lastSendErr)
}

// newRetryableStream creates a new retryable stream wrapper. `timeout` is the
// maximum duration for reconnection. `responseType` is the type of the response
// proto received on the stream.
func newRetryableStream(ctx context.Context, handler streamHandler, timeout time.Duration, responseType reflect.Type) *retryableStream {
	initReq, _ := handler.initialRequest()
	return &retryableStream{
		ctx:               ctx,
		handler:           handler,
		responseType:      responseType,
		timeout:           timeout,
		streamInitTimeout: defaultStreamInitTimeout,
		initReq:           initReq,
	}
}

// Start establishes a stream connection. It is a no-op if the stream has
// already started.
func (rs *retryableStream) Start() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.status == streamUninitialized {
		go rs.connectStream(notifyReset(false))
	}
}

// Stop gracefully closes the stream without error.
func (rs *retryableStream) Stop() {
	rs.terminate(nil)
}

// Send attempts to send the request to the underlying stream and returns true
// if successfully sent. Returns false if an error occurred or a reconnection is
// in progress.
func (rs *retryableStream) Send(request interface{}) (sent bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.stream != nil {
		err := rs.stream.SendMsg(request)
		rs.lastSendTime = time.Now()
		rs.lastSendErr = err
		rs.initStatus = "sent msg"
		// Note: if SendMsg returns an error, the stream is aborted.
		switch {
		case err == nil:
			sent = true
		case err == io.EOF:
			// If SendMsg returns io.EOF, RecvMsg will return the status of the
			// stream. Nothing to do here.
			log.Printf("SendIOF: [%T] %v", rs.initReq, rs.initReq)
			break
		case isRetryableSendError(err):
			log.Printf("SendRetryable: [%T] %v", rs.initReq, rs.initReq)
			go rs.connectStream(notifyReset(false))
		default:
			log.Printf("SendFatalError: [%T] %v", rs.initReq, rs.initReq)
			rs.unsafeTerminate(err)
		}
	}
	return
}

// Status returns the current status of the retryable stream.
func (rs *retryableStream) Status() streamStatus {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.status
}

// Error returns the error that caused the stream to terminate. Can be nil if it
// was initiated by Stop().
func (rs *retryableStream) Error() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.finalErr
}

func (rs *retryableStream) currentStream() grpc.ClientStream {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.stream
}

// unsafeClearStream must be called with the retryableStream.mu locked.
func (rs *retryableStream) unsafeClearStream() {
	if rs.cancelStream != nil {
		// If the stream did not already abort due to error, this will abort it.
		rs.cancelStream()
		rs.cancelStream = nil
	}
	if rs.stream != nil {
		rs.stream = nil
	}
}

func (rs *retryableStream) setCancel(cancel context.CancelFunc) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.unsafeClearStream()
	rs.cancelStream = cancel
}

// connectStream attempts to establish a valid connection with the server. Due
// to the potential high latency, initNewStream() should not be done while
// holding retryableStream.mu. Hence we need to handle the stream being force
// terminated during reconnection.
//
// Intended to be called in a goroutine. It ends once the client stream closes.
func (rs *retryableStream) connectStream(notifyReset notifyReset) {
	canReconnect := func() bool {
		rs.mu.Lock()
		defer rs.mu.Unlock()

		if rs.status == streamReconnecting {
			// There can only be 1 goroutine reconnecting.
			return false
		}
		if rs.status == streamTerminated {
			return false
		}
		rs.status = streamReconnecting
		rs.unsafeClearStream()
		return true
	}
	if !canReconnect() {
		return
	}

	rs.handler.onStreamStatusChange(streamReconnecting)
	if notifyReset {
		rs.handler.onStreamStatusChange(streamResetState)
	}
	// Check whether handler terminated stream before reconnecting.
	if rs.Status() == streamTerminated {
		return
	}

	newStream, cancelFunc, err := rs.initNewStream()
	if err != nil {
		rs.terminate(err)
		return
	}

	connected := func() bool {
		rs.mu.Lock()
		defer rs.mu.Unlock()

		if rs.status == streamTerminated {
			rs.unsafeClearStream()
			return false
		}
		rs.status = streamConnected
		rs.stream = newStream
		rs.cancelStream = cancelFunc
		return true
	}
	if !connected() {
		return
	}

	rs.handler.onStreamStatusChange(streamConnected)
	rs.listen(newStream)
}

type initStatus int

const (
	initInProgress initStatus = iota
	initCanceled
	initSuccess
)

func (rs *retryableStream) initNewStream() (newStream grpc.ClientStream, cancelFunc context.CancelFunc, err error) {
	r := newStreamRetryer(rs.timeout)
	var initStatusMu sync.Mutex
	attempt := 0
	rs.lastReconnectTime = time.Now()
	rs.initStatus = "begin initNewStream"

	for {
		attempt++
		log.Printf("ConnectStart(%d): [%T] %v", attempt, rs.initReq, rs.initReq)

		backoff, shouldRetry := func() (time.Duration, bool) {
			var cctx context.Context
			cctx, cancelFunc = context.WithCancel(rs.ctx)
			// Store the cancel func to quickly cancel reconnecting if the stream is
			// terminated.
			rs.setCancel(cancelFunc)

			// Bounds the duration of the stream initialization attempt - from stream
			// creation to the initial response.
			status := initInProgress
			initTimer := time.AfterFunc(2*time.Minute, func() {
				initStatusMu.Lock()
				defer initStatusMu.Unlock()
				if status == initInProgress {
					rs.LogState()
					status = initCanceled
					cancelFunc()
					log.Fatalf("*** Init timer fired, checkpoint=%q, req=%T, %v", rs.initStatus, rs.initReq, rs.initReq)
				}
			})
			defer initTimer.Stop()

			resolveError := func(err error) error {
				initStatusMu.Lock()
				defer initStatusMu.Unlock()
				// If initTimer canceled the stream, the gRPC error would be Canceled
				// (non-retryable) and should be replaced.
				if status == initCanceled {
					return errStreamInitTimeout
				}
				status = initCanceled
				return err
			}

			rs.lastNewStreamTime = time.Now()
			rs.initStatus = "before newStream"
			newStream, err = rs.handler.newStream(cctx)
			rs.initStatus = "stream created"
			if err != nil {
				err = resolveError(err)
				return r.RetryRecv(err)
			}

			initReq, needsResponse := rs.handler.initialRequest()
			err = newStream.SendMsg(initReq)
			rs.initStatus = "sent initial request"
			rs.initReq = initReq
			rs.lastSendTime = time.Now()
			rs.lastSendErr = err
			if err != nil {
				err = resolveError(err)
				return r.RetrySend(err)
			}

			if needsResponse {
				rs.initStatus = "receiving initial response"
				response := reflect.New(rs.responseType).Interface()
				err = newStream.RecvMsg(response)
				rs.initStatus = "received initial response"
				rs.lastRecvTime = time.Now()
				rs.lastRecvErr = err
				if err != nil {
					if isStreamResetSignal(err) {
						rs.handler.onStreamStatusChange(streamResetState)
					}
					err = resolveError(err)
					return r.RetryRecv(err)
				}

				err = rs.handler.validateInitialResponse(response)
				rs.initStatus = "validated initial response"
				if err != nil {
					// An unexpected initial response from the server is a permanent error.
					cancelFunc()
					return 0, false
				}
			}

			initStatusMu.Lock()
			defer initStatusMu.Unlock()
			if status == initCanceled {
				// Unfortunately, the initTimer just fired, so the stream needs to be
				// reconnected.
				err = errStreamInitTimeout
				return r.RetryRecv(err)
			}
			// We have a valid connection and should break from the outer loop.
			status = initSuccess
			rs.initStatus = "init success"
			return 0, false
		}()

		if (shouldRetry || err != nil) && cancelFunc != nil {
			// Ensure that streams aren't leaked.
			cancelFunc()
			cancelFunc = nil
			newStream = nil
		}
		if !shouldRetry || rs.Status() == streamTerminated {
			if err != nil {
				log.Printf("ConnectFatalError(%d): [%T] %v: %v", attempt, rs.initReq, rs.initReq, err)
			}
			if rs.Status() == streamTerminated {
				log.Printf("ConnectStreamTerminated(%d): [%T] %v", attempt, rs.initReq, rs.initReq)
			}
			if newStream != nil {
				log.Printf("Connected(%d): [%T] %v", attempt, rs.initReq, rs.initReq)
			}
			break
		}
		log.Printf("ConnectRetryable(%d): [%T] %v: %v", attempt, rs.initReq, rs.initReq, err)
		if r.ExceededDeadline() {
			log.Printf("ConnectTimedOut(%d): [%T] %v", attempt, rs.initReq, rs.initReq)
			err = xerrors.Errorf("%v: %w", err, ErrBackendUnavailable)
			break
		}
		if err = gax.Sleep(rs.ctx, backoff); err != nil {
			log.Printf("ConnectGaxSleep(%d): [%T] %v: %v", attempt, rs.initReq, rs.initReq, err)
			break
		}
	}
	return
}

// listen receives responses from the current stream. It initiates reconnection
// upon retryable errors or terminates the stream upon permanent error.
func (rs *retryableStream) listen(recvStream grpc.ClientStream) {
	for {
		response := reflect.New(rs.responseType).Interface()
		err := recvStream.RecvMsg(response)
		rs.lastRecvTime = time.Now()
		rs.lastRecvErr = err
		rs.initStatus = "recv msg"

		// If the current stream has changed while listening, any errors or messages
		// received now are obsolete. Discard and end the goroutine. Assume the
		// stream has been cancelled elsewhere.
		if rs.currentStream() != recvStream {
			log.Printf("RecvObsolete: [%T] %v: %v", rs.initReq, rs.initReq, err)
			break
		}
		if err != nil {
			if isRetryableRecvError(err) {
				log.Printf("RecvRetryable: [%T] %v: %v", rs.initReq, rs.initReq, err)
				go rs.connectStream(notifyReset(isStreamResetSignal(err)))
			} else {
				log.Printf("RecvFatal: [%T] %v: %v", rs.initReq, rs.initReq, err)
				rs.terminate(err)
			}
			break
		}
		rs.handler.onResponse(response)
	}
}

// terminate forces the stream to terminate with the given error (can be nil)
// Is a no-op if the stream has already terminated.
func (rs *retryableStream) terminate(err error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.unsafeTerminate(err)
}

func (rs *retryableStream) unsafeTerminate(err error) {
	if rs.status == streamTerminated {
		return
	}
	rs.status = streamTerminated
	rs.finalErr = err
	rs.unsafeClearStream()

	// terminate can be called from within a streamHandler method with a lock
	// held. So notify from a goroutine to prevent deadlock.
	go rs.handler.onStreamStatusChange(streamTerminated)
}
