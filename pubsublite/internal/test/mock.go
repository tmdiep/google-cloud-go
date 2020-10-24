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

package test

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

// waitTimeout specifies how long to block when waiting for events.
const waitTimeout = 30 * time.Second

// Server is a mock Pub/Sub Lite server that can be used for unit testing.
type Server struct {
	LiteServer *MockLiteServer
	gRPCServer *testutil.Server
}

// NewServer creates a new mock Pub/Sub Lite server.
func NewServer() (*Server, error) {
	srv, err := testutil.NewServer()
	if err != nil {
		return nil, err
	}
	liteServer := newMockLiteServer()
	pb.RegisterPublisherServiceServer(srv.Gsrv, liteServer)
	srv.Start()
	return &Server{LiteServer: liteServer, gRPCServer: srv}, nil
}

// Addr returns the address that the server is listening on.
func (s *Server) Addr() string {
	return s.gRPCServer.Addr
}

// Close shuts down the server and releases all resources.
func (s *Server) Close() {
	s.gRPCServer.Close()
}

type requestOrError struct {
	req interface{}
	err error
}

// ServerStreamHandler allows unit tests to inspect requests sent by the client
// to the server over a gRPC stream and to send fake responses back to the
// client.
type ServerStreamHandler struct {
	requestType reflect.Type
	ready       chan struct{}
	requests    chan *requestOrError

	mu     sync.Mutex
	stream grpc.ServerStream
}

// newServerStreamHandler creates a stream handler for a given request type.
func newServerStreamHandler(requestType reflect.Type) *ServerStreamHandler {
	return &ServerStreamHandler{
		requestType: requestType,
		ready:       make(chan struct{}),
		requests:    make(chan *requestOrError, 100),
	}
}

// Wait blocks and returns nil once the server has accepted the stream, or error
// when the timeout has expired.
func (s *ServerStreamHandler) Wait() error {
	select {
	case <-s.ready:
		return nil
	case <-time.After(waitTimeout):
		return status.Errorf(codes.DeadlineExceeded, "mockserver: server stream not opened within %v", waitTimeout)
	}
}

// NextRequest blocks and returns the next request received from the client, or
// error when the timeout has expired. The first request is always the initial
// request for the stream.
func (s *ServerStreamHandler) NextRequest(response interface{}) (interface{}, error) {
	select {
	case r := <-s.requests:
		return r.req, r.err
	case <-time.After(waitTimeout):
		return nil, status.Errorf(codes.DeadlineExceeded, "mockserver: server did not receive request within %v", waitTimeout)
	}
}

// SendResponse sends a fake response to the client.
func (s *ServerStreamHandler) SendResponse(response interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stream == nil {
		return status.Error(codes.FailedPrecondition, "mockserver: server stream uninitialized")
	}
	return s.stream.SendMsg(response)
}

func (s *ServerStreamHandler) setStream(stream grpc.ServerStream, initialRequest interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stream != nil {
		panic("mockserver: stream already initialized")
	}
	s.stream = stream
	close(s.ready)
	s.requests <- &requestOrError{req: initialRequest}
	go s.receive()
}

// receive ends when the stream receives an error.
func (s *ServerStreamHandler) receive() {
	for {
		req := reflect.New(s.requestType).Interface()
		err := s.stream.RecvMsg(req)
		if err != nil {
			s.requests <- &requestOrError{err: err}
			break
		}
		s.requests <- &requestOrError{req: req}
	}
}

// MockLiteServer is an in-memory mock implementation of a Pub/Sub Lite service,
// which allows unit tests to inspect requests received by the server and send
// fake responses.
type MockLiteServer struct {
	pb.PublisherServiceServer

	mu sync.Mutex

	publishStreamHandlers map[string]*ServerStreamHandler
}

func key(path string, partition int) string {
	return fmt.Sprintf("%s:%d", path, partition)
}

func newMockLiteServer() *MockLiteServer {
	return &MockLiteServer{
		publishStreamHandlers: make(map[string]*ServerStreamHandler),
	}
}

func (s *MockLiteServer) PublishStreamHandler(topicPath string, partition int) *ServerStreamHandler {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := key(topicPath, partition)
	h, ok := s.publishStreamHandlers[key]
	if ok {
		delete(s.publishStreamHandlers, key)
	} else {
		h = newServerStreamHandler(reflect.TypeOf(pb.PublishRequest{}))
		s.publishStreamHandlers[key] = h
	}
	return h
}

func (s *MockLiteServer) Publish(stream pb.PublisherService_PublishServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if len(req.GetInitialRequest().GetTopic()) == 0 {
		return status.Errorf(codes.InvalidArgument, "mockserver: received invalid initial publish request: %v", req)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := key(req.GetInitialRequest().GetTopic(), int(req.GetInitialRequest().GetPartition()))
	h, ok := s.publishStreamHandlers[key]
	if ok && h.stream == nil {
		h.setStream(stream, req)
	} else {
		h = newServerStreamHandler(reflect.TypeOf(pb.PublishRequest{}))
		h.setStream(stream, req)
		s.publishStreamHandlers[key] = h
	}
	return nil
}
