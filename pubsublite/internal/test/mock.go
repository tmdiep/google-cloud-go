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
	"container/list"
	"fmt"
	"io"
	"sync"
	"testing"

	"cloud.google.com/go/internal/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

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

type rpcMetadata struct {
	wantRequest interface{}
	retResponse interface{}
	retErr      error
}

type RPCVerifier struct {
	t *testing.T
	// An ordered list of expected RPCs.
	rpcs     *list.List
	numCalls int
}

func newRPCVerifier(t *testing.T) *RPCVerifier {
	return &RPCVerifier{
		t:        t,
		rpcs:     list.New(),
		numCalls: -1,
	}
}

func (v *RPCVerifier) Push(wantRequest interface{}, retResponse interface{}, retErr error) {
	v.rpcs.PushBack(&rpcMetadata{
		wantRequest: wantRequest,
		retResponse: retResponse,
		retErr:      retErr,
	})
}

func (v *RPCVerifier) Pop(gotRequest interface{}) (interface{}, error) {
	v.numCalls++
	elem := v.rpcs.Front()
	if elem == nil {
		v.t.Error("call(%d): unexpected request:\n%v", v.numCalls, gotRequest)
		return nil, status.Error(codes.FailedPrecondition, "mockserver: got unexpected request")
	}

	rpc, _ := elem.Value.(*rpcMetadata)
	v.rpcs.Remove(elem)

	if !testutil.Equal(gotRequest, rpc.wantRequest) {
		v.t.Errorf("call(%d): got request: %v\nwant request: %v", v.numCalls, gotRequest, rpc.wantRequest)
	}
	return rpc.retResponse, rpc.retErr
}

type streamVerifiers struct {
	t *testing.T
	// An ordered list of RPCVerifiers for each stream connection.
	verifiers  *list.List
	numStreams int
}

func newStreamVerifiers(t *testing.T) *streamVerifiers {
	return &streamVerifiers{
		t:          t,
		verifiers:  list.New(),
		numStreams: -1,
	}
}

func (v *streamVerifiers) Push(rv *RPCVerifier) {
	v.verifiers.PushBack(rv)
}

func (v *streamVerifiers) Pop() (*RPCVerifier, error) {
	v.numStreams++
	elem := v.verifiers.Front()
	if elem == nil {
		v.t.Error("stream(%d): unexpected connection with no verifiers", v.numStreams)
		return nil, status.Error(codes.FailedPrecondition, "mockserver: got unexpected stream connection")
	}

	rv, _ := elem.Value.(*RPCVerifier)
	v.verifiers.Remove(elem)
	return rv, nil
}

// MockLiteServer is an in-memory mock implementation of a Pub/Sub Lite service,
// which allows unit tests to inspect requests received by the server and send
// fake responses.
type MockLiteServer struct {
	pb.PublisherServiceServer

	mu sync.Mutex

	// Publish stream verifiers by topic & partition.
	publishVerifiers map[string]*streamVerifiers
}

func key(path string, partition int) string {
	return fmt.Sprintf("%s:%d", path, partition)
}

func newMockLiteServer() *MockLiteServer {
	return &MockLiteServer{
		publishVerifiers: make(map[string]*streamVerifiers),
	}
}

func (s *MockLiteServer) pushStreamVerifier(key string, v *RPCVerifier, verifiers map[string]*streamVerifiers) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sv, ok := verifiers[key]
	if !ok {
		sv = newStreamVerifiers(v.t)
		verifiers[key] = sv
	}
	sv.Push(v)
}

func (s *MockLiteServer) popStreamVerifier(key string, verifiers map[string]*streamVerifiers) (*RPCVerifier, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sv, ok := verifiers[key]
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "mockserver: unexpected connection with no verifiers")
	}
	return sv.Pop()
}

func (s *MockLiteServer) AddPublishVerifier(topic string, partition int, verifier *RPCVerifier) {
	s.pushStreamVerifier(key(topic, partition), verifier, s.publishVerifiers)
}

func (s *MockLiteServer) Publish(stream pb.PublisherService_PublishServer) error {
	req, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "mockserver: stream recv error before initial request: %v", err)
	}
	if len(req.GetInitialRequest().GetTopic()) == 0 {
		return status.Errorf(codes.InvalidArgument, "mockserver: received invalid initial publish request: %v", req)
	}

	initReq := req.GetInitialRequest()
	verifier, err := s.popStreamVerifier(
		key(initReq.GetTopic(), int(initReq.GetPartition())),
		s.publishVerifiers)
	if err != nil {
		return err
	}

	for {
		retResponse, retErr := verifier.Pop(req)
		if retErr != nil {
			return retErr
		}
		resp, _ := retResponse.(*pb.PublishResponse)
		if err = stream.Send(resp); err != nil {
			return status.Errorf(codes.FailedPrecondition, "mockserver: stream send error: %v", err)
		}

		req, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.FailedPrecondition, "mockserver: stream recv error: %v", err)
		}
	}
	return nil
}
