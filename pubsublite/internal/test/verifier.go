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
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/internal/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	blockWaitTimeout = 30 * time.Second
)

type rpcMetadata struct {
	wantRequest interface{}
	retResponse interface{}
	retErr      error
	block       chan struct{}
}

// wait until the block is released by the test, or a timeout occurs. Returns
// immediately if there was no block.
func (r *rpcMetadata) wait() error {
	if r.block == nil {
		return nil
	}
	select {
	case <-time.After(blockWaitTimeout):
		return status.Errorf(codes.FailedPrecondition, "mockserver: test did not unblock response within %v", blockWaitTimeout)
	case <-r.block:
		return nil
	}
}

type RPCVerifier struct {
	t  *testing.T
	mu sync.Mutex
	// An ordered list of expected RPCs stored in rpcMetadata.
	rpcs     *list.List
	numCalls int
}

func NewRPCVerifier(t *testing.T) *RPCVerifier {
	return &RPCVerifier{
		t:        t,
		rpcs:     list.New(),
		numCalls: -1,
	}
}

func (v *RPCVerifier) Push(wantRequest interface{}, retResponse interface{}, retErr error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.rpcs.PushBack(&rpcMetadata{
		wantRequest: wantRequest,
		retResponse: retResponse,
		retErr:      retErr,
	})
}

// PushWithBlock is like Push, but returns a channel that the test should close
// when it would like the response to be sent. This is useful for introducing
// delays to allow the client sufficient time to perform some work.
func (v *RPCVerifier) PushWithBlock(wantRequest interface{}, retResponse interface{}, retErr error) chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()

	block := make(chan struct{})
	v.rpcs.PushBack(&rpcMetadata{
		wantRequest: wantRequest,
		retResponse: retResponse,
		retErr:      retErr,
		block:       block,
	})
	return block
}

func (v *RPCVerifier) Pop(gotRequest interface{}) (interface{}, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.numCalls++
	elem := v.rpcs.Front()
	if elem == nil {
		v.t.Errorf("call(%d): unexpected request:\n%v", v.numCalls, gotRequest)
		return nil, status.Error(codes.FailedPrecondition, "mockserver: got unexpected request")
	}

	rpc, _ := elem.Value.(*rpcMetadata)
	v.rpcs.Remove(elem)

	if !testutil.Equal(gotRequest, rpc.wantRequest) {
		v.t.Errorf("call(%d): got request: %v\nwant request: %v", v.numCalls, gotRequest, rpc.wantRequest)
	}
	if err := rpc.wait(); err != nil {
		return nil, err
	}
	return rpc.retResponse, rpc.retErr
}

func (v *RPCVerifier) Flush() {
	v.mu.Lock()
	defer v.mu.Unlock()

	for elem := v.rpcs.Front(); elem != nil; elem = elem.Next() {
		v.numCalls++
		rpc, _ := elem.Value.(*rpcMetadata)
		v.t.Errorf("call(%d): did not receive expected request:\n%v", v.numCalls, rpc.wantRequest)
	}
	v.rpcs.Init()
}

type streamVerifiers struct {
	t *testing.T
	// An ordered list of RPCVerifiers for each unique stream connection.
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
		v.t.Errorf("stream(%d): unexpected connection with no verifiers", v.numStreams)
		return nil, status.Error(codes.FailedPrecondition, "mockserver: got unexpected stream connection")
	}

	rv, _ := elem.Value.(*RPCVerifier)
	v.verifiers.Remove(elem)
	return rv, nil
}
