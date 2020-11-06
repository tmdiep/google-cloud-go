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

package integration

import (
	"log"
	"sync"
	"time"

	"cloud.google.com/go/internal/uid"
)

// MsgQueue is a helper for checking whether a set of messages make a full
// round trip from publisher to subscriber.
type MsgQueue struct {
	msgIDs *uid.Space
	msgMap map[string]bool
	done   chan struct{}
	mu     sync.Mutex
}

func NewMsgQueue() *MsgQueue {
	return &MsgQueue{
		msgIDs: uid.NewSpace("hello", &uid.Options{Short: true}),
		msgMap: make(map[string]bool),
		done:   make(chan struct{}),
	}
}

func (mq *MsgQueue) AddMsg() string {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	msg := mq.msgIDs.New()
	mq.msgMap[msg] = true
	return msg
}

func (mq *MsgQueue) RemoveMsg(msg string) bool {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	_, exists := mq.msgMap[msg]
	delete(mq.msgMap, msg)
	if len(mq.msgMap) == 0 {
		var s struct{}
		mq.done <- s
	}
	return exists
}

func (mq *MsgQueue) Wait(timeout time.Duration) {
	select {
	case <-time.After(timeout):
		log.Fatalf("Failed to receive %d messages", len(mq.msgMap))
		mq.clear()
	case <-mq.done:
	}
}

func (mq *MsgQueue) clear() {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	mq.msgMap = make(map[string]bool)
}
