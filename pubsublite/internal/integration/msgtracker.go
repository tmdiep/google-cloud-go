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
	"fmt"
	"sync"
	"time"
)

// MsgTracker is a helper for checking whether a set of messages make a full
// round trip from publisher to subscriber.
type MsgTracker struct {
	msgMap map[string]bool
	done   chan struct{}
	mu     sync.Mutex
}

func NewMsgTracker() *MsgTracker {
	return &MsgTracker{
		msgMap: make(map[string]bool),
		done:   make(chan struct{}),
	}
}

func (mt *MsgTracker) Add(msg string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.msgMap[msg] = true
}

func (mt *MsgTracker) Remove(msg string) bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	_, exists := mt.msgMap[msg]
	delete(mt.msgMap, msg)
	if len(mt.msgMap) == 0 {
		var s struct{}
		mt.done <- s
	}
	return exists
}

func (mt *MsgTracker) Wait(timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		err := fmt.Errorf("failed to receive %d messages", len(mt.msgMap))
		mt.clear()
		return err
	case <-mt.done:
		return nil
	}
}

func (mt *MsgTracker) clear() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.msgMap = make(map[string]bool)
}
