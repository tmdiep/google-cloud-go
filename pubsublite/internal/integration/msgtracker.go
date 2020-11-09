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
	"strconv"
	"strings"
	"sync"
	"time"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
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

type OrderingValidator struct {
	totalMsgCount int64
	received      map[string]int64
	mu            sync.Mutex
}

func NewOrderingValidator() *OrderingValidator {
	return &OrderingValidator{
		received: make(map[string]int64),
	}
}

func parseMsgIndex(msg string) int64 {
	pos := strings.LastIndex(msg, "/")
	if pos >= 0 {
		if n, err := strconv.ParseInt(msg[pos+1:], 10, 64); err == nil {
			return n
		}
	}
	return -1
}

func (ov *OrderingValidator) NextPublishedMsg(prefix string, partition int) *pb.PubSubMessage {
	ov.mu.Lock()
	defer ov.mu.Unlock()

	ov.totalMsgCount++
	return &pb.PubSubMessage{
		Key:  []byte(fmt.Sprintf("key%d", partition)),
		Data: []byte(fmt.Sprintf("%s/%d", prefix, ov.totalMsgCount)),
	}
}

func (ov *OrderingValidator) ReceiveMsg(msg *pb.PubSubMessage) error {
	ov.mu.Lock()
	defer ov.mu.Unlock()

	partition := string(msg.Key)
	nextMinIdx, _ := ov.received[partition]
	idx := parseMsgIndex(string(msg.Data))
	if idx < nextMinIdx {
		return fmt.Errorf("message ordering failed for partition %s, expected idx >= %d, got idx: %d", partition, nextMinIdx, idx)
	}
	ov.received[partition] = idx + 1
	return nil
}

func (ov *OrderingValidator) TotalMsgCount() int64 {
	ov.mu.Lock()
	defer ov.mu.Unlock()
	return ov.totalMsgCount
}
