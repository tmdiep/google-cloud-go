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

/*
longrunning attempts to publish and subscribe for as long as possible. It
supports a few options for running various test cases.

Example simple usage:
  go run longrunning.go --project=<project> --topic=<topic id> --zone=<zone>

Example for testing subscriber partition assignment (must use topic with
multiple partitions):
  go run longrunning.go --project=<project> --topic=<topic id> --zone=<zone> --assignment=true

Example for testing ordering (must use topic with 1 partition):
  go run longrunning.go --project=<project> --topic=<topic id> --zone=<zone> --message_count=100 --publish_batch=5 --sleep=5s

Example for testing throughput:
  go run longrunning.go --project=<project> --topic=<topic id> --zone=<zone> --message_count=100 --sleep=0s --verbose=false
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsublite/internal/integration"
	"cloud.google.com/go/pubsublite/internal/wire"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	messageCount    = flag.Int("message_count", 5, "the number of messages to publish and receive per cycle, per partition")
	subscriberCount = flag.Int("subscriber_count", 2, "the number of subscriber clients to create (only applies when assignments are enabled)")
	sleepPeriod     = flag.Duration("sleep", time.Minute, "the duration to sleep between cycles")
	verbose         = flag.Bool("verbose", true, "whether to log verbose messages")
)

const msgWaitTimeout = 2 * time.Minute

func parseMsgIndex(msg string) int64 {
	pos := strings.LastIndex(msg, "/")
	if pos >= 0 {
		if n, err := strconv.ParseInt(msg[pos+1:], 10, 64); err == nil {
			return n
		}
	}
	return -1
}

func main() {
	harness := integration.NewTestHarness()
	partitionCount := harness.TopicPartitions()
	numSubscribers := 1
	if harness.EnableAssignment {
		if partitionCount < *subscriberCount {
			log.Fatalf("Topic requires at least %d partitions to assign to subscribers, but only has %d partitions", *subscriberCount, partitionCount)
		}
		numSubscribers = *subscriberCount
	}

	start := time.Now()
	msgTracker := integration.NewMsgTracker()
	msgPrefix := fmt.Sprintf("hello-%d", start.Unix())
	lastMsgIdx := int64(-1)

	// Setup subscribers.
	onReceive := func(seqMsg *pb.SequencedMessage, ack wire.AckConsumer) {
		ack.Ack()

		msg := string(seqMsg.GetMessage().GetData())
		if !msgTracker.Remove(msg) {
			return
		}

		if *verbose {
			log.Printf("Received: (offset=%d) %s", seqMsg.GetCursor().GetOffset(), msg)
		}

		// Validating ordering can only be done with 1 partition as the msg receiver
		// does not have the partition number.
		if partitionCount == 1 {
			idx := parseMsgIndex(msg)
			if idx <= atomic.LoadInt64(&lastMsgIdx) {
				log.Fatalf("Message ordering failed, last idx: %d, got idx: %d", lastMsgIdx, idx)
			}
			atomic.StoreInt64(&lastMsgIdx, idx)
		}
	}

	var subscribers []wire.Subscriber
	for i := 0; i < numSubscribers; i++ {
		subscriber := harness.StartSubscriber(onReceive)
		subscribers = append(subscribers, subscriber)

		go func(id int) {
			log.Printf("Subscriber%d listening to messages...", id)
			err := subscriber.WaitStopped()
			log.Fatalf("Subscriber%d stopped with error: %v, time elapsed: %v", id, err, time.Now().Sub(start))
		}(i)
	}

	// Setup publisher.
	publisher := harness.StartPublisher()
	onPublished := func(pm *wire.PublishMetadata, err error) {
		if err != nil {
			log.Fatalf("Publish error: %v, time elapsed: %v", err, time.Now().Sub(start))
		}
		if *verbose {
			log.Printf("Published: (partition=%d, offset=%d)", pm.Partition, pm.Offset)
		}
	}

	// Main publishing loop.
	var totalMsgCount int64
	log.Printf("Starting test...")

	for {
		cycleStart := time.Now()
		cycleMsgCount := *messageCount * partitionCount

		for i := 0; i < cycleMsgCount; i++ {
			msg := fmt.Sprintf("%s/%d", msgPrefix, totalMsgCount)
			msgTracker.Add(msg)
			totalMsgCount++
			publisher.Publish(&pb.PubSubMessage{Data: []byte(msg)}, onPublished)
		}
		if err := msgTracker.Wait(msgWaitTimeout); err != nil {
			log.Fatalf("Test failed: %v, time elapsed: %v", err, time.Now().Sub(start))
		}

		now := time.Now()
		log.Printf("Cycle elapsed: %v, cycle messages: %d, total elapsed: %v, total messages: %d", now.Sub(cycleStart), cycleMsgCount, now.Sub(start), totalMsgCount)

		if *sleepPeriod > 0 {
			time.Sleep(*sleepPeriod)
		}
	}
}
