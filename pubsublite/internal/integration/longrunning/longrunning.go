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
longrunning attempts to publish and subscribe for as long as possible.
*/
package main

import (
	"flag"
	"log"
	"time"

	"cloud.google.com/go/pubsublite/internal/integration"
	"cloud.google.com/go/pubsublite/internal/wire"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	messageCount    = flag.Int("message_count", 5, "the number of messages to publish and receive per cycle, per partition")
	subscriberCount = flag.Int("subscriber_count", 2, "the number of subscriber clients to create (only applies when assignments are enabled)")
	sleepPeriod     = flag.Duration("sleep", time.Minute, "the duration to sleep between cycles")
)

const (
	msgWaitTimeout = 2 * time.Minute
)

func main() {
	start := time.Now()
	harness := integration.NewTestHarness()
	partitionCount := harness.TopicPartitions()
	msgQueue := integration.NewMsgQueue()
	numSubscribers := 1

	if harness.EnableAssignment {
		if partitionCount < *subscriberCount {
			log.Fatalf("Topic requires at least %d partitions to assign to subscribers, but only has %d partitions", *subscriberCount, partitionCount)
		}
		numSubscribers = *subscriberCount
	}

	// Setup subscribers.
	onReceive := func(msg *pb.SequencedMessage, ack wire.AckConsumer) {
		ack.Ack()

		str := string(msg.GetMessage().GetData())
		if msgQueue.RemoveMsg(str) {
			log.Printf("Received: (offset=%d) %s", msg.GetCursor().GetOffset(), str)
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
		log.Printf("Published: (partition=%d, offset=%d)", pm.Partition, pm.Offset)
	}

	// Main publishing loop.
	log.Printf("Starting test...")
	for {
		for i := 0; i < *messageCount*partitionCount; i++ {
			publisher.Publish(&pb.PubSubMessage{Data: []byte(msgQueue.AddMsg())}, onPublished)
		}

		if err := msgQueue.Wait(msgWaitTimeout); err != nil {
			log.Fatalf("Test failed: %v, time elapsed: %v", err, time.Now().Sub(start))
		}
		log.Printf("Time elapsed: %v", time.Now().Sub(start))
		time.Sleep(*sleepPeriod)
	}
}
