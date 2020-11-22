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
longtest attempts to continuously publish and subscribe until the process is
killed. It measures how long it takes for publisher and subscriber clients to
experience a permanent error.

Example simple usage:
  go run longtest.go --project=<project> --topic=<topic id> --zone=<zone>

Example for testing subscriber partition assignment (must use topic with
multiple partitions):
  go run longtest.go --project=<project> --topic=<topic id> --zone=<zone> --assignment=true --subscriber_count=3

Example for testing ordering:
  go run longtest.go --project=<project> --topic=<topic id> --zone=<zone> --message_count=100 --publish_setting_batch=10 --verbose=false

Example for testing throughput:
  go run longtest.go --project=<project> --topic=<topic id> --zone=<zone> --message_count=100 --sleep=0s --verbose=false
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsublite/common"
	"cloud.google.com/go/pubsublite/internal/test"
	"cloud.google.com/go/pubsublite/internal/test/integration"
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
	msgPrefix := fmt.Sprintf("longtest-%d", start.Unix())
	msgTracker := test.NewMsgTracker()
	orderingValidator := test.NewOrderingValidator()

	// Setup subscribers.
	onReceive := func(msgs []*wire.ReceivedMessage) {
		for _, m := range msgs {
			m.Ack.Ack()

			msg := string(m.Msg.GetMessage().GetData())
			if !msgTracker.Remove(msg) {
				// Ignore messages from a previous test run.
				return
			}
			if *verbose {
				log.Printf("Received: (offset=%d) %s", m.Msg.GetCursor().GetOffset(), msg)
			}
			if err := orderingValidator.ReceiveMsg(m.Msg.Message); err != nil {
				log.Fatalf("Test failed: %v, time elapsed: %v", err, time.Now().Sub(start))
			}
		}
	}

	// TODO: Move assignment to separate test and test multiple subscriptions here
	// Auto create resources
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
	onPublished := func(pm *common.PublishMetadata, err error) {
		if err != nil {
			log.Fatalf("Publish error: %v, time elapsed: %v", err, time.Now().Sub(start))
		}
		if *verbose {
			log.Printf("Published: (partition=%d, offset=%d)", pm.Partition, pm.Offset)
		}
	}

	// Main publishing loop.
	log.Printf("Starting test...")
	cycleMsgCount := *messageCount * partitionCount

	for {
		cycleStart := time.Now()

		var toPublish []*pb.PubSubMessage
		for partition := 0; partition < partitionCount; partition++ {
			for i := 0; i < *messageCount; i++ {
				msg := orderingValidator.NextPublishedMsg(msgPrefix, partition)
				toPublish = append(toPublish, msg)
				if *verbose {
					log.Printf("Publishing: (key=%s) %s", string(msg.Key), string(msg.Data))
				}
				msgTracker.Add(string(msg.Data))
			}
		}
		// Add all messages for the cycle to the MsgTracker first to avoid Wait()
		// releasing too early.
		for _, msg := range toPublish {
			publisher.Publish(msg, onPublished)
		}

		if err := msgTracker.Wait(msgWaitTimeout); err != nil {
			log.Fatalf("Test failed: %v, time elapsed: %v", err, time.Now().Sub(start))
		}

		now := time.Now()
		log.Printf("*** Cycle elapsed: %v, cycle messages: %d, total elapsed: %v, total messages: %d ****",
			now.Sub(cycleStart), cycleMsgCount, now.Sub(start), orderingValidator.TotalMsgCount())

		if *sleepPeriod > 0 {
			time.Sleep(*sleepPeriod)
		}
	}
}
