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

Verifies that messages within a partition are ordered. Detects duplicate
messages published or received.

Example simple usage (note: topic and subscription have same ID):
  go run longtest.go --project=<project> --zone=<zone> --topic=<topic id>

Example for testing ordering:
  go run longtest.go --project=<project> --zone=<zone> --topic=<topic id> \
    --message_count=100 --publish_setting_batch=10 --verbose=false

Example for testing throughput:
  go run longtest.go --project=<project> --zone=<zone> --topic=<topic id> \
    --message_count=100 --sleep=0s --verbose=false

Example for testing multiple subscriptions:
  go run longtest.go --project=<project> --zone=<zone> --topic=<topic id> \
    --subscription=<comma separated subscription ids>
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/common"
	"cloud.google.com/go/pubsublite/internal/test"
	"cloud.google.com/go/pubsublite/internal/test/integration"
	"cloud.google.com/go/pubsublite/internal/wire"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	messageCount = flag.Int("message_count", 5, "the number of messages to publish and receive per cycle, per partition")
	sleepPeriod  = flag.Duration("sleep", time.Minute, "the duration to sleep between cycles")
	waitTimeout  = flag.Duration("timeout", 2*time.Minute, "timeout for receiving all messages per cycle")
	verbose      = flag.Bool("verbose", true, "whether to log verbose messages")
)

const maxPrintMsgLen = 70

func truncateMsg(msg string) string {
	if len(msg) > maxPrintMsgLen {
		return fmt.Sprintf("%s...", msg[0:maxPrintMsgLen])
	}
	return msg
}

// subscriber contains a wire subscriber with message validators.
type subscriber struct {
	Subscription      pubsublite.SubscriptionPath
	Sub               wire.Subscriber
	MsgTracker        *test.MsgTracker
	OrderingValidator *test.OrderingReceiver
	DuplicateDetector *test.DuplicateMsgDetector
}

func newSubscriber(harness *integration.TestHarness, subscription pubsublite.SubscriptionPath) *subscriber {
	s := &subscriber{
		Subscription:      subscription,
		MsgTracker:        test.NewMsgTracker(),
		OrderingValidator: test.NewOrderingReceiver(),
		DuplicateDetector: test.NewDuplicateMsgDetector(),
	}

	sub := harness.StartSubscriber(subscription, s.onReceive)
	s.Sub = sub
	go func() {
		log.Printf("Subscriber %s listening to messages...", subscription)
		err := sub.WaitStopped()
		log.Fatalf("%s: stopped with error: %v", subscription, err)
	}()

	return s
}

func (s *subscriber) onReceive(m *wire.ReceivedMessage) {
	m.Ack.Ack()

	data := string(m.Msg.GetMessage().GetData())
	if !s.MsgTracker.Remove(data) {
		// Ignore messages from a previous test run.
		if *verbose {
			log.Printf("Ignoring %s", truncateMsg(data))
		}
		return
	}

	offset := m.Msg.GetCursor().GetOffset()
	key := string(m.Msg.GetMessage().GetKey())
	if *verbose {
		log.Printf("Received: (key=%s, offset=%d) %s", key, offset, data)
	}

	// Ordering and duplicate validation.
	if err := s.OrderingValidator.Receive(data, key); err != nil {
		log.Fatalf("%s: %v", s.Subscription, err)
	}
	s.DuplicateDetector.Receive(data, offset)
	if s.DuplicateDetector.HasReceiveDuplicates() {
		log.Fatalf("%s: %s", s.Subscription, s.DuplicateDetector.Status())
	}
}

func (s *subscriber) Wait() {
	if err := s.MsgTracker.Wait(*waitTimeout); err != nil {
		log.Fatalf("%s: failed waiting for messages: %v", s.Subscription, err)
	}
}

func main() {
	harness := integration.NewTestHarness()
	start := time.Now()

	// Setup subscribers.
	var subscribers []*subscriber
	for _, subscription := range harness.Subscriptions {
		subscribers = append(subscribers, newSubscriber(harness, subscription))
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
	msgPrefix := fmt.Sprintf("longtest-%d", start.Unix())
	orderingSender := test.NewOrderingSender()
	cycleMsgCount := *messageCount * harness.TopicPartitionCount

	for {
		cycleStart := time.Now()
		var toPublish []*pb.PubSubMessage
		var trackedMsgs []string

		for partition := 0; partition < harness.TopicPartitionCount; partition++ {
			for i := 0; i < *messageCount; i++ {
				key := fmt.Sprintf("p%d", partition)
				data := orderingSender.Next(msgPrefix)
				trackedMsgs = append(trackedMsgs, data)
				toPublish = append(toPublish, &pb.PubSubMessage{Key: []byte(key), Data: []byte(data)})
				if *verbose {
					log.Printf("Publishing: (key=%s) %s", key, data)
				}
			}
		}

		// Add all messages for the cycle to the MsgTrackers first to avoid Wait()
		// releasing too early.
		for _, sub := range subscribers {
			sub.MsgTracker.Add(trackedMsgs...)
		}

		// Now publish.
		for _, msg := range toPublish {
			publisher.Publish(msg, onPublished)
		}

		// Wait for all subscribers to receive all messages for the cycle.
		for _, sub := range subscribers {
			sub.Wait()
			duplicates := sub.DuplicateDetector.Status()
			if len(duplicates) > 0 {
				log.Printf("%s: %s", sub.Subscription, duplicates)
			}
		}

		now := time.Now()
		log.Printf("*** Cycle elapsed: %v, cycle messages: %d, total elapsed: %v, total messages: %d ****",
			now.Sub(cycleStart), cycleMsgCount, now.Sub(start), orderingSender.TotalMsgCount)

		if *sleepPeriod > 0 {
			time.Sleep(*sleepPeriod)
		}
	}
}
