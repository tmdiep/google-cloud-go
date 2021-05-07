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
    --message_count=50 --sleep=1s --verbose=false

Example for testing multiple subscriptions:
  go run longtest.go --project=<project> --zone=<zone> --topic=<topic id> \
    --subscription=<comma separated subscription ids>
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/internal/test"
	"cloud.google.com/go/pubsublite/internal/test/integration"
	"cloud.google.com/go/pubsublite/internal/wire"
	"cloud.google.com/go/pubsublite/pscompat"
	"golang.org/x/sync/errgroup"
)

var (
	messageCount   = flag.Int("message_count", 5, "the number of messages to publish and receive per cycle, per partition")
	messagePadding = flag.Int("padding_bytes", 0, "the number of bytes to pad per partition (divided by message count)")
	sleepPeriod    = flag.Duration("sleep", time.Minute, "the duration to sleep between cycles")
	waitTimeout    = flag.Duration("timeout", 5*time.Minute, "timeout for receiving all messages per cycle")
	verbose        = flag.Bool("verbose", true, "whether to log verbose messages")
	duplicates     = flag.Bool("duplicates", false, "whether to detect duplicates")
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
	Subscription      wire.SubscriptionPath
	Sub               *pscompat.SubscriberClient
	MsgTracker        *test.MsgTracker
	OrderingValidator *test.OrderingReceiver
	DuplicateDetector *test.DuplicateMsgDetector
}

func newSubscriber(harness *integration.TestHarness, subscription wire.SubscriptionPath) *subscriber {
	s := &subscriber{
		Subscription:      subscription,
		MsgTracker:        test.NewMsgTracker(),
		OrderingValidator: test.NewOrderingReceiver(),
		DuplicateDetector: test.NewDuplicateMsgDetector(),
	}

	sub := harness.StartSubscriber(subscription)
	s.Sub = sub
	go func() {
		log.Printf("Subscriber %s listening to messages...", subscription)
		err := sub.Receive(context.Background(), s.onReceive)
		log.Println("BEGIN DUMP STATE")
		sub.LogState()
		log.Println("END DUMP STATE")
		log.Fatalf("%s: stopped with error: %v", subscription, err)
	}()

	return s
}

func (s *subscriber) onReceive(ctx context.Context, msg *pubsub.Message) {
	msg.Ack()

	data := string(msg.Data)
	if !s.MsgTracker.Remove(data) {
		// Ignore messages from a previous test run.
		//if *verbose {
		log.Printf("### Ignoring %s: %s", msg.ID, truncateMsg(data))
		//}
		return
	}

	metadata, err := pscompat.ParseMessageMetadata(msg.ID)
	if err != nil {
		log.Fatalf("Error parsing message metadata %q: %v", msg.ID, err)
	}
	if *verbose {
		log.Printf("Received: (partition=%d, offset=%d) %s", metadata.Partition, metadata.Offset, data)
	}

	// Ordering and duplicate validation.
	if err := s.OrderingValidator.Receive(data, fmt.Sprintf("%d", metadata.Partition)); err != nil {
		log.Fatalf("Ordering failed: %s: %v", s.Subscription, err)
	}
	if *duplicates {
		// Note: This causes OOMs when the test runs too long.
		s.DuplicateDetector.Receive(data, metadata.Offset)
		if s.DuplicateDetector.HasReceiveDuplicates() {
			log.Fatalf("Detected duplicates: %s: %s", s.Subscription, s.DuplicateDetector.Status())
		}
	}
}

func (s *subscriber) Wait() ([]string, error) {
	return s.MsgTracker.Wait(*waitTimeout)
}

type ElapsedCounter struct {
	Threshold time.Duration
	Count     int64
}

func main() {
	ctx := context.Background()
	harness := integration.NewTestHarness()
	start := time.Now()
	cycleCount := 0

	// Setup subscribers.
	var subscribers []*subscriber
	for _, subscription := range harness.Subscriptions {
		subscribers = append(subscribers, newSubscriber(harness, subscription))
	}

	// Setup publisher.
	publisher := harness.StartPublisher()

	// Main publishing loop.
	log.Printf("Starting test...")
	msgPrefix := fmt.Sprintf("longtest-%d", start.Unix())
	orderingSender := test.NewOrderingSender()
	padding := *messagePadding / *messageCount
	counters := []*ElapsedCounter{
		{2 * time.Second, 0},
		{5 * time.Second, 0},
		{10 * time.Second, 0},
		{30 * time.Second, 0},
		{time.Minute, 0},
		{2 * time.Minute, 0},
		{5 * time.Minute, 0},
		{10 * time.Minute, 0},
	}

	for {
		cycleCount++
		cycleStart := time.Now()
		var toPublish []*pubsub.Message
		var trackedMsgs []string

		for partition := 0; partition < harness.TopicPartitionCount; partition++ {
			for i := 0; i < *messageCount; i++ {
				data := orderingSender.Next(msgPrefix)
				trackedMsgs = append(trackedMsgs, data)
				msg := &pubsub.Message{Data: []byte(data)}
				if padding > 0 {
					msg.Attributes = map[string]string{"padding": strings.Repeat("*", padding)}
				}
				toPublish = append(toPublish, msg)
			}
		}

		// Add all messages for the cycle to the MsgTrackers first to avoid Wait()
		// releasing too early.
		for _, sub := range subscribers {
			sub.MsgTracker.Add(trackedMsgs...)
		}

		// Now publish.
		g := new(errgroup.Group)
		var mu sync.Mutex
		msgMap := make(map[string]string)
		for _, tp := range toPublish {
			msg := tp // Ensure msg is bound to loop element.
			result := publisher.Publish(ctx, msg)
			g.Go(func() error {
				cctx, cancel := context.WithTimeout(ctx, *waitTimeout)
				id, err := result.Get(cctx)
				cancel()
				if err != nil {
					log.Fatalf("Publish failed: %v. Publisher error: %v", err, publisher.Error())
				}
				if *verbose && err == nil {
					log.Printf("Published: (id=%s) %s", id, string(msg.Data))
				}
				mu.Lock()
				msgMap[string(msg.Data)] = id
				mu.Unlock()
				return err
			})
		}

		// Wait for all subscribers to receive all messages for the cycle.
		for _, sub := range subscribers {
			if msgs, err := sub.Wait(); err != nil {
				log.Println("BEGIN DUMP STATE")
				publisher.LogState()
				sub.Sub.LogState()
				log.Println("END DUMP STATE")

				mu.Lock()
				log.Println("BEGIN OUTSTANDING MESSAGES")
				for _, msg := range msgs {
					log.Printf("%s | %s", msg, msgMap[msg])
				}
				log.Println("END OUTSTANDING MESSAGES")
				mu.Unlock()

				log.Fatalf("%s: failed waiting for messages: %v", sub.Subscription, err)
			}

			if *duplicates {
				dup := sub.DuplicateDetector.Status()
				if len(dup) > 0 {
					log.Printf("%s: %s", sub.Subscription, dup)
				}
			}
		}

		// Ensure all publish results succeeded.
		if err := g.Wait(); err != nil {
			log.Fatalf("Publish failed: %v", err)
		}

		now := time.Now()
		cycleElapsed := now.Sub(cycleStart)
		var statuses []string
		counterPrefix := ""
		for _, counter := range counters {
			if cycleCount > 1 && cycleElapsed > counter.Threshold {
				counter.Count++
			}
			statuses = append(statuses, fmt.Sprintf(">%v=%d", counter.Threshold, counter.Count))
			if counter.Count > 0 {
				counterPrefix = "! "
			}
		}
		log.Printf("*** Cycle elapsed: %v, total elapsed: %v, total messages: %d ****",
			cycleElapsed, now.Sub(start), orderingSender.TotalMsgCount)
		log.Printf("    %scycles=%d, %s", counterPrefix, cycleCount, strings.Join(statuses, ", "))

		harness.WriteMemProfile()

		if *sleepPeriod > 0 {
			time.Sleep(*sleepPeriod)
		}
	}
}
