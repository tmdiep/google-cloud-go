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
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/internal/test"
	"cloud.google.com/go/pubsublite/internal/test/integration"
	"cloud.google.com/go/pubsublite/internal/wire"
	"cloud.google.com/go/pubsublite/pscompat"
	"golang.org/x/sync/errgroup"
)

var (
	messageCount   = flag.Int("message_count", 10, "the number of messages to publish and receive per cycle, per partition")
	messagePadding = flag.Int("padding_bytes", 0, "the number of bytes to pad per partition (divided by message count)")
	sleepPeriod    = flag.Duration("sleep", 15*time.Second, "the duration to sleep between cycles")
	waitTimeout    = flag.Duration("timeout", 2*time.Minute, "timeout for receiving all messages per cycle")
	publishTimeout = flag.Duration("publish_timeout", time.Minute, "timeout for waiting for publish result")
	verbose        = flag.Bool("verbose", true, "whether to log verbose messages")
)

// subscriber contains a wire subscriber with message validators.
type subscriber struct {
	Subscription      wire.SubscriptionPath
	Sub               *pscompat.SubscriberClient
	MsgTracker        *test.MsgTracker
	OrderingValidator *test.OrderingReceiver
}

func newSubscriber(harness *integration.TestHarness, subscription wire.SubscriptionPath) *subscriber {
	s := &subscriber{
		Subscription:      subscription,
		MsgTracker:        test.NewMsgTracker(),
		OrderingValidator: test.NewOrderingReceiver(),
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
	metadata, err := pscompat.ParseMessageMetadata(msg.ID)
	if err != nil {
		log.Fatalf("Error parsing message metadata %q: %v", msg.ID, err)
	}

	isExpected := s.MsgTracker.Remove(data)
	if !isExpected {
		log.Printf("Ignoring duplicate: (partition=%d, offset=%d, expected=%v) %s", metadata.Partition, metadata.Offset, isExpected, data)
	} else if *verbose {
		log.Printf("Received: (partition=%d, offset=%d, expected=%v) %s", metadata.Partition, metadata.Offset, isExpected, data)
	}

	if err := s.OrderingValidator.Receive(data, fmt.Sprintf("%d", metadata.Partition), metadata.Offset, isExpected); err != nil {
		log.Fatalf("Ordering failed: %s: %v", s.Subscription, err)
	}
}

func (s *subscriber) Wait() ([]string, error) {
	return s.MsgTracker.Wait(*waitTimeout)
}

func main() {
	ctx := context.Background()
	harness := integration.NewTestHarness()
	start := time.Now()
	cycleCount := 0

	// Setup subscribers.
	var subscribers []*subscriber
	for _, subscription := range harness.Subscriptions {
		op, err := harness.AdminClient.SeekSubscription(ctx, subscription.String(), pubsublite.End)
		if err != nil {
			log.Fatalf("Failed to seek %s to end", subscription)
		} else {
			log.Printf("Seek %s to end; operation %s", subscription, op.Name())
		}

		subscribers = append(subscribers, newSubscriber(harness, subscription))
	}

	// Setup publisher.
	publisher := harness.StartPublisher()

	// Main publishing loop.
	log.Printf("Starting test...")
	msgPrefix := fmt.Sprintf("longtest-%d", start.Unix())
	orderingSender := test.NewOrderingSender()
	padding := *messagePadding / *messageCount
	latencies := integration.NewLatencyHistogram([]time.Duration{
		1 * time.Second,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		time.Minute,
		2 * time.Minute,
		5 * time.Minute,
		10 * time.Minute,
	})
	var totalPublishTimeoutCount int32

	dumpState := func() {
		log.Println("BEGIN DUMP STATE")
		log.Println("PUBLISHER:")
		publisher.LogState()
		for _, sub := range subscribers {
			log.Printf("SUBSCRIBER(%s):", sub.Subscription.String())
			sub.Sub.LogState()
		}
		log.Println("END DUMP STATE")
	}

	for {
		cycleCount++
		cycleStart := time.Now()
		var toPublish []*pubsub.Message
		var trackedMsgs []string
		var publishSucceeded int32
		var publishTimedOut int32

		for partition := 0; partition < harness.TopicPartitionCount; partition++ {
			for i := 0; i < *messageCount; i++ {
				data := orderingSender.Next(fmt.Sprintf("%s/%d", msgPrefix, cycleCount))
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
				cctx, cancel := context.WithTimeout(ctx, *publishTimeout)
				id, err := result.Get(cctx)
				cancel()
				if err != nil {
					if err == context.DeadlineExceeded {
						atomic.AddInt32(&publishTimedOut, 1)
					}
					log.Printf("Publish failed for %q: %v. Publisher error: %v", string(msg.Data), err, publisher.Error())
					return err
				}
				atomic.AddInt32(&publishSucceeded, 1)
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
				dumpState()

				mu.Lock()
				log.Println("BEGIN OUTSTANDING MESSAGES")
				for _, msg := range msgs {
					log.Printf("%s | %s", msg, msgMap[msg])
				}
				log.Println("END OUTSTANDING MESSAGES")
				mu.Unlock()

				log.Printf("%d of %d messages successfully published. Timeouts (>%v): %d",
					atomic.LoadInt32(&publishSucceeded), len(toPublish), *publishTimeout, atomic.LoadInt32(&publishTimedOut))
				log.Fatalf("%s: failed waiting for messages: %v", sub.Subscription, err)
			}
			log.Printf("Ordering state: " + sub.OrderingValidator.State())
		}

		// Ensure all publish results succeeded.
		if err := g.Wait(); err != nil {
			if publishTimedOut > 0 {
				totalPublishTimeoutCount += 1
			} else {
				dumpState()
				log.Fatalf("Publish failed: %v", err)
			}
		}

		now := time.Now()
		cycleElapsed := now.Sub(cycleStart)
		if cycleCount > 1 {
			latencies.Add(cycleElapsed)
		}
		if cycleElapsed > 30*time.Second {
			dumpState()
			log.Printf("%d of %d messages successfully published. Timeouts (>%v): %d",
				atomic.LoadInt32(&publishSucceeded), len(toPublish), *publishTimeout, atomic.LoadInt32(&publishTimedOut))
		}
		log.Printf("*** Cycle elapsed: %v, total elapsed: %v, total messages: %d, publish timeouts: %d ****",
			cycleElapsed, now.Sub(start), orderingSender.TotalMsgCount, totalPublishTimeoutCount)
		log.Printf("    %s", latencies.Status("cycles"))

		harness.WriteMemProfile()

		if *sleepPeriod > 0 {
			time.Sleep(*sleepPeriod)
		}
	}
}
