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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/internal/test"
	"cloud.google.com/go/pubsublite/internal/test/integration"
	"cloud.google.com/go/pubsublite/internal/wire"
	"cloud.google.com/go/pubsublite/pscompat"
	"github.com/golang/protobuf/ptypes"
)

var (
	messageCount  = flag.Int("message_count", 2, "the number of messages to publish and receive per cycle, per partition")
	sleepPeriod   = flag.Duration("sleep", 20*time.Second, "the duration to sleep between cycles")
	seekPeriod    = flag.Duration("seek_period", 6*time.Minute, "the duration between seek cycles")
	waitTimeout   = flag.Duration("timeout", 60*time.Second, "timeout for receiving all messages per cycle")
	verbose       = flag.Bool("verbose", true, "whether to log verbose messages")
	printInterval = flag.Int("print_interval", 10, "print status every n-th message sent/received")
)

const maxPrintMsgLen = 70

// subscriber contains a wire subscriber with message validators.
type subscriber struct {
	Subscription        wire.SubscriptionPath
	Sub                 *pscompat.SubscriberClient
	MsgTracker          *test.MsgTracker
	EarliestPublishTime time.Time
	receiveCount        int64
	mu                  sync.Mutex
}

func newSubscriber(harness *integration.TestHarness, subscription wire.SubscriptionPath) *subscriber {
	s := &subscriber{
		Subscription: subscription,
		MsgTracker:   test.NewMsgTracker(),
	}

	sub := harness.StartSubscriber(subscription)
	s.Sub = sub
	go func() {
		log.Printf("Subscriber %s listening to messages...", subscription)
		err := sub.Receive(context.Background(), s.onReceive)
		log.Fatalf("%s: stopped with error: %v", subscription, err)
	}()

	return s
}

func (s *subscriber) onMessage(msgPublishTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.EarliestPublishTime.IsZero() || msgPublishTime.Before(s.EarliestPublishTime) {
		s.EarliestPublishTime = msgPublishTime
	}
}

func (s *subscriber) onReceive(ctx context.Context, msg *pubsub.Message) {
	defer msg.Ack()

	if !s.MsgTracker.Remove(string(msg.Data)) && *verbose {
		log.Fatalf("Unexpected message: offset=%s, data=%s", msg.ID, string(msg.Data))
	}

	s.onMessage(msg.PublishTime)

	count := atomic.AddInt64(&s.receiveCount, 1)
	if *verbose || count%int64(*printInterval) == 0 {
		tspb, _ := ptypes.TimestampProto(msg.PublishTime)
		log.Printf("Received: offset=%s, data=%s, published=%s", msg.ID, string(msg.Data), tspb)
	}
}

func (s *subscriber) Wait() {
	if _, err := s.MsgTracker.Wait(*waitTimeout); err != nil {
		buf := make([]byte, 1<<16)
		runtime.Stack(buf, true)
		fmt.Printf("%s", buf)

		log.Fatalf("%s: failed to wait for messages: %v", s.Subscription, err)
	}
	s.MsgTracker = test.NewMsgTracker()
}

func main() {
	ctx := context.Background()
	harness := integration.NewTestHarness()
	start := time.Now()

	// Setup subscribers.
	var subscribers []*subscriber
	for _, subscription := range harness.Subscriptions {
		op, err := harness.AdminClient.SeekSubscription(ctx, subscription.String(), pubsublite.End)
		if err != nil {
			log.Fatalf("Failed to seek %s to head", subscription)
		}
		met, _ := op.Metadata()
		log.Printf("Got operation %s; done: %v\nmetadata: %v", op.Name(), op.Done(), met)

		subscribers = append(subscribers, newSubscriber(harness, subscription))

		if err := op.Wait(ctx); err != nil {
			log.Printf("Failed to wait for operation: %v", err)
		}
	}

	// Setup publisher.
	publisher := harness.StartPublisher()

	// Main publishing loop.
	log.Printf("Publishing...")
	msgPrefix := fmt.Sprintf("seek-%d", start.Unix())
	orderingSender := test.NewOrderingSender()
	var seekMsgs []string
	lastSeek := time.Now()
	seekLatencies := integration.NewLatencyHistogram([]time.Duration{
		5 * time.Second,
		10 * time.Second,
		20 * time.Second,
		30 * time.Second,
		40 * time.Second,
		50 * time.Second,
		60 * time.Second,
	})

	for {
		var toPublish []*pubsub.Message
		var trackedMsgs []string

		for partition := 0; partition < harness.TopicPartitionCount; partition++ {
			for i := 0; i < *messageCount; i++ {
				data := orderingSender.Next(msgPrefix)
				trackedMsgs = append(trackedMsgs, data)
				toPublish = append(toPublish, &pubsub.Message{Data: []byte(data)})
				if *verbose {
					log.Printf("Publishing: %s", data)
				}
			}
		}

		// Add all messages for the cycle to the MsgTrackers first to avoid Wait()
		// releasing too early.
		for _, sub := range subscribers {
			sub.MsgTracker.Add(trackedMsgs...)
		}
		seekMsgs = append(seekMsgs, trackedMsgs...)

		// Now publish.
		var results []*pubsub.PublishResult
		for _, msg := range toPublish {
			results = append(results, publisher.Publish(ctx, msg))
		}

		for _, result := range results {
			_, err := result.Get(ctx)
			if err != nil {
				log.Fatalf("Publishing error: %v", err)
			}
		}

		for _, sub := range subscribers {
			// Wait for first round of messages to get earliest publish time.
			sub.Wait()

			if time.Now().Sub(lastSeek) >= *seekPeriod {
				// Refill messages before seek.
				sub.MsgTracker.Add(seekMsgs...)

				pubTime, _ := ptypes.TimestampProto(sub.EarliestPublishTime)
				op, err := harness.AdminClient.SeekSubscription(ctx, sub.Subscription.String(), pubsublite.PublishTime(sub.EarliestPublishTime))
				if err != nil {
					log.Fatalf("Failed to seek %s to time %v", sub.Subscription, pubTime)
				}
				seekStartTime := time.Now()
				met, _ := op.Metadata()
				log.Printf("*** Seek to time %v got operation: %s", pubTime, op.Name())
				log.Printf("Initial metadata: %v", met)
				log.Printf("Waiting for subscriber to react...")

				// Ensure all messages are received again.
				sub.Wait()
				seekElapsed := time.Now().Sub(seekStartTime)
				seekLatencies.Add(seekElapsed)
				log.Printf("*** Seek completed in %v", seekElapsed)
				if err := op.Wait(ctx); err != nil {
					log.Fatalf("Waiting for seek operation returned err: %v", err)
				}
				if !op.Done() {
					log.Fatalf("Seek operation is not done")
				}
				met, _ = op.Metadata()
				log.Printf("Final metadata: %v", met)

				// Reset earliest time.
				sub.EarliestPublishTime = time.Time{}
				lastSeek = time.Now()
				seekMsgs = nil
			}
		}

		now := time.Now()
		log.Printf("**** Total elapsed: %v, total messages: %d ****",
			now.Sub(start), orderingSender.TotalMsgCount)
		log.Printf("**** %s ****", seekLatencies.Status("seeks"))

		if *sleepPeriod > 0 {
			time.Sleep(*sleepPeriod)
		}
	}
}
