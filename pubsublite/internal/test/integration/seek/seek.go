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
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/internal/test"
	"cloud.google.com/go/pubsublite/internal/test/integration"
	"cloud.google.com/go/pubsublite/internal/wire"
	"cloud.google.com/go/pubsublite/pscompat"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc/codes"
)

var (
	messageCount  = flag.Int("message_count", 100, "the number of messages to publish and receive per cycle, per partition")
	sleepPeriod   = flag.Duration("sleep", 10*time.Second, "the duration to sleep between cycles")
	seekPeriod    = flag.Duration("seek_period", 6*time.Minute, "the duration between seek cycles")
	waitTimeout   = flag.Duration("timeout", 120*time.Second, "timeout for receiving all messages per cycle")
	opwaitTimeout = flag.Duration("operation_timeout", 90*time.Second, "timeout for waiting for operation")
	verbose       = flag.Bool("verbose", false, "whether to log verbose messages")
	printInterval = flag.Int("print_interval", 50, "print status every n-th message sent/received")
)

const maxPrintMsgLen = 70

var seekLatencies = integration.NewLatencyHistogram([]time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	30 * time.Second,
	40 * time.Second,
	50 * time.Second,
	60 * time.Second,
})

func printSeekLatencies() {
	log.Printf("**** %s ****", seekLatencies.Status("seeks"))
}

// subscriber contains a wire subscriber with message validators.
type subscriber struct {
	Subscription        wire.SubscriptionPath
	Sub                 *pscompat.SubscriberClient
	MsgTracker          *test.MsgTracker
	EarliestPublishTime time.Time
	stopSubscriber      context.CancelFunc
	subscriberStopped   *test.Condition
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
	return s
}

func (s *subscriber) StartReceive() {
	go func() {
		log.Printf("Subscriber %s listening to messages...", s.Subscription)
		var ctx context.Context
		ctx, s.stopSubscriber = context.WithCancel(context.Background())
		s.subscriberStopped = test.NewCondition("subscriber-stopped")
		if err := s.Sub.Receive(ctx, s.onReceive); err != nil {
			log.Fatalf("%s: stopped with error: %v", s.Subscription, err)
		}
		s.subscriberStopped.SetDone()
	}()
}

func (s *subscriber) StartReceiveAllowUnexpected() {
	go func() {
		log.Printf("Subscriber %s listening to messages...", s.Subscription)
		var ctx context.Context
		ctx, s.stopSubscriber = context.WithCancel(context.Background())
		s.subscriberStopped = test.NewCondition("subscriber-stopped")
		if err := s.Sub.Receive(ctx, s.onReceiveAllowUnexpected); err != nil {
			log.Fatalf("%s: stopped with error: %v", s.Subscription, err)
		}
		s.subscriberStopped.SetDone()
	}()
}

func (s *subscriber) onReceive(ctx context.Context, msg *pubsub.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer msg.Ack()

	if !s.MsgTracker.Remove(string(msg.Data)) {
		log.Fatalf("Unexpected message: offset=%s, data=%s", msg.ID, string(msg.Data))
	}

	if s.EarliestPublishTime.IsZero() || msg.PublishTime.Before(s.EarliestPublishTime) {
		s.EarliestPublishTime = msg.PublishTime
	}

	s.receiveCount++
	if *verbose || s.receiveCount%int64(*printInterval) == 0 {
		tspb, _ := ptypes.TimestampProto(msg.PublishTime)
		log.Printf("Received: offset=%s, data=%s, published=%s", msg.ID, string(msg.Data), tspb)
	}
}

func (s *subscriber) onReceiveAllowUnexpected(ctx context.Context, msg *pubsub.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer msg.Ack()

	expected := s.MsgTracker.Remove(string(msg.Data))

	s.receiveCount++
	if *verbose || s.receiveCount%int64(*printInterval) == 0 {
		tspb, _ := ptypes.TimestampProto(msg.PublishTime)
		log.Printf("Received: offset=%s, data=%s, published=%s, expected=%v", msg.ID, string(msg.Data), tspb, expected)
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

func (s *subscriber) Stop() {
	s.stopSubscriber()
	if err := s.subscriberStopped.Wait(*waitTimeout); err != nil {
		log.Fatalf("Failed to wait for subscriber to stop: %v", err)
	}
}

func seekToEnd(ctx context.Context, adminClient *pubsublite.AdminClient, sub *subscriber) {
	seekStartTime := time.Now()
	op, err := adminClient.SeekSubscription(ctx, sub.Subscription.String(), pubsublite.End)
	if err != nil {
		log.Fatalf("Failed to seek %s to end", sub.Subscription)
	}
	met, _ := op.Metadata()
	log.Printf("Seek to end operation %s; done: %v\nmetadata: %v", op.Name(), op.Done(), met)

	sub.StartReceive()

	cctx, _ := context.WithTimeout(ctx, *opwaitTimeout)
	if err := op.Wait(cctx); err != nil {
		log.Fatalf("Failed to wait for operation: %v", err)
	}
	seekLatencies.Add(time.Now().Sub(seekStartTime))
	printSeekLatencies()
}

func seekToBeginning(ctx context.Context, adminClient *pubsublite.AdminClient, sub *subscriber) {
	for i := 1; i <= 800; i++ {
		sub.MsgTracker.Add(fmt.Sprintf("seek/%d", i))
	}

	seekStartTime := time.Now()
	op, err := adminClient.SeekSubscription(ctx, sub.Subscription.String(), pubsublite.Beginning)
	if err != nil {
		log.Fatalf("Failed to seek %s to beginning", sub.Subscription)
	}
	met, _ := op.Metadata()
	log.Printf("Seek to beginning operation %s; done: %v\nmetadata: %v", op.Name(), op.Done(), met)

	sub.StartReceiveAllowUnexpected()
	sub.Wait()
	sub.Stop()

	cctx, _ := context.WithTimeout(ctx, *opwaitTimeout)
	if err := op.Wait(cctx); err != nil {
		log.Fatalf("Failed to wait for operation: %v", err)
	}
	seekLatencies.Add(time.Now().Sub(seekStartTime))
	printSeekLatencies()
}

func main() {
	ctx := context.Background()
	harness := integration.NewTestHarness()
	start := time.Now()

	// Setup subscriber
	sub := newSubscriber(harness, harness.Subscriptions[0])
	seekToBeginning(ctx, harness.AdminClient, sub)
	seekToEnd(ctx, harness.AdminClient, sub)

	// Setup publisher.
	publisher := harness.StartPublisher()

	// Main publishing loop.
	log.Printf("Publishing...")
	msgPrefix := fmt.Sprintf("seek-%d", start.Unix())
	orderingSender := test.NewOrderingSender()
	var seekMsgs []string
	lastSeek := time.Now()

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
		sub.MsgTracker.Add(trackedMsgs...)
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

		// Wait for first round of messages to get earliest publish time.
		sub.Wait()

		if time.Now().Sub(lastSeek) >= *seekPeriod {
			// Refill messages before seek.
			sub.MsgTracker.Add(seekMsgs...)

			pubTime, _ := ptypes.TimestampProto(sub.EarliestPublishTime)
			var seekTarget pubsublite.SeekTarget
			if seekLatencies.Total%2 == 0 {
				seekTarget = pubsublite.PublishTime(sub.EarliestPublishTime)
			} else {
				seekTarget = pubsublite.EventTime(sub.EarliestPublishTime)
			}
			op, err := harness.AdminClient.SeekSubscription(ctx, sub.Subscription.String(), seekTarget)
			if err != nil {
				log.Fatalf("Failed to seek %s to time %v", sub.Subscription, pubTime)
			}
			seekStartTime := time.Now()
			met, _ := op.Metadata()
			log.Printf("Seek to time %v operation %s; done: %v\nmetadata: %v", pubTime, op.Name(), op.Done(), met)
			log.Printf("Waiting for subscriber to react...")

			// Ensure all messages are received again.
			sub.Wait()
			seekElapsed := time.Now().Sub(seekStartTime)
			seekLatencies.Add(seekElapsed)
			log.Printf("Seek completed in %v", seekElapsed)

			// Operation should already be done.
			cctx, _ := context.WithTimeout(ctx, 10*time.Second)
			if err := op.Wait(cctx); err != nil {
				log.Fatalf("Waiting for seek operation returned err: %v", err)
			}
			if !op.Done() {
				log.Fatalf("Seek operation is not done")
			}
			met, _ = op.Metadata()
			log.Printf("Seek to time final metadata: %v", met)

			// Reset earliest time.
			sub.EarliestPublishTime = time.Time{}
			lastSeek = time.Now()
			seekMsgs = nil

			// Additional seek test cases. Restarts subscriber.
			sub.Stop()
			seekToBeginning(ctx, harness.AdminClient, sub)

			op, err = harness.AdminClient.SeekSubscription(ctx, sub.Subscription.String(), seekTarget)
			if err != nil {
				log.Fatalf("Aborted test: Failed to seek %s to time %v", sub.Subscription, pubTime)
			}
			met, _ = op.Metadata()
			log.Printf("Test abort seek operation %s; done: %v\nmetadata: %v", op.Name(), op.Done(), met)

			seekToEnd(ctx, harness.AdminClient, sub)

			cctx, _ = context.WithTimeout(ctx, 10*time.Second)
			if err := op.Wait(cctx); !test.ErrorHasCode(err, codes.Aborted) || !test.ErrorHasMsg(err, "Aborted due to a more recent seek operation") {
				log.Fatalf("Unexpected aborted seek err: %v", err)
			} else {
				log.Printf("Aborted seek err: %v", err)
			}
			if !op.Done() {
				log.Fatalf("Aborted seek operation is not done")
			}
			met, _ = op.Metadata()
			log.Printf("Aborted seek final metadata: %v", met)
		}

		now := time.Now()
		log.Printf("**** Total elapsed: %v, total messages: %d ****",
			now.Sub(start), orderingSender.TotalMsgCount)
		printSeekLatencies()

		if *sleepPeriod > 0 {
			time.Sleep(*sleepPeriod)
		}
	}
}
