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
	"cloud.google.com/go/pubsublite/internal/test/integration"
	"cloud.google.com/go/pubsublite/internal/wire"
	"cloud.google.com/go/pubsublite/pscompat"
	"golang.org/x/sync/errgroup"
)

var (
	printInterval  = flag.Int("print_interval", 100, "print status every n-th message sent/received")
	verbose        = flag.Bool("verbose", false, "whether to log verbose messages")
	ackDelay       = flag.Duration("ack_delay", 0, "sleep before acking messages")
	receiveTimeout = flag.Duration("receive_timeout", 2*time.Minute, "fail if messages not received within this duration for a partition")
)

type receiveCounter struct {
	count      int64
	partitions map[int]time.Time
	mu         sync.Mutex
}

func newReceiveCounter(numPartitions int) *receiveCounter {
	c := &receiveCounter{
		count:      0,
		partitions: make(map[int]time.Time),
	}
	now := time.Now()
	for i := 0; i < numPartitions; i++ {
		c.partitions[i] = now
	}
	return c
}

func (r *receiveCounter) OnMsgReceived(p int) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.count++
	r.partitions[p] = time.Now()
	return r.count
}

func (r *receiveCounter) checkReceived() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	timeout := false
	for _, t := range r.partitions {
		if now.Sub(t) > *receiveTimeout {
			timeout = true
			break
		}
	}
	if !timeout {
		return
	}

	fmt.Println("------------------------------------------------------------")
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	fmt.Printf("%s\n", buf)
	fmt.Println("------------------------------------------------------------")

	for p, t := range r.partitions {
		elapsed := now.Sub(t)
		suffix := ""
		if elapsed > *receiveTimeout {
			suffix = " ***"
		}
		log.Printf("%d: %v%s", p, now.Sub(t), suffix)
	}
	log.Fatal("Timed out receiving messages")
}

func (r *receiveCounter) Poll() {
	go func() {
		for true {
			r.checkReceived()
			time.Sleep(10 * time.Second)
		}
	}()
}

const maxPrintMsgLen = 70

func truncateMsg(msg string) string {
	if len(msg) > maxPrintMsgLen {
		return fmt.Sprintf("%s...", msg[0:maxPrintMsgLen])
	}
	return msg
}

// subscriber contains a wire subscriber with message validators.
type subscriber struct {
	Subscription wire.SubscriptionPath
	Sub          *pscompat.SubscriberClient
	counter      *receiveCounter
}

func newSubscriber(ctx context.Context, harness *integration.TestHarness, subscription wire.SubscriptionPath, partitions int, group *errgroup.Group) *subscriber {
	s := &subscriber{
		Subscription: subscription,
		counter:      newReceiveCounter(partitions),
	}

	sub := harness.StartSubscriber(subscription)
	s.Sub = sub
	group.Go(func() error {
		log.Printf("Subscriber %s listening to messages...", subscription)
		err := sub.Receive(ctx, s.onReceive)
		log.Fatalf("%s: stopped with error: %v", subscription, err)
		return err
	})
	s.counter.Poll()

	return s
}

func (s *subscriber) onReceive(ctx context.Context, msg *pubsub.Message) {
	msg.Ack()
	m, err := pscompat.ParseMessageMetadata(msg.ID)
	if err != nil {
		log.Fatalf("Failed to parse metadata %q: %v", msg.ID, err)
	}
	count := s.counter.OnMsgReceived(m.Partition)
	if *verbose || count%int64(*printInterval) == 0 {
		log.Printf("Received: offset=%s, data=%s, published=%v, count=%d", msg.ID, truncateMsg(string(msg.Data)), msg.PublishTime, count)
	}
	if *ackDelay > 0 {
		time.Sleep(*ackDelay)
	}
}

func main() {
	harness := integration.NewTestHarness()

	// Setup subscribers.
	group, gctx := errgroup.WithContext(context.Background())
	var subscribers []*subscriber
	for _, subscription := range harness.Subscriptions {
		subscribers = append(subscribers, newSubscriber(gctx, harness, subscription, harness.TopicPartitionCount, group))
	}
	group.Wait()
}
