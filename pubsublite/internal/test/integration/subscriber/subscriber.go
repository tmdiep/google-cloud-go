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

type partitionState struct {
	count    int64
	lastRecv time.Time
}

type receiveCounter struct {
	totalCount int64
	partitions map[int]*partitionState
	mu         sync.Mutex
}

func newReceiveCounter(numPartitions int) *receiveCounter {
	c := &receiveCounter{
		totalCount: 0,
		partitions: make(map[int]*partitionState),
	}
	now := time.Now()
	for i := 0; i < numPartitions; i++ {
		s := &partitionState{lastRecv: now}
		c.partitions[i] = s
	}
	return c
}

func (r *receiveCounter) OnMsgReceived(p int) (int64, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalCount++
	s := r.partitions[p]
	s.count++
	s.lastRecv = time.Now()
	return s.count, r.totalCount
}

func (r *receiveCounter) checkReceived() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	timeout := false
	for _, s := range r.partitions {
		if now.Sub(s.lastRecv) > *receiveTimeout {
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

	r.printStatus(now)
	log.Fatal("Timed out receiving messages")
}

func (r *receiveCounter) printStatus(now time.Time) {
	for p, s := range r.partitions {
		elapsed := now.Sub(s.lastRecv)
		suffix := ""
		if elapsed > *receiveTimeout {
			suffix = " ***"
		}
		log.Printf("[%d] %d, %v%s", p, s.count, elapsed, suffix)
	}
	log.Printf("Total messages received: %d", r.totalCount)
}

func (r *receiveCounter) PrintStatus() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.printStatus(time.Now())
}

func (r *receiveCounter) Poll() {
	go func() {
		lastPrint := time.Now()
		for {
			r.checkReceived()
			now := time.Now()
			if now.Sub(lastPrint) > 30*time.Second {
				r.PrintStatus()
				lastPrint = now
			}
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
	pcount, _ := s.counter.OnMsgReceived(m.Partition)
	if *verbose || pcount%int64(*printInterval) == 0 {
		size := len(msg.Data)
		for k, v := range msg.Attributes {
			size += len(k)
			size += len(v)
		}
		log.Printf("Received: id=%s, data=%s, size=%d, published=%v", msg.ID, truncateMsg(string(msg.Data)), size, msg.PublishTime)
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
