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
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/internal/test"
	"cloud.google.com/go/pubsublite/internal/test/integration"
	"cloud.google.com/go/pubsublite/internal/wire"
	"cloud.google.com/go/pubsublite/pscompat"
	"golang.org/x/sync/errgroup"
)

var (
	waitTimeout = flag.Duration("timeout", 2*time.Minute, "timeout for receiving all messages per cycle")
	verbose     = flag.Bool("verbose", true, "whether to log verbose messages")
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
	OrderingValidator *test.OrderingReceiver
	DuplicateDetector *test.DuplicateMsgDetector
}

func newSubscriber(ctx context.Context, harness *integration.TestHarness, subscription wire.SubscriptionPath, group *errgroup.Group) *subscriber {
	s := &subscriber{
		Subscription:      subscription,
		OrderingValidator: test.NewOrderingReceiver(),
		DuplicateDetector: test.NewDuplicateMsgDetector(),
	}

	sub := harness.StartSubscriber(subscription)
	s.Sub = sub
	group.Go(func() error {
		log.Printf("Subscriber %s listening to messages...", subscription)
		err := sub.Receive(ctx, s.onReceive)
		log.Fatalf("%s: stopped with error: %v", subscription, err)
		return err
	})

	return s
}

func (s *subscriber) onReceive(ctx context.Context, msg *pubsub.Message) {
	msg.Ack()

	data := string(msg.Data)
	offset, _ := strconv.ParseInt(msg.ID, 10, 64)
	key := msg.OrderingKey
	log.Printf("Received: (key=%s, offset=%d) %s", key, offset, data)

	// Ordering and duplicate validation.
	if err := s.OrderingValidator.Receive(data, key); err != nil {
		log.Fatalf("%s: %v", s.Subscription, err)
	}
	s.DuplicateDetector.Receive(data, offset)
	if s.DuplicateDetector.HasReceiveDuplicates() {
		log.Fatalf("%s: %s", s.Subscription, s.DuplicateDetector.Status())
	}
}

func main() {
	harness := integration.NewTestHarness()

	// Setup subscribers.
	group, gctx := errgroup.WithContext(context.Background())
	var subscribers []*subscriber
	for _, subscription := range harness.Subscriptions {
		subscribers = append(subscribers, newSubscriber(gctx, harness, subscription, group))
	}
	group.Wait()
}
