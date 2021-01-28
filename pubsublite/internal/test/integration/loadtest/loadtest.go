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
loadtest attempts to publish and receive a high volume of data. Pub/Sub Lite
topics are provisioned a publish and subscribe throughput, so this tests flow
control.

Example simple usage:
  go run loadtest.go --project=<project> --zone=<zone> --topic=<topic id>
*/
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/internal/test"
	"cloud.google.com/go/pubsublite/internal/test/integration"
	"cloud.google.com/go/pubsublite/pscompat"
)

var (
	messageCount       = flag.Int("message_count", 2000, "the number of messages to publish and receive")
	messageSize        = flag.Int("message_size", 500000, "the size (bytes) of each message")
	batchSize          = flag.Int("batch_size", 500, "the maximum number of messages per batch")
	printInterval      = flag.Int("print_interval", 100, "print status every n-th message sent/received")
	maxReceiveWaitTime = flag.Duration("receive_wait", 30*time.Minute, "wait to receive all messages before terminating subscriber")

	msgTagPrefix = fmt.Sprintf("loadtest-%d", time.Now().Unix())
)

const (
	tagAttribute = "tag"
	mibi         = 1 << 20
)

// publishBatch publishes a batch of messages and waits until all publish
// results have been received.
func publishBatch(ctx context.Context, msgTracker *test.MsgTracker, publisher *pscompat.PublisherClient, batchCount int, publishedCount *int32) {
	var tags []string
	var results []*pubsub.PublishResult
	for i := 0; i < batchCount; i++ {
		tag := fmt.Sprintf("%s-%d", *publishedCount+int32(i))
		tags = append(tags, tag)

		msg := &pubsub.Message{
			Data: bytes.Repeat([]byte{'*'}, *messageSize),
			Attributes: map[string]string{
				tagAttribute: tag,
			},
		}
		results = append(results, publisher.Publish(ctx, msg))
	}

	msgTracker.Add(tags...)

	for _, result := range results {
		id, err := result.Get(ctx)
		if err != nil {
			log.Fatalf("Publish error: %v", err)
		}

		count := atomic.AddInt32(publishedCount, 1)
		if count%int32(*printInterval) == 0 {
			log.Printf("Published: msg #%d (%s)", count, id)
		}
	}
}

func publishAll(ctx context.Context, harness *integration.TestHarness, msgTracker *test.MsgTracker) {
	totalBytes := float64(*messageCount) * float64(*messageSize)
	log.Printf("Transmitting %d messages, %d bytes per message, total %.2f MiB", *messageCount, *messageSize, totalBytes/float64(mibi))

	start := time.Now()
	publisher := harness.StartPublisher()
	log.Printf("Publisher started in: %v", time.Since(start))

	start = time.Now()
	var publishedCount int32
	for messagesRemaining := *messageCount; messagesRemaining > 0; {
		batchCount := messagesRemaining
		if batchCount > *batchSize {
			batchCount = *batchSize
		}
		messagesRemaining -= batchCount
		publishBatch(ctx, msgTracker, publisher, batchCount, &publishedCount)
	}

	duration := time.Since(start)
	rate := totalBytes / duration.Seconds() / float64(mibi)

	publisher.Stop()
	log.Printf("**** Publish elapsed time: %v (%.2f MiB/s) ****", time.Since(start), rate)
}

func receiveAll(ctx context.Context, harness *integration.TestHarness, msgTracker *test.MsgTracker) {
	start := time.Now()
	var receivedCount int32

	onReceive := func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()

		var tag string
		if value, exists := msg.Attributes[tagAttribute]; exists {
			tag = value
		}
		if !msgTracker.Remove(tag) {
			// Ignore messages that were not sent during this test run.
			return
		}

		count := atomic.AddInt32(&receivedCount, 1)
		if count == 1 {
			log.Printf("Subscriber started in: %v", time.Since(start))
			start = time.Now()
		}
		if count%int32(*printInterval) == 0 {
			log.Printf("Received: msg #%d (offset=%s)", count, msg.ID)
		}
	}

	subscriber := harness.StartFirstSubscriber()
	cctx, stop := context.WithCancel(ctx)

	go func() {
		log.Printf("Receiving...")
		err := subscriber.Receive(cctx, onReceive)
		if err != nil {
			log.Fatalf("Subscriber stopped with error: %v", err)
		}
	}()

	msgTracker.Wait(*maxReceiveWaitTime)
	duration := time.Since(start)
	rate := float64(*messageCount) * float64(*messageSize) / duration.Seconds() / float64(mibi)

	stop()
	log.Printf("**** Receive elapsed time: %v (%.2f MiB/s) ****", time.Since(start), rate)
}

func main() {
	harness := integration.NewTestHarness()
	harness.PublishSettings.BufferedByteLimit = 2 * *batchSize * *messageSize
	msgTracker := test.NewMsgTracker()

	ctx := context.Background()
	publishAll(ctx, harness, msgTracker)
	receiveAll(ctx, harness, msgTracker)
}
