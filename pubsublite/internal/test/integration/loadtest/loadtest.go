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
	"flag"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/internal/uid"
	"cloud.google.com/go/pubsublite/common"
	"cloud.google.com/go/pubsublite/internal/test"
	"cloud.google.com/go/pubsublite/internal/test/integration"
	"cloud.google.com/go/pubsublite/internal/wire"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	messageCount       = flag.Int("message_count", 4000, "the number of messages to publish and receive")
	messageSize        = flag.Int("message_size", 500000, "the size (bytes) of each message")
	batchSize          = flag.Int("batch_size", 1000, "the maximum number of messages per batch")
	printInterval      = flag.Int("print_interval", 100, "print status every n-th message sent/received")
	maxReceiveWaitTime = flag.Duration("receive_wait", 30*time.Minute, "wait to receive all messages before terminating subscriber")

	msgTags = uid.NewSpace("loadtest", nil)
)

const tagAttribute = "tag"

// publishBatch publishes a batch of messages and waits until all publish
// results have been received.
func publishBatch(msgTracker *test.MsgTracker, publisher wire.Publisher, batchCount int, publishedCount *int32) {
	var pubWG sync.WaitGroup
	pubWG.Add(batchCount)

	onPublished := func(pm *common.PublishMetadata, err error) {
		if err != nil {
			log.Fatalf("Publish error: %v", err)
		}

		pubWG.Done()
		count := atomic.AddInt32(publishedCount, 1)
		if count%int32(*printInterval) == 0 {
			log.Printf("Published: msg #%d (partition=%d, offset=%d)", count, pm.Partition, pm.Offset)
		}
	}

	var tags []string
	for i := 0; i < batchCount; i++ {
		tag := msgTags.New()
		tags = append(tags, tag)

		msg := &pb.PubSubMessage{
			Data: bytes.Repeat([]byte{'*'}, *messageSize),
			Attributes: map[string]*pb.AttributeValues{
				tagAttribute: {Values: [][]byte{[]byte(tag)}},
			},
		}
		publisher.Publish(msg, onPublished)
	}

	msgTracker.Add(tags...)
	pubWG.Wait()
}

func publishAll(harness *integration.TestHarness, msgTracker *test.MsgTracker) {
	start := time.Now()
	publisher := harness.StartPublisher()
	log.Printf("Publisher started in: %v", time.Now().Sub(start))

	var publishedCount int32
	for messagesRemaining := *messageCount; messagesRemaining > 0; {
		batchCount := messagesRemaining
		if batchCount > *batchSize {
			batchCount = *batchSize
		}
		messagesRemaining -= batchCount
		publishBatch(msgTracker, publisher, batchCount, &publishedCount)
	}

	publisher.Stop()
	log.Printf("**** Publish elapsed time: %v ****", time.Now().Sub(start))
}

func receiveAll(harness *integration.TestHarness, msgTracker *test.MsgTracker) {
	start := time.Now()
	var receivedCount int32

	onReceive := func(m *wire.ReceivedMessage) {
		m.Ack.Ack()

		var tag string
		if values, exists := m.Msg.GetMessage().Attributes[tagAttribute]; exists {
			if len(values.Values) > 0 {
				tag = string(values.Values[0])
			}
		}
		if !msgTracker.Remove(tag) {
			// Ignore messages that were not sent during this test run.
			return
		}

		count := atomic.AddInt32(&receivedCount, 1)
		if count%int32(*printInterval) == 0 {
			log.Printf("Received: msg #%d (offset=%d)", count, m.Msg.GetCursor().GetOffset())
		}
	}

	subscriber := harness.StartFirstSubscriber(onReceive)
	log.Printf("Subscriber started in: %v", time.Now().Sub(start))

	go func() {
		log.Printf("Receiving...")
		err := subscriber.WaitStopped()
		if err != nil {
			log.Fatalf("Subscriber stopped with error: %v, time elapsed: %v", err, time.Now().Sub(start))
		}
	}()

	msgTracker.Wait(*maxReceiveWaitTime)
	subscriber.Stop()
	log.Printf("**** Receive elapsed time: %v ****", time.Now().Sub(start))
}

func main() {
	harness := integration.NewTestHarness()
	harness.PublishSettings.BufferedByteLimit = 2 * *batchSize * *messageSize
	msgTracker := test.NewMsgTracker()

	log.Printf("Transmitting %d messages, %d bytes per message, %d total bytes", *messageCount, *messageSize, *messageCount**messageSize)
	publishAll(harness, msgTracker)
	receiveAll(harness, msgTracker)
}
