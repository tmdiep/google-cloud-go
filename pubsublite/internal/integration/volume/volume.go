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
volume attempts to publish and receive a high volume of data.

Example simple usage:
  go run volume.go --project=<project> --topic=<topic id> --zone=<zone>
*/
package main

import (
	"bytes"
	"flag"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsublite/internal/integration"
	"cloud.google.com/go/pubsublite/internal/wire"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	messageCount  = flag.Int("message_count", 4000, "the number of messages to publish and receive")
	messageSize   = flag.Int("message_size", 500000, "the size (bytes) of each message")
	batchSize     = flag.Int("batch_size", 1000, "the maximum number of messages per batch")
	printInterval = flag.Int("print_interval", 100, "print status every n-th message sent/received")
)

func publishBatch(publisher wire.Publisher, batchCount int, publishedCount *int32) {
	var pubWG sync.WaitGroup
	pubWG.Add(batchCount)

	onPublished := func(pm *wire.PublishMetadata, err error) {
		if err != nil {
			log.Fatalf("Publish error: %v", err)
		}

		pubWG.Done()
		count := atomic.AddInt32(publishedCount, 1)
		if count%int32(*printInterval) == 0 {
			log.Printf("Published: msg #%d (partition=%d, offset=%d)", count, pm.Partition, pm.Offset)
		}
	}

	for i := 0; i < batchCount; i++ {
		publisher.Publish(&pb.PubSubMessage{Data: bytes.Repeat([]byte{'*'}, *messageSize)}, onPublished)
	}
	pubWG.Wait()
}

func main() {
	harness := integration.NewTestHarness()
	harness.PublishSettings.BufferedByteLimit = 2 * *batchSize * *messageSize
	log.Printf("Transmitting %d messages, %d bytes per message, %d total bytes", *messageCount, *messageSize, *messageCount**messageSize)

	// Publish.
	start := time.Now()
	publisher := harness.StartPublisher()

	var publishedCount int32
	for messagesRemaining := *messageCount; messagesRemaining > 0; {
		batchCount := messagesRemaining
		if batchCount > *batchSize {
			batchCount = *batchSize
		}
		messagesRemaining -= batchCount
		publishBatch(publisher, batchCount, &publishedCount)
	}

	log.Printf("**** Publish elapsed time: %v ****", time.Now().Sub(start))
	publisher.Stop()

	// Receive.
	var subWG sync.WaitGroup
	subWG.Add(*messageCount)
	var receivedCount int32

	onReceive := func(msg *pb.SequencedMessage, ack wire.AckConsumer) {
		ack.Ack()
		subWG.Done()

		count := atomic.AddInt32(&receivedCount, 1)
		if count%int32(*printInterval) == 0 {
			log.Printf("Received: msg #%d (offset=%d)", count, msg.GetCursor().GetOffset())
		}
	}
	start = time.Now()
	subscriber := harness.StartSubscriber(onReceive)

	go func() {
		log.Printf("Receiving...")
		err := subscriber.WaitStopped()
		if err != nil {
			log.Fatalf("Subscriber stopped with error: %v, time elapsed: %v", err, time.Now().Sub(start))
		}
	}()

	subWG.Wait()
	log.Printf("**** Receive elapsed time: %v ****", time.Now().Sub(start))
	subscriber.Stop()
}
