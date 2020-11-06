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
longrunning attempts to publish and subscribe for as long as possible.
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsublite/internal/integration"
	"cloud.google.com/go/pubsublite/internal/wire"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	messageCount = flag.Int("message_count", 10, "the number of messages to publish and receive")
)

const sleepPeriod = 60 * time.Second

func main() {
	start := time.Now()
	harness := integration.NewTestHarness()

	// Setup subscriber.
	onReceive := func(msg *pb.SequencedMessage, ack wire.AckConsumer) {
		log.Printf("Received: (offset=%d) %s", msg.GetCursor().GetOffset(), string(msg.GetMessage().GetData()))
		ack.Ack()
	}
	subscriber := harness.StartSubscriber(onReceive)
	go func() {
		log.Printf("Listening to messages...")
		err := subscriber.WaitStopped()
		log.Fatalf("Subscriber stopped with error: %v, time elapsed: %v", err, time.Now().Sub(start))
	}()

	// Setup publisher.
	publisher := harness.StartPublisher()
	onPublished := func(pm *wire.PublishMetadata, err error) {
		if err != nil {
			log.Fatalf("Publish error: %v, time elapsed: %v", err, time.Now().Sub(start))
		}
		log.Printf("Published: (partition=%d, offset=%d)", pm.Partition, pm.Offset)
	}

	// Main publishing loop.
	log.Printf("Running test...")
	for {
		for i := 0; i < *messageCount; i++ {
			publisher.Publish(&pb.PubSubMessage{Data: []byte(fmt.Sprintf("hello-world-%d", i))}, onPublished)
		}

		log.Printf("Time elapsed: %v", time.Now().Sub(start))
		time.Sleep(sleepPeriod)
	}
}
