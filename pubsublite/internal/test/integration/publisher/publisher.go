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
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite/internal/test"
	"cloud.google.com/go/pubsublite/internal/test/integration"
)

var (
	messageCount = flag.Int("message_count", 5, "the number of messages to publish and receive per cycle, per partition")
	sleepPeriod  = flag.Duration("sleep", time.Minute, "the duration to sleep between cycles")
	verbose      = flag.Bool("verbose", true, "whether to log verbose messages")
)

func main() {
	ctx := context.Background()
	harness := integration.NewTestHarness()
	start := time.Now()

	// Setup publisher.
	publisher := harness.StartPublisher()

	// Main publishing loop.
	log.Printf("Publishing...")
	msgPrefix := fmt.Sprintf("publisher-%d", start.Unix())
	orderingSender := test.NewOrderingSender()
	cycleMsgCount := *messageCount * harness.TopicPartitionCount

	for {
		cycleStart := time.Now()
		var toPublish []*pubsub.Message
		var trackedMsgs []string

		for partition := 0; partition < harness.TopicPartitionCount; partition++ {
			for i := 0; i < *messageCount; i++ {
				key := fmt.Sprintf("p%d", partition)
				data := orderingSender.Next(msgPrefix)
				trackedMsgs = append(trackedMsgs, data)
				toPublish = append(toPublish, &pubsub.Message{OrderingKey: key, Data: []byte(data)})
				if *verbose {
					log.Printf("Publishing: (key=%s) %s", key, data)
				}
			}
		}

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

		now := time.Now()
		log.Printf("*** Cycle elapsed: %v, cycle messages: %d, total elapsed: %v, total messages: %d ****",
			now.Sub(cycleStart), cycleMsgCount, now.Sub(start), orderingSender.TotalMsgCount)

		if *sleepPeriod > 0 {
			time.Sleep(*sleepPeriod)
		}
	}
}
