// Copyright 2021 Google LLC
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
	"log"
	"time"

	"cloud.google.com/go/pubsublite"

	"cloud.google.com/go/pubsublite/internal/test/integration"
)

var (
	sleepPeriod   = flag.Duration("sleep", 30*time.Second, "the duration to sleep between cycles")
	opwaitTimeout = flag.Duration("operation_timeout", 2*time.Minute, "timeout for waiting for operation")
	seekTime      = flag.String("publish_time", "2021-09-05T07:00:00Z", "seek to publish time")
)

func main() {
	pubTime, err := time.Parse(time.RFC3339, *seekTime)
	if err != nil {
		log.Fatalf("Failed to parse seek time %q: %v", *seekTime, err)
	}
	log.Printf("Seek time: %v", pubTime)
	seekTarget := pubsublite.PublishTime(pubTime)

	ctx := context.Background()
	harness := integration.NewTestHarness()

	var seekCount int64
	subscription := harness.Subscriptions[0]

	for {
		op, err := harness.AdminClient.SeekSubscription(ctx, subscription.String(), seekTarget)
		if err != nil {
			log.Fatalf("Failed to seek %s to time %v", subscription, pubTime)
		}

		log.Printf("Initiated seek %s (%d)", op.Name(), seekCount)

		cctx, _ := context.WithTimeout(ctx, *opwaitTimeout)
		if _, err := op.Wait(cctx); err != nil {
			log.Fatalf("Waiting for seek operation %s returned err: %v", op.Name(), err)
		}
		m, err := op.Metadata()
		if err != nil {
			log.Fatalf("Failed to parse seek metadata: %v", err)
		}

		seekCount++
		log.Printf("Seek completed in %v", m.EndTime.Sub(m.CreateTime))
		time.Sleep(*sleepPeriod)
	}
}
