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
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/internal/wire"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

var (
	project        = flag.String("project", "", "the project owning the topic/subscription resources")
	zone           = flag.String("zone", "us-central1-a", "the cloud zone where the topic/subscription resources are located")
	topicID        = flag.String("topic", "go-longrunning-test", "the topic to publish to")
	subscriptionID = flag.String("subscription", "go-longrunning-test", "the subscription to receive from")
	numMessages    = flag.Int("messages", 10, "the number of messages to publish and receive")
)

const sleepPeriod = 60 * time.Second

func publishAndReceive(ctx context.Context, topic pubsublite.TopicPath, subscription pubsublite.SubscriptionPath, region string, numPartitions int) {
	start := time.Now()

	publisher, err := wire.NewPublisher(ctx, wire.DefaultPublishSettings, region, topic.String())
	if err != nil {
		log.Fatal(err)
	}
	publisher.Start()
	if err := publisher.WaitStarted(); err != nil {
		log.Fatal(err)
	}
	onPublished := func(pm *wire.PublishMetadata, err error) {
		if err != nil {
			log.Fatalf("Publish error: %v, time elapsed: %v", err, time.Now().Sub(start))
		}
		log.Printf("Published: (partition=%d, offset=%d)", pm.Partition, pm.Offset)
	}

	onReceive := func(msg *pb.SequencedMessage, ack wire.AckConsumer) {
		log.Printf("Received: (offset=%d) %s", msg.GetCursor().GetOffset(), string(msg.GetMessage().GetData()))
		ack.Ack()
	}
	receiveSettings := wire.DefaultReceiveSettings
	for i := 0; i < numPartitions; i++ {
		receiveSettings.Partitions = append(receiveSettings.Partitions, i)
	}
	subscriber, err := wire.NewSubscriber(ctx, receiveSettings, onReceive, region, subscription.String())
	if err != nil {
		log.Fatal(err)
	}
	subscriber.Start()
	if err := subscriber.WaitStarted(); err != nil {
		log.Fatal(err)
	}

	// Detect when the subscriber has stopped.
	go func() {
		log.Printf("Waiting for subscriber to stop...")
		err := subscriber.WaitStopped()
		log.Printf("Subscriber stopped with error: %v, time elapsed: %v", err, time.Now().Sub(start))
		os.Exit(1)
	}()

	// Main publishing loop.
	log.Printf("Running test...")
	for {
		for i := 0; i < *numMessages; i++ {
			publisher.Publish(&pb.PubSubMessage{Data: []byte(fmt.Sprintf("hello%d", i))}, onPublished)
		}

		log.Printf("Time elapsed: %v", time.Now().Sub(start))
		time.Sleep(sleepPeriod)
	}
}

func main() {
	flag.Parse()

	proj := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if *project != "" {
		proj = *project
	}
	if proj == "" {
		log.Fatal("Must provide --project or set GOOGLE_CLOUD_PROJECT environment variable")
	}
	if _, err := strconv.ParseInt(proj, 10, 64); err != nil {
		log.Fatal("Only project number currently supported.")
	}

	ctx := context.Background()
	topic := pubsublite.TopicPath{Project: proj, Zone: *zone, TopicID: *topicID}
	subscription := pubsublite.SubscriptionPath{Project: proj, Zone: *zone, SubscriptionID: *subscriptionID}
	region, err := pubsublite.ZoneToRegion(*zone)
	if err != nil {
		log.Fatal(err)
	}

	admin, err := pubsublite.NewAdminClient(ctx, region)
	if err != nil {
		log.Fatal(err)
	}
	numPartitions, err := admin.TopicPartitions(ctx, topic)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Verified topic: %s, partition count: %d", topic, numPartitions)
	if _, err := admin.Subscription(ctx, subscription); err != nil {
		log.Fatal(err)
	}
	log.Printf("Verified subscription: %s ", subscription)

	publishAndReceive(ctx, topic, subscription, region, numPartitions)
}
