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

package integration

import (
	"context"
	"flag"
	"log"
	"os"
	"strconv"

	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/internal/wire"
)

var (
	project          = flag.String("project", "", "the project owning the topic/subscription resources")
	zone             = flag.String("zone", "", "the cloud zone where the topic/subscription resources are located")
	topicID          = flag.String("topic", "", "the topic to publish to")
	subscriptionID   = flag.String("subscription", "", "the subscription to receive from")
	enableAssignment = flag.Bool("enable_assignment", false, "use partition assignment for subscribers")
)

type TestHarness struct {
	topic        pubsublite.TopicPath
	subscription pubsublite.SubscriptionPath
	region       string
}

func NewTestHarness() *TestHarness {
	th := new(TestHarness)
	th.init()
	return th
}

func (th *TestHarness) init() {
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

	if *zone == "" {
		log.Fatal("Must provide --zone of the topic & subscription resources")
	}
	if *topicID == "" {
		log.Fatal("Must set --topic to the topic ID")
	}
	subsID := *topicID
	if *subscriptionID != "" {
		subsID = *subscriptionID
	}

	var err error
	th.region, err = pubsublite.ZoneToRegion(*zone)
	if err != nil {
		log.Fatal(err)
	}
	th.topic = pubsublite.TopicPath{Project: proj, Zone: *zone, TopicID: *topicID}
	th.subscription = pubsublite.SubscriptionPath{Project: proj, Zone: *zone, SubscriptionID: subsID}
}

func (th *TestHarness) StartPublisher() wire.Publisher {
	publisher, err := wire.NewPublisher(context.Background(), wire.DefaultPublishSettings, th.region, th.topic.String())
	if err != nil {
		log.Fatal(err)
	}
	publisher.Start()
	if err := publisher.WaitStarted(); err != nil {
		log.Fatal(err)
	}
	return publisher
}

func (th *TestHarness) StartSubscriber(onReceive wire.MessageReceiverFunc) wire.Subscriber {
	settings := wire.DefaultReceiveSettings
	if !*enableAssignment {
		for i := 0; i < th.topicPartitions(); i++ {
			settings.Partitions = append(settings.Partitions, i)
		}
	}
	subscriber, err := wire.NewSubscriber(context.Background(), settings, onReceive, th.region, th.subscription.String())
	if err != nil {
		log.Fatal(err)
	}
	subscriber.Start()
	if err := subscriber.WaitStarted(); err != nil {
		log.Fatal(err)
	}
	return subscriber
}

func (th *TestHarness) topicPartitions() int {
	ctx := context.Background()
	admin, err := pubsublite.NewAdminClient(ctx, th.region)
	if err != nil {
		log.Fatal(err)
	}
	numPartitions, err := admin.TopicPartitions(ctx, th.topic)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Topic %s has %d partitions", th.topic, numPartitions)
	return numPartitions
}
