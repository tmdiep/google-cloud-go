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
	"strings"

	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/internal/wire"
	"cloud.google.com/go/pubsublite/pscompat"
)

var (
	project          = flag.String("project", "", "the project owning the topic/subscription resources")
	zone             = flag.String("zone", "", "the cloud zone where the topic/subscription resources are located")
	topicID          = flag.String("topic", "", "the topic to publish to")
	subscriptionIDs  = flag.String("subscription", "", "comma separated subscriptions to receive from")
	enableAssignment = flag.Bool("assignment", false, "use partition assignment for subscribers")
	publishBatchSize = flag.Int("publish_setting_batch", 100, "publish batch size")
	enableLogging    = flag.Bool("logging", true, "log informational messages")
)

type TestHarness struct {
	PublishSettings     pscompat.PublishSettings
	ReceiveSettings     pscompat.ReceiveSettings
	EnableAssignment    bool
	TopicPartitionCount int
	Topic               wire.TopicPath
	Subscriptions       []wire.SubscriptionPath

	region string
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

	if *zone == "" {
		log.Fatal("Must provide --zone of the topic & subscription resources")
	}
	if *topicID == "" {
		log.Fatal("Must set --topic to the topic ID")
	}
	subsIDs := *topicID
	if *subscriptionIDs != "" {
		subsIDs = *subscriptionIDs
	}

	var err error
	th.region, err = wire.ZoneToRegion(*zone)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	admin, err := pubsublite.NewAdminClient(ctx, th.region)
	if err != nil {
		log.Fatal(err)
	}

	th.Topic = wire.TopicPath{Project: proj, Zone: *zone, TopicID: *topicID}
	th.TopicPartitionCount, err = admin.TopicPartitionCount(ctx, th.Topic.String())
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Topic %s has %d partitions", th.Topic, th.TopicPartitionCount)

	for _, subsID := range strings.Split(subsIDs, ",") {
		subscription := wire.SubscriptionPath{Project: proj, Zone: *zone, SubscriptionID: subsID}
		if _, err := admin.Subscription(ctx, subscription.String()); err != nil {
			log.Fatal(err)
		}
		th.Subscriptions = append(th.Subscriptions, subscription)
	}

	th.PublishSettings = pscompat.DefaultPublishSettings
	th.PublishSettings.CountThreshold = *publishBatchSize
	th.ReceiveSettings = pscompat.DefaultReceiveSettings
	th.EnableAssignment = *enableAssignment
	/*
		if *enableLogging {
			onLog := func(msg string) {
				log.Printf(msg)
			}
			th.PublishSettings.OnLog = onLog
			th.ReceiveSettings.OnLog = onLog
		}*/
}

func (th *TestHarness) StartPublisher() *pscompat.PublisherClient {
	publisher, err := pscompat.NewPublisherClientWithSettings(context.Background(), th.Topic.String(), th.PublishSettings)
	if err != nil {
		log.Fatal(err)
	}
	return publisher
}

func (th *TestHarness) StartFirstSubscriber() *pscompat.SubscriberClient {
	return th.StartSubscriber(th.Subscriptions[0])
}

func (th *TestHarness) StartSubscriber(subscription wire.SubscriptionPath) *pscompat.SubscriberClient {
	settings := th.ReceiveSettings
	if !th.EnableAssignment {
		for p := 0; p < th.TopicPartitionCount; p++ {
			settings.Partitions = append(settings.Partitions, p)
		}
	}
	subscriber, err := pscompat.NewSubscriberClientWithSettings(context.Background(), subscription.String(), settings)
	if err != nil {
		log.Fatal(err)
	}
	return subscriber
}
