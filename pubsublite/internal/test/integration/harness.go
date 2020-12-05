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
	PublishSettings     wire.PublishSettings
	ReceiveSettings     wire.ReceiveSettings
	EnableAssignment    bool
	TopicPartitionCount int
	Topic               pubsublite.TopicPath
	Subscriptions       []pubsublite.SubscriptionPath

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
	th.region, err = pubsublite.ZoneToRegion(*zone)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	admin, err := pubsublite.NewAdminClient(ctx, th.region)
	if err != nil {
		log.Fatal(err)
	}

	th.Topic = pubsublite.TopicPath{Project: proj, Zone: *zone, TopicID: *topicID}
	th.TopicPartitionCount, err = admin.TopicPartitions(ctx, th.Topic)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Topic %s has %d partitions", th.Topic, th.TopicPartitionCount)

	for _, subsID := range strings.Split(subsIDs, ",") {
		subscription := pubsublite.SubscriptionPath{Project: proj, Zone: *zone, SubscriptionID: subsID}
		if _, err := admin.Subscription(ctx, subscription); err != nil {
			log.Fatal(err)
		}
		th.Subscriptions = append(th.Subscriptions, subscription)
	}

	th.PublishSettings = wire.DefaultPublishSettings
	th.PublishSettings.CountThreshold = *publishBatchSize
	th.ReceiveSettings = wire.DefaultReceiveSettings
	th.EnableAssignment = *enableAssignment
	if *enableLogging {
		onLog := func(msg string) {
			log.Printf(msg)
		}
		th.PublishSettings.OnLog = onLog
		th.ReceiveSettings.OnLog = onLog
	}
}

func (th *TestHarness) StartPublisher() wire.Publisher {
	publisher, err := wire.NewPublisher(context.Background(), th.PublishSettings, th.region, th.Topic.String())
	if err != nil {
		log.Fatal(err)
	}
	publisher.Start()
	if err := publisher.WaitStarted(); err != nil {
		log.Fatal(err)
	}
	return publisher
}

func (th *TestHarness) StartFirstSubscriber(onReceive wire.MessageReceiverFunc) wire.Subscriber {
	return th.StartSubscriber(th.Subscriptions[0], onReceive)
}

func (th *TestHarness) StartSubscriber(subscription pubsublite.SubscriptionPath, onReceive wire.MessageReceiverFunc) wire.Subscriber {
	settings := th.ReceiveSettings
	if !th.EnableAssignment {
		for p := 0; p < th.TopicPartitionCount; p++ {
			settings.Partitions = append(settings.Partitions, p)
		}
	}
	subscriber, err := wire.NewSubscriber(context.Background(), settings, onReceive, th.region, subscription.String())
	if err != nil {
		log.Fatal(err)
	}
	subscriber.Start()
	if err := subscriber.WaitStarted(); err != nil {
		log.Fatal(err)
	}
	return subscriber
}
