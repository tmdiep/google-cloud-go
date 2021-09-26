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
	"runtime/pprof"
	"strings"
	"time"

	"cloud.google.com/go/internal/testutil"
	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/internal/wire"
	"cloud.google.com/go/pubsublite/pscompat"
	"google.golang.org/api/option"

	vkit "cloud.google.com/go/pubsublite/apiv1"
)

var (
	project          = flag.String("project", "", "the project owning the topic/subscription resources")
	location         = flag.String("location", "", "the cloud location where the topic/subscription resources are located")
	topicID          = flag.String("topic", "", "the topic to publish to")
	subscriptionIDs  = flag.String("subscription", "", "comma separated subscriptions to receive from")
	enableAssignment = flag.Bool("assignment", false, "use partition assignment for subscribers")
	publishBatchSize = flag.Int("publish_setting_batch", 100, "publish batch size")
	enableLogging    = flag.Bool("logging", true, "log informational messages")
	autopush         = flag.Bool("autopush", false, "use autopush")
	connectTimeout   = flag.Duration("connect_timeout", 60*time.Second, "timeout for connecting to the server")
	cpuprofile       = flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile       = flag.String("memprofile", "", "write memory profile to `file`")
)

func withGRPCHeadersAssertion(opts ...option.ClientOption) []option.ClientOption {
	grpcHeadersEnforcer := &testutil.HeadersEnforcer{
		OnFailure: log.Printf,
		Checkers: []*testutil.HeaderChecker{
			testutil.XGoogClientHeaderChecker,
		},
	}
	return append(grpcHeadersEnforcer.CallOptions(), opts...)
}

func clientOptions(opts ...option.ClientOption) []option.ClientOption {
	ts := testutil.TokenSource(context.Background(), vkit.DefaultAuthScopes()...)
	ret := withGRPCHeadersAssertion(option.WithTokenSource(ts))
	if *autopush {
		ret = append(ret, option.WithEndpoint("us-central1-autopush-pubsublite.sandbox.googleapis.com:443"))
	}
	return append(ret, opts...)
}

type TestHarness struct {
	AdminClient         *pubsublite.AdminClient
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

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	proj := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if *project != "" {
		proj = *project
	}
	if proj == "" {
		log.Fatal("Must provide --project or set GOOGLE_CLOUD_PROJECT environment variable")
	}

	if *location == "" {
		log.Fatal("Must provide --location of the topic & subscription resources")
	}
	if *topicID == "" {
		log.Fatal("Must set --topic to the topic ID")
	}
	subsIDs := *topicID
	if *subscriptionIDs != "" {
		subsIDs = *subscriptionIDs
	}

	var err error
	th.region, err = wire.LocationToRegion(*location)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	th.AdminClient, err = pubsublite.NewAdminClient(ctx, th.region, clientOptions()...)
	if err != nil {
		log.Fatal(err)
	}

	th.Topic = wire.TopicPath{Project: proj, Location: *location, TopicID: *topicID}
	th.TopicPartitionCount, err = th.AdminClient.TopicPartitionCount(ctx, th.Topic.String())
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Topic %s has %d partitions", th.Topic, th.TopicPartitionCount)

	for _, subsID := range strings.Split(subsIDs, ",") {
		subscription := wire.SubscriptionPath{Project: proj, Location: *location, SubscriptionID: subsID}
		if _, err := th.AdminClient.Subscription(ctx, subscription.String()); err != nil {
			log.Fatal(err)
		}
		th.Subscriptions = append(th.Subscriptions, subscription)
	}

	th.PublishSettings = pscompat.DefaultPublishSettings
	th.PublishSettings.CountThreshold = *publishBatchSize
	th.PublishSettings.Timeout = *connectTimeout
	th.ReceiveSettings = pscompat.DefaultReceiveSettings
	th.ReceiveSettings.Timeout = *connectTimeout
	th.EnableAssignment = *enableAssignment
}

func (th *TestHarness) StartPublisher() *pscompat.PublisherClient {
	publisher, err := pscompat.NewPublisherClientWithSettings(context.Background(), th.Topic.String(), th.PublishSettings, clientOptions()...)
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
	subscriber, err := pscompat.NewSubscriberClientWithSettings(context.Background(), subscription.String(), settings, clientOptions()...)
	if err != nil {
		log.Fatal(err)
	}
	return subscriber
}

func (th *TestHarness) TopicStatsClient() *vkit.TopicStatsClient {
	client, err := vkit.NewTopicStatsClient(context.Background(), clientOptions()...)
	if err != nil {
		log.Fatalf("Failed to create topic stats client: %v", err)
	}
	return client
}

func (th *TestHarness) WriteMemProfile() {
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		//runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
