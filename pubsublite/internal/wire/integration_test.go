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

package wire

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"cloud.google.com/go/internal/testutil"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

func initIntegrationTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Integration tests skipped in short mode")
	}
	if testutil.ProjID() == "" {
		t.Skip("Integration tests skipped. See CONTRIBUTING.md for details")
	}
	// The Pub/Sub Lite server will accept project ID or number by EOQ4, 2020.
	if _, err := strconv.ParseInt(testutil.ProjID(), 10, 64); err != nil {
		t.Skip("Integration tests skipped. Only project number currently supported.")
	}
}

func TestPubSub(t *testing.T) {
	initIntegrationTest(t)

	ctx := context.Background()
	proj := testutil.ProjID()
	zone := "us-central1-b"
	region := "us-central1"
	resourceID := "go-publish-test-4"
	topicPath := fmt.Sprintf("projects/%s/locations/%s/topics/%s", proj, zone, resourceID)
	subscriptionPath := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s", proj, zone, resourceID)
	numMessages := 20

	adminClient, err := NewAdminClient(ctx, region)
	if err != nil {
		t.Fatal(err)
	}

	partitions, err := adminClient.GetTopicPartitions(ctx, &pb.GetTopicPartitionsRequest{Name: topicPath})
	if err != nil {
		t.Fatal(err)
	}

	pubSettings := DefaultPublishSettings
	publisher, err := NewPublisher(ctx, pubSettings, region, topicPath)
	if err != nil {
		t.Fatal(err)
	}

	publisher.Start()
	if err := publisher.WaitStarted(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	published := func(pm *PublishMetadata, err error) {
		fmt.Printf("Published msg: %s, %v\n", pm, err)
	}

	for i := 0; i < numMessages; i++ {
		publisher.Publish(&pb.PubSubMessage{Data: []byte(fmt.Sprintf("%d", i))}, published)
	}

	publisher.Stop()
	publisher.WaitStopped()

	settings := DefaultReceiveSettings
	settings.MaxOutstandingMessages = 8
	for i := 0; i < int(partitions.GetPartitionCount()); i++ {
		// Comment out to use assigning subscriber
		settings.Partitions = append(settings.Partitions, i)
	}

	var wg sync.WaitGroup
	wg.Add(numMessages)
	receive := func(msg *pb.SequencedMessage, ack AckConsumer) {
		wg.Done()
		fmt.Printf("Received: offset=%d, data=%s\n", msg.GetCursor().GetOffset(), string(msg.GetMessage().GetData()))
		ack.Ack()
	}

	subscriber, err := NewSubscriber(ctx, settings, receive, region, subscriptionPath)
	if err != nil {
		t.Fatal(err)
	}
	subscriber.Start()
	if err := subscriber.WaitStarted(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	wg.Wait()
	//time.Sleep(5 * time.Second) // Comment out wg.Done

	fmt.Println("Stopping")
	subscriber.Stop()
	subscriber.WaitStopped()
}
