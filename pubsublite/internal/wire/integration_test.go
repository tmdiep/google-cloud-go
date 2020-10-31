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
	"testing"
	"time"

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

func TestSubscribe(t *testing.T) {
	initIntegrationTest(t)

	ctx := context.Background()
	proj := testutil.ProjID()
	zone := "us-central1-b"
	region := "us-central1"
	resourceID := "go-publish-test"
	subscription := subscriptionPartition{
		path:      fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s", proj, zone, resourceID),
		partition: 0,
	}

	subsClient, err := newSubscriberClient(ctx, region)
	if err != nil {
		t.Fatal(err)
	}
	cursorClient, err := newCursorClient(ctx, region)
	if err != nil {
		t.Fatal(err)
	}

	settings := DefaultReceiveSettings
	settings.MaxOutstandingMessages = 20
	subscriber := newSinglePartitionSubscriber(ctx, subsClient, cursorClient, settings, subscription)

	subscriber.Start()
	if err := subscriber.WaitStarted(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	receive := func(msg *pb.SequencedMessage, ack *ackReplyConsumer) {
		fmt.Printf("Got msg: %v\n", msg)
		ack.Ack()
	}
	subscriber.Receive(receive)

	time.Sleep(5 * time.Second)

	subscriber.Stop()
	subscriber.WaitStopped()
}
