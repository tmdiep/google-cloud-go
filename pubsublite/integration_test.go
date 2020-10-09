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

package pubsublite

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/internal/testutil"
)

const (
	gibi = 1 << 30
)

func checkRunPreconditions(t *testing.T) {
	if testing.Short() {
		t.Skip("Integration tests skipped in short mode")
	}
	if testutil.ProjID() == "" {
		t.Skip("Integration tests skipped. See CONTRIBUTING.md for details")
	}
}

func cleanUpTopic(ctx context.Context, t *testing.T, client *Client, name TopicPath) {
	if err := client.DeleteTopic(ctx, name); err != nil {
		t.Errorf("Failed to delete topic %s: %v", name, err)
	}
}

func TestResourceAdminOperations(t *testing.T) {
	checkRunPreconditions(t)

	ctx := context.Background()
	proj := Project(testutil.ProjID())
	zone := CloudZone("us-central1-b")
	resourceID := "integration-test-go-1"

	client, err := NewClient(ctx, zone.Region())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	topicPath := TopicPath{Project: proj, Zone: zone, ID: TopicID(resourceID)}
	newTopicConfig := &TopicConfig{
		Name:                       topicPath,
		PartitionCount:             2,
		PublishCapacityMiBPerSec:   4,
		SubscribeCapacityMiBPerSec: 4,
		PerPartitionBytes:          30 * gibi,
		RetentionDuration:          time.Duration(24 * time.Hour),
	}

	gotTopicConfig, err := client.CreateTopic(ctx, newTopicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer cleanUpTopic(ctx, t, client, topicPath)
	if !testutil.Equal(gotTopicConfig, newTopicConfig) {
		t.Errorf("CreateTopic() got: %v, want: %v", gotTopicConfig, newTopicConfig)
	}

	if gotTopicConfig, err := client.Topic(ctx, topicPath); err != nil {
		t.Errorf("Failed to get topic: %v", err)
	} else if !testutil.Equal(gotTopicConfig, newTopicConfig) {
		t.Errorf("Topic() got: %v, want: %v", gotTopicConfig, newTopicConfig)
	}

	topicUpdate1 := &TopicConfigToUpdate{
		Name:                       topicPath,
		PublishCapacityMiBPerSec:   6,
		SubscribeCapacityMiBPerSec: 8,
	}
	wantUpdatedTopicConfig1 := &TopicConfig{
		Name:                       topicPath,
		PartitionCount:             2,
		PublishCapacityMiBPerSec:   6,
		SubscribeCapacityMiBPerSec: 8,
		PerPartitionBytes:          30 * gibi,
		RetentionDuration:          time.Duration(24 * time.Hour),
	}
	if gotTopicConfig, err := client.UpdateTopic(ctx, topicUpdate1); err != nil {
		t.Errorf("Failed to update topic: %v", err)
	} else if !testutil.Equal(gotTopicConfig, wantUpdatedTopicConfig1) {
		t.Errorf("UpdateTopic() got: %v, want: %v", gotTopicConfig, wantUpdatedTopicConfig1)
	}

	topicUpdate2 := &TopicConfigToUpdate{
		Name:              topicPath,
		PerPartitionBytes: 35 * gibi,
		RetentionDuration: time.Duration(0),
	}
	wantUpdatedTopicConfig2 := &TopicConfig{
		Name:                       topicPath,
		PartitionCount:             2,
		PublishCapacityMiBPerSec:   6,
		SubscribeCapacityMiBPerSec: 8,
		PerPartitionBytes:          35 * gibi,
	}
	if gotTopicConfig, err := client.UpdateTopic(ctx, topicUpdate2); err != nil {
		t.Errorf("Failed to update topic: %v", err)
	} else if !testutil.Equal(gotTopicConfig, wantUpdatedTopicConfig2) {
		t.Errorf("UpdateTopic() got: %v, want: %v", gotTopicConfig, wantUpdatedTopicConfig2)
	}
}
