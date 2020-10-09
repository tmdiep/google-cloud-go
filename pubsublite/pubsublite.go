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
	"fmt"

	"google.golang.org/api/option"

	vkit "cloud.google.com/go/pubsublite/apiv1"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

// Client provides an interface for Google Pub/Sub Lite admin operations for
// resources within a Google Cloud region. A Client may be shared by multiple
// goroutines.
type Client struct {
	Region CloudRegion
	admin  *vkit.AdminClient
}

// NewClient creates a new Cloud Pub/Sub Lite client for a given region.
func NewClient(ctx context.Context, region CloudRegion, opts ...option.ClientOption) (c *Client, err error) {
	options := []option.ClientOption{option.WithEndpoint(region.String() + "-pubsublite.googleapis.com:443")}
	admin, err := vkit.NewAdminClient(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("pubsublite: %v", err)
	}
	return &Client{Region: region, admin: admin}, nil
}

func (c *Client) validateRegion(resourceType string, region CloudRegion) error {
	if region != c.Region {
		return fmt.Errorf("pubsublite: %s region (%s) differs from Client region (%s)", resourceType, region, c.Region)
	}
	return nil
}

// CreateTopic creates a new topic from the given config.
func (c *Client) CreateTopic(ctx context.Context, config *TopicConfig) (*TopicConfig, error) {
	if err := c.validateRegion("topic", config.Name.Zone.Region()); err != nil {
		return nil, err
	}
	req := &pb.CreateTopicRequest{
		Parent:  config.Name.Location().String(),
		Topic:   config.toProto(),
		TopicId: config.Name.ID.String(),
	}
	topicpb, err := c.admin.CreateTopic(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("pubsublite: failed to create topic: %v", err)
	}
	return protoToTopicConfig(topicpb)
}

// UpdateTopic updates an existing topic from the given config and returns the
// new topic config.
func (c *Client) UpdateTopic(ctx context.Context, config *TopicConfigToUpdate) (*TopicConfig, error) {
	if err := c.validateRegion("topic", config.Name.Zone.Region()); err != nil {
		return nil, err
	}
	topicpb, err := c.admin.UpdateTopic(ctx, config.toUpdateRequest())
	if err != nil {
		return nil, fmt.Errorf("pubsublite: failed to update topic: %v", err)
	}
	return protoToTopicConfig(topicpb)
}

// Topic retrieves the configuration of a topic.
func (c *Client) Topic(ctx context.Context, name TopicPath) (*TopicConfig, error) {
	if err := c.validateRegion("topic", name.Zone.Region()); err != nil {
		return nil, err
	}
	topicpb, err := c.admin.GetTopic(ctx, &pb.GetTopicRequest{Name: name.String()})
	if err != nil {
		return nil, fmt.Errorf("pubsublite: failed to retrieve topic config: %v", err)
	}
	return protoToTopicConfig(topicpb)
}

// DeleteTopic deletes a topic.
func (c *Client) DeleteTopic(ctx context.Context, name TopicPath) error {
	if err := c.validateRegion("topic", name.Zone.Region()); err != nil {
		return err
	}
	if err := c.admin.DeleteTopic(ctx, &pb.DeleteTopicRequest{Name: name.String()}); err != nil {
		return fmt.Errorf("pubsublite: failed to delete topic: %v", err)
	}
	return nil
}

// CreateSubscription creates a new subscription from the given config.
func (c *Client) CreateSubscription(ctx context.Context, config *SubscriptionConfig) (*SubscriptionConfig, error) {
	if err := c.validateRegion("subscription", config.Name.Zone.Region()); err != nil {
		return nil, err
	}
	req := &pb.CreateSubscriptionRequest{
		Parent:         config.Name.Location().String(),
		Subscription:   config.toProto(),
		SubscriptionId: config.Name.ID.String(),
	}
	subspb, err := c.admin.CreateSubscription(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("pubsublite: failed to create subscription: %v", err)
	}
	return protoToSubscriptionConfig(subspb)
}

// UpdateSubscription updates an existing subscription from the given config and
// returns the new subscription config.
func (c *Client) UpdateSubscription(ctx context.Context, config *SubscriptionConfigToUpdate) (*SubscriptionConfig, error) {
	if err := c.validateRegion("subscription", config.Name.Zone.Region()); err != nil {
		return nil, err
	}
	subspb, err := c.admin.UpdateSubscription(ctx, config.toUpdateRequest())
	if err != nil {
		return nil, fmt.Errorf("pubsublite: failed to update subscription: %v", err)
	}
	return protoToSubscriptionConfig(subspb)
}

// Subscription retrieves the configuration of a subscription.
func (c *Client) Subscription(ctx context.Context, name SubscriptionPath) (*SubscriptionConfig, error) {
	if err := c.validateRegion("subscription", name.Zone.Region()); err != nil {
		return nil, err
	}
	subspb, err := c.admin.GetSubscription(ctx, &pb.GetSubscriptionRequest{Name: name.String()})
	if err != nil {
		return nil, fmt.Errorf("pubsublite: failed to retrieve subscription config: %v", err)
	}
	return protoToSubscriptionConfig(subspb)
}

// DeleteSubscription deletes a subscription.
func (c *Client) DeleteSubscription(ctx context.Context, name SubscriptionPath) error {
	if err := c.validateRegion("subscription", name.Zone.Region()); err != nil {
		return err
	}
	if err := c.admin.DeleteSubscription(ctx, &pb.DeleteSubscriptionRequest{Name: name.String()}); err != nil {
		return fmt.Errorf("pubsublite: failed to delete subscription: %v", err)
	}
	return nil
}
