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

package ps

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/internal/testutil"
	"cloud.google.com/go/internal/uid"
	"cloud.google.com/go/pubsublite"
	pubsub "cloud.google.com/go/pubsublite/internal/pubsub"
	"cloud.google.com/go/pubsublite/internal/test"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/api/option"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"

	vkit "cloud.google.com/go/pubsublite/apiv1"
)

const (
	gibi               = 1 << 30
	defaultTestTimeout = 5 * time.Minute
)

var (
	resourceIDs = uid.NewSpace("go-ps-test", nil)

	// A random zone is selected for each integration test run.
	supportedZones = []string{
		"us-central1-a",
		"us-central1-b",
		"us-central1-c",
		"europe-west1-b",
		"europe-west1-d",
	}
)

func initIntegrationTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Integration tests skipped in short mode")
	}
	if testutil.ProjID() == "" {
		t.Skip("Integration tests skipped. See CONTRIBUTING.md for details")
	}
}

func withGRPCHeadersAssertion(t *testing.T, opts ...option.ClientOption) []option.ClientOption {
	grpcHeadersEnforcer := &testutil.HeadersEnforcer{
		OnFailure: t.Errorf,
		Checkers: []*testutil.HeaderChecker{
			testutil.XGoogClientHeaderChecker,
		},
	}
	return append(grpcHeadersEnforcer.CallOptions(), opts...)
}

func testOptions(ctx context.Context, t *testing.T, opts ...option.ClientOption) []option.ClientOption {
	ts := testutil.TokenSource(ctx, vkit.DefaultAuthScopes()...)
	if ts == nil {
		t.Skip("Integration tests skipped. See CONTRIBUTING.md for details")
	}
	return append(withGRPCHeadersAssertion(t, option.WithTokenSource(ts)), opts...)
}

func adminClient(ctx context.Context, t *testing.T, region string, opts ...option.ClientOption) *pubsublite.AdminClient {
	opts = testOptions(ctx, t, opts...)
	admin, err := pubsublite.NewAdminClient(ctx, region, opts...)
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	return admin
}

func publisherClient(ctx context.Context, t *testing.T, settings PublishSettings, topic pubsublite.TopicPath, opts ...option.ClientOption) *PublisherClient {
	opts = testOptions(ctx, t, opts...)
	pub, err := NewPublisherClient(ctx, settings, topic, opts...)
	if err != nil {
		t.Fatalf("Failed to create publisher client: %v", err)
	}
	return pub
}

func subscriberClient(ctx context.Context, t *testing.T, settings ReceiveSettings, subscription pubsublite.SubscriptionPath, opts ...option.ClientOption) *SubscriberClient {
	opts = testOptions(ctx, t, opts...)
	sub, err := NewSubscriberClient(ctx, settings, subscription, opts...)
	if err != nil {
		t.Fatalf("Failed to create publisher client: %v", err)
	}
	return sub
}

func randomLiteZone() string {
	return supportedZones[rand.Intn(len(supportedZones))]
}

func initResourcePaths(t *testing.T) (string, pubsublite.TopicPath, pubsublite.SubscriptionPath) {
	initIntegrationTest(t)

	proj := testutil.ProjID()
	zone := randomLiteZone()
	region, _ := pubsublite.ZoneToRegion(zone)
	resourceID := resourceIDs.New()

	topicPath := pubsublite.TopicPath{Project: proj, Zone: zone, TopicID: resourceID}
	subscriptionPath := pubsublite.SubscriptionPath{Project: proj, Zone: zone, SubscriptionID: resourceID}
	return region, topicPath, subscriptionPath
}

func createTopic(ctx context.Context, t *testing.T, admin *pubsublite.AdminClient, topic pubsublite.TopicPath, partitionCount int) {
	topicConfig := pubsublite.TopicConfig{
		Name:                       topic,
		PartitionCount:             partitionCount,
		PublishCapacityMiBPerSec:   4,
		SubscribeCapacityMiBPerSec: 8,
		PerPartitionBytes:          30 * gibi,
		RetentionDuration:          24 * time.Hour,
	}
	_, err := admin.CreateTopic(ctx, topicConfig)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	} else {
		t.Logf("Created topic %s", topic)
	}
}

func cleanUpTopic(ctx context.Context, t *testing.T, admin *pubsublite.AdminClient, topic pubsublite.TopicPath) {
	if err := admin.DeleteTopic(ctx, topic); err != nil {
		t.Errorf("Failed to delete topic %s: %v", topic, err)
	} else {
		t.Logf("Deleted topic %s", topic)
	}
}

func createSubscription(ctx context.Context, t *testing.T, admin *pubsublite.AdminClient, subscription pubsublite.SubscriptionPath, topic pubsublite.TopicPath) {
	subConfig := &pubsublite.SubscriptionConfig{
		Name:                subscription,
		Topic:               topic,
		DeliveryRequirement: pubsublite.DeliverImmediately,
	}
	_, err := admin.CreateSubscription(ctx, *subConfig)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	} else {
		t.Logf("Created subscription %s", subscription)
	}
}

func cleanUpSubscription(ctx context.Context, t *testing.T, admin *pubsublite.AdminClient, subscription pubsublite.SubscriptionPath) {
	if err := admin.DeleteSubscription(ctx, subscription); err != nil {
		t.Errorf("Failed to delete subscription %s: %v", subscription, err)
	} else {
		t.Logf("Deleted subscription %s", subscription)
	}
}

func publishMessages(t *testing.T, settings PublishSettings, topic pubsublite.TopicPath, msgPrefix string, messageCount int) []string {
	ctx := context.Background()
	publisher := publisherClient(ctx, t, settings, topic)
	defer publisher.Stop()

	orderingSender := test.NewOrderingSender()
	var pubResults []*pubsub.PublishResult
	var msgs []string
	for i := 0; i < messageCount; i++ {
		data := orderingSender.Next(msgPrefix)
		msgs = append(msgs, data)
		pubResults = append(pubResults, publisher.Publish(ctx, &pubsub.Message{Data: []byte(data)}))
	}
	waitForPublishResults(t, pubResults)
	return msgs
}

func waitForPublishResults(t *testing.T, pubResults []*pubsub.PublishResult) {
	cctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	for i, result := range pubResults {
		_, err := result.Get(cctx)
		if err != nil {
			t.Errorf("Publish(%d) got err: %v", i, err)
		}
	}
	t.Logf("Published %d messages", len(pubResults))
	cancel()
}

const maxPrintMsgLen = 70

func truncateMsg(msg string) string {
	if len(msg) > maxPrintMsgLen {
		return fmt.Sprintf("%s...", msg[0:maxPrintMsgLen])
	}
	return msg
}

func receiveAllMessages(t *testing.T, msgTracker *test.MsgTracker, settings ReceiveSettings, subscription pubsublite.SubscriptionPath, checkOrdering bool) {
	cctx, stopSubscriber := context.WithTimeout(context.Background(), defaultTestTimeout)
	orderingValidator := test.NewOrderingReceiver()

	messageReceiver := func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		data := string(msg.Data)
		if !msgTracker.Remove(data) {
			t.Errorf("Received unexpected message: %q", truncateMsg(data))
			return
		}
		if checkOrdering {
			if err := orderingValidator.Receive(data, msg.OrderingKey); err != nil {
				t.Errorf("Received unordered message: %q", truncateMsg(data))
			}
		}
		if msgTracker.Empty() {
			stopSubscriber() // Stop the subscriber when all messages have been received
		}
	}

	subscriber := subscriberClient(cctx, t, settings, subscription)
	if err := subscriber.Receive(cctx, messageReceiver); err != nil {
		t.Errorf("Receive() got err: %v", err)
	}
	if err := msgTracker.Status(); err != nil {
		t.Error(err)
	}
}

func receiveAndVerifyMessage(t *testing.T, want *pubsub.Message, settings ReceiveSettings, subscription pubsublite.SubscriptionPath) {
	cctx, stopSubscriber := context.WithTimeout(context.Background(), defaultTestTimeout)

	messageReceiver := func(ctx context.Context, got *pubsub.Message) {
		got.Ack()
		stopSubscriber()

		if diff := testutil.Diff(got, want, cmpopts.IgnoreUnexported(pubsub.Message{}), cmpopts.IgnoreFields(pubsub.Message{}, "ID", "PublishTime"), cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("Received message got: -, want: +\n%s", diff)
		}
		if len(got.ID) == 0 {
			t.Error("Received message missing ID")
		}
		if got.PublishTime.IsZero() {
			t.Error("Received message missing PublishTime")
		}
	}

	subscriber := subscriberClient(cctx, t, settings, subscription)
	if err := subscriber.Receive(cctx, messageReceiver); err != nil {
		t.Errorf("Receive() got err: %v", err)
	}
}

func TestIntegration_PublishSubscribeSinglePartition(t *testing.T) {
	region, topicPath, subscriptionPath := initResourcePaths(t)
	ctx := context.Background()
	const partitionCount = 1
	recvSettings := DefaultReceiveSettings
	recvSettings.Partitions = []int{0}

	admin := adminClient(ctx, t, region)
	defer admin.Close()
	createTopic(ctx, t, admin, topicPath, partitionCount)
	defer cleanUpTopic(ctx, t, admin, topicPath)
	createSubscription(ctx, t, admin, subscriptionPath, topicPath)
	defer cleanUpSubscription(ctx, t, admin, subscriptionPath)

	// The same topic and subscription resources are used for each test. This
	// implicitly verifies commits. If cursors are not successfully committed at
	// the end of each test, the next test will receive an incorrect message and
	// fail.

	// Sets all fields for a message and ensures it is correctly received.
	t.Run("AllFieldsRoundTrip", func(t *testing.T) {
		publisher := publisherClient(ctx, t, DefaultPublishSettings, topicPath)
		defer publisher.Stop()

		msg := &pubsub.Message{
			Data:        []byte("round_trip"),
			OrderingKey: "ordering_key",
			Attributes: map[string]string{
				"attr1": "value1",
				"attr2": "value2",
			},
		}
		result := publisher.Publish(ctx, msg)
		waitForPublishResults(t, []*pubsub.PublishResult{result})
		receiveAndVerifyMessage(t, msg, recvSettings, subscriptionPath)
	})

	// Verifies a custom key extractor in PublishSettings.
	t.Run("CustomKeyExtractor", func(t *testing.T) {
		pubSettings := DefaultPublishSettings
		pubSettings.KeyExtractor = func(_ *pubsub.Message) []byte {
			return []byte("replaced_key")
		}
		publisher := publisherClient(ctx, t, pubSettings, topicPath)
		defer publisher.Stop()

		msg := &pubsub.Message{
			Data:        []byte("custom_key_extractor"),
			OrderingKey: "ordering_key",
		}
		result := publisher.Publish(ctx, msg)
		waitForPublishResults(t, []*pubsub.PublishResult{result})

		want := &pubsub.Message{
			Data:        []byte("custom_key_extractor"),
			OrderingKey: "replaced_key",
		}
		receiveAndVerifyMessage(t, want, recvSettings, subscriptionPath)
	})

	// Verifies a custom message transformer in PublishSettings.
	t.Run("CustomPublishMessageTransformer", func(t *testing.T) {
		pubSettings := DefaultPublishSettings
		pubSettings.MessageTransformer = func(msg *pubsub.Message) (*pb.PubSubMessage, error) {
			return &pb.PubSubMessage{
				Data: []byte(string(msg.Data) + "_transformed"),
				Key:  []byte(msg.OrderingKey + "_transformed"),
			}, nil
		}
		publisher := publisherClient(ctx, t, pubSettings, topicPath)
		defer publisher.Stop()

		msg := &pubsub.Message{
			Data:        []byte("publish_msg_transformer"),
			OrderingKey: "ordering_key",
			Attributes: map[string]string{
				"attr1": "value1",
			},
		}
		result := publisher.Publish(ctx, msg)
		waitForPublishResults(t, []*pubsub.PublishResult{result})

		want := &pubsub.Message{
			Data:        []byte("publish_msg_transformer_transformed"),
			OrderingKey: "ordering_key_transformed",
		}
		receiveAndVerifyMessage(t, want, recvSettings, subscriptionPath)
	})

	// Verifies a custom message transformer in ReceiveSettings.
	t.Run("CustomReceiveMessageTransformer", func(t *testing.T) {
		publisher := publisherClient(ctx, t, DefaultPublishSettings, topicPath)
		defer publisher.Stop()

		msg := &pubsub.Message{
			Data:        []byte("receive_msg_transformer"),
			OrderingKey: "ordering_key",
		}
		result := publisher.Publish(ctx, msg)
		waitForPublishResults(t, []*pubsub.PublishResult{result})

		customSettings := recvSettings
		customSettings.MessageTransformer = func(wireMsg *pb.SequencedMessage, msg *pubsub.Message) error {
			// Swaps data and key.
			msg.Data = wireMsg.GetMessage().GetKey()
			msg.OrderingKey = string(wireMsg.GetMessage().GetData())
			msg.ID = "FAKE_ID"
			msg.PublishTime = time.Now()
			return nil
		}
		want := &pubsub.Message{
			Data:        []byte("ordering_key"),
			OrderingKey: "receive_msg_transformer",
		}
		receiveAndVerifyMessage(t, want, customSettings, subscriptionPath)
	})

	// Verifies that nacks are correctly handled.
	t.Run("Nack", func(t *testing.T) {
		publisher := publisherClient(ctx, t, DefaultPublishSettings, topicPath)
		defer publisher.Stop()

		msg1 := &pubsub.Message{Data: []byte("nack_msg1")}
		msg2 := &pubsub.Message{Data: []byte("nack_msg2")}
		result1 := publisher.Publish(ctx, msg1)
		result2 := publisher.Publish(ctx, msg2)
		waitForPublishResults(t, []*pubsub.PublishResult{result1, result2})

		// Case A: Default nack handler.
		cctx, _ := context.WithTimeout(context.Background(), defaultTestTimeout)
		messageReceiver := func(ctx context.Context, got *pubsub.Message) {
			if diff := testutil.Diff(got, msg1, cmpopts.IgnoreUnexported(pubsub.Message{}), cmpopts.IgnoreFields(pubsub.Message{}, "ID", "PublishTime"), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Received message got: -, want: +\n%s", diff)
			}
			got.Nack()
		}
		subscriber := subscriberClient(cctx, t, recvSettings, subscriptionPath)
		if gotErr := subscriber.Receive(cctx, messageReceiver); !test.ErrorEqual(gotErr, errNackCalled) {
			t.Errorf("Receive() got err: (%v), want err: (%v)", gotErr, errNackCalled)
		}

		// Case B: Custom nack handler.
		errNack := errors.New("message nacked")
		customSettings := recvSettings
		customSettings.NackHandler = func(msg *pubsub.Message) error {
			if string(msg.Data) == "nack_msg1" {
				return nil // Causes msg1 to be acked
			}
			if string(msg.Data) == "nack_msg2" {
				return errNack
			}
			return fmt.Errorf("Received unexpected message: %q", truncateMsg(string(msg.Data)))
		}
		subscriber = subscriberClient(cctx, t, customSettings, subscriptionPath)

		messageReceiver = func(ctx context.Context, got *pubsub.Message) {
			got.Nack()
		}
		if gotErr := subscriber.Receive(cctx, messageReceiver); !test.ErrorEqual(gotErr, errNack) {
			t.Errorf("Receive() got err: (%v), want err: (%v)", gotErr, errNack)
		}

		// Finally: receive and ack msg2.
		receiveAndVerifyMessage(t, msg2, recvSettings, subscriptionPath)
	})

	// Checks that messages are published and received in order.
	t.Run("Ordering", func(t *testing.T) {
		const messageCount = 1000
		const publishBatchSize = 20

		// Publish messages.
		pubSettings := DefaultPublishSettings
		pubSettings.CountThreshold = publishBatchSize
		pubSettings.DelayThreshold = 100 * time.Millisecond
		msgs := publishMessages(t, pubSettings, topicPath, "ordering", messageCount)

		// Receive messages, checking for ordering.
		msgTracker := test.NewMsgTracker()
		msgTracker.Add(msgs...)
		receiveAllMessages(t, msgTracker, recvSettings, subscriptionPath, true)
	})

	// Checks that subscriber flow control works.
	t.Run("SubscriberFlowControl", func(t *testing.T) {
		const messageCount = 100
		const maxOutstandingMessages = 10 // Receive small batches of messages

		// Publish messages.
		msgs := publishMessages(t, DefaultPublishSettings, topicPath, "subscriber_flow_control", messageCount)

		// Receive messages, checking for ordering.
		msgTracker := test.NewMsgTracker()
		msgTracker.Add(msgs...)
		customSettings := recvSettings
		customSettings.MaxOutstandingMessages = maxOutstandingMessages
		receiveAllMessages(t, msgTracker, customSettings, subscriptionPath, true)
	})

	// Verifies that large messages can be sent and received.
	t.Run("LargeMessages", func(t *testing.T) {
		const messageCount = 10
		const messageLen = MaxPublishMessageBytes - 50

		// Publish messages.
		publisher := publisherClient(ctx, t, DefaultPublishSettings, topicPath)
		defer publisher.Stop()

		msgTracker := test.NewMsgTracker()
		var pubResults []*pubsub.PublishResult
		for i := 0; i < messageCount; i++ {
			data := strings.Repeat(fmt.Sprintf("%d", i), messageLen)
			msgTracker.Add(data)
			pubResults = append(pubResults, publisher.Publish(ctx, &pubsub.Message{Data: []byte(data)}))
		}
		waitForPublishResults(t, pubResults)

		// Receive messages.
		receiveAllMessages(t, msgTracker, recvSettings, subscriptionPath, false)
	})
}

func TestIntegration_PublishSubscribeMultiPartition(t *testing.T) {
	region, topicPath, subscriptionPath := initResourcePaths(t)
	ctx := context.Background()
	const partitionCount = 4
	recvSettings := DefaultReceiveSettings
	recvSettings.Partitions = []int{0, 1, 2, 3}

	admin := adminClient(ctx, t, region)
	defer admin.Close()
	createTopic(ctx, t, admin, topicPath, partitionCount)
	defer cleanUpTopic(ctx, t, admin, topicPath)
	createSubscription(ctx, t, admin, subscriptionPath, topicPath)
	defer cleanUpSubscription(ctx, t, admin, subscriptionPath)

	// The same topic and subscription resources are used for each test. This
	// implicitly verifies commits. If cursors are not successfully committed at
	// the end of each test, the next test will receive an incorrect message and
	// fail.

	// Tests messages published without ordering key.
	t.Run("PublishRoutingNoKey", func(t *testing.T) {
		const messageCount = 10 * partitionCount

		// Publish messages.
		msgs := publishMessages(t, DefaultPublishSettings, topicPath, "routing_no_key", messageCount)

		// Receive messages, not checking for ordering since they do not have a key.
		// However, they would still be ordered within their partition.
		msgTracker := test.NewMsgTracker()
		msgTracker.Add(msgs...)
		receiveAllMessages(t, msgTracker, recvSettings, subscriptionPath, false)
	})

	// Tests messages published with ordering key.
	t.Run("PublishRoutingWithKey", func(t *testing.T) {
		const messageCountPerPartition = 10

		// Publish messages.
		publisher := publisherClient(ctx, t, DefaultPublishSettings, topicPath)
		defer publisher.Stop()

		orderingSender := test.NewOrderingSender()
		msgTracker := test.NewMsgTracker()
		var pubResults []*pubsub.PublishResult
		for partition := 0; partition < partitionCount; partition++ {
			for i := 0; i < messageCountPerPartition; i++ {
				data := orderingSender.Next("routing_with_key")
				msgTracker.Add(data)
				msg := &pubsub.Message{
					Data:        []byte(data),
					OrderingKey: fmt.Sprintf("p%d", partition),
				}
				pubResults = append(pubResults, publisher.Publish(ctx, msg))
			}
		}
		waitForPublishResults(t, pubResults)

		// Receive messages, checking for ordering.
		receiveAllMessages(t, msgTracker, recvSettings, subscriptionPath, true)
	})

	// Verifies usage of the partition assignment service.
	t.Run("PartitionAssignment", func(t *testing.T) {
		const messageCount = 1000
		const subscriberCount = 2 // Must be less than partitionCount

		// Publish messages.
		msgs := publishMessages(t, DefaultPublishSettings, topicPath, "partition_assignment", messageCount)

		// Start multiple subscribers that use partition assignment.
		msgTracker := test.NewMsgTracker()
		msgTracker.Add(msgs...)

		messageReceiver := func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()
			data := string(msg.Data)
			if !msgTracker.Remove(data) {
				t.Errorf("Received unexpected message: %q", truncateMsg(data))
			}
		}

		cctx, stopSubscribers := context.WithTimeout(context.Background(), defaultTestTimeout)
		for i := 0; i < subscriberCount; i++ {
			// Subscribers must be started in a goroutine as Receive() blocks.
			go func() {
				subscriber := subscriberClient(cctx, t, DefaultReceiveSettings, subscriptionPath)
				if err := subscriber.Receive(cctx, messageReceiver); err != nil {
					t.Errorf("Receive() got err: %v", err)
				}
			}()
		}

		// Wait until all messages have been received.
		msgTracker.Wait(defaultTestTimeout)
		stopSubscribers()
	})
}

func TestIntegration_SubscribeFanOut(t *testing.T) {
	// Creates multiple subscriptions for the same topic and ensures that each
	// subscription receives the published messages. This must be a standalone
	// test as the subscribers should not read from backlog.

	const subscriberCount = 3
	const messageCount = 500
	region, topicPath, baseSubscriptionPath := initResourcePaths(t)
	ctx := context.Background()
	const partitionCount = 2
	recvSettings := DefaultReceiveSettings
	recvSettings.Partitions = []int{0, 1}

	admin := adminClient(ctx, t, region)
	defer admin.Close()
	createTopic(ctx, t, admin, topicPath, partitionCount)
	defer cleanUpTopic(ctx, t, admin, topicPath)

	var subscriptionPaths []pubsublite.SubscriptionPath
	for i := 0; i < subscriberCount; i++ {
		subscription := baseSubscriptionPath
		subscription.SubscriptionID += fmt.Sprintf("%s-%d", baseSubscriptionPath.SubscriptionID, i)
		subscriptionPaths = append(subscriptionPaths, subscription)

		createSubscription(ctx, t, admin, subscription, topicPath)
		defer cleanUpSubscription(ctx, t, admin, subscription)
	}

	// Publish messages.
	msgs := publishMessages(t, DefaultPublishSettings, topicPath, "fan_out", messageCount)

	// Receive messages from multiple subscriptions.
	for _, subscription := range subscriptionPaths {
		msgTracker := test.NewMsgTracker()
		msgTracker.Add(msgs...)
		receiveAllMessages(t, msgTracker, recvSettings, subscription, false)
	}
}
