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
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

// AttributeValues is a slice of strings.
type AttributeValues []string

// Message represents a Pub/Sub message.
type Message struct {
	// ID identifies this message. This ID is assigned by the server and is
	// populated for Messages obtained from a subscription.
	//
	// This field is read-only.
	ID string

	// Data is the actual data in the message.
	Data []byte

	// Attributes can be used to label the message. A key may have multiple
	// values.
	Attributes map[string]AttributeValues

	// OrderingKey identifies related messages for which publish order should
	// be respected. Messages with the same ordering key are published to the
	// same topic partition and subscribers will receive the messages in order.
	// If the ordering key is empty, the message will be sent to an arbitrary
	// partition.
	OrderingKey string

	// EventTime is an optional, user-specified event time for this message.
	EventTime time.Time

	// PublishTime is the time at which the message was published. This is
	// populated by the server for Messages obtained from a subscription.
	//
	// This field is read-only.
	PublishTime time.Time
}

// KeyExtractorFunc is a function that extracts an ordering key from a Message.
type KeyExtractorFunc func(*Message) []byte

// extractOrderingKey extracts the ordering key from the message for routing
// during publishing. It is the default KeyExtractorFunc implementation.
func extractOrderingKey(msg *Message) []byte {
	if len(msg.OrderingKey) == 0 {
		return nil
	}
	return []byte(msg.OrderingKey)
}

// PublishMessageTransformerFunc transforms a Message to a PubSubMessage API
// proto.
type PublishMessageTransformerFunc func(*Message) (*pb.PubSubMessage, error)

// transformPublishedMessage is the default PublishMessageTransformerFunc
// implementation.
func transformPublishedMessage(m *Message, extractKey KeyExtractorFunc) (*pb.PubSubMessage, error) {
	msgpb := &pb.PubSubMessage{
		Data: m.Data,
		Key:  extractKey(m),
	}

	if len(m.Attributes) > 0 {
		msgpb.Attributes = make(map[string]*pb.AttributeValues)
		for key, values := range m.Attributes {
			var byteValues [][]byte
			for _, v := range values {
				byteValues = append(byteValues, []byte(v))
			}
			msgpb.Attributes[key] = &pb.AttributeValues{Values: byteValues}
		}
	}

	if !m.EventTime.IsZero() {
		ts, err := ptypes.TimestampProto(m.EventTime)
		if err != nil {
			return nil, fmt.Errorf("pubsublite: error converting message timestamp: %v", err)
		}
		msgpb.EventTime = ts
	}
	return msgpb, nil
}
