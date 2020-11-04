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
	"cloud.google.com/go/pubsub"

	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

// KeyExtractorFunc is a function that extracts an ordering key from a Message.
type KeyExtractorFunc func(*pubsub.Message) []byte

// extractOrderingKey extracts the ordering key from the message for routing
// during publishing. It is the default KeyExtractorFunc implementation.
func extractOrderingKey(msg *pubsub.Message) []byte {
	if len(msg.OrderingKey) == 0 {
		return nil
	}
	return []byte(msg.OrderingKey)
}

// PublishMessageTransformerFunc transforms a Message to a PubSubMessage API
// proto.
type PublishMessageTransformerFunc func(*pubsub.Message) (*pb.PubSubMessage, error)

// transformPublishedMessage is the default PublishMessageTransformerFunc
// implementation.
func transformPublishedMessage(m *pubsub.Message, extractKey KeyExtractorFunc) (*pb.PubSubMessage, error) {
	msgpb := &pb.PubSubMessage{
		Data: m.Data,
		Key:  extractKey(m),
	}

	if len(m.Attributes) > 0 {
		msgpb.Attributes = make(map[string]*pb.AttributeValues)
		for key, value := range m.Attributes {
			msgpb.Attributes[key] = &pb.AttributeValues{Values: [][]byte{[]byte(value)}}
		}
	}
	return msgpb, nil
}
