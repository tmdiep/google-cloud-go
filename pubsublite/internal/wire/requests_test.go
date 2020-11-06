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

import pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"

// AdminService

func topicPartitionsReq(topicPath string) *pb.GetTopicPartitionsRequest {
	return &pb.GetTopicPartitionsRequest{Name: topicPath}
}

func topicPartitionsResp(count int) *pb.TopicPartitions {
	return &pb.TopicPartitions{PartitionCount: int64(count)}
}

// CursorService

func initCommitReq(subscription subscriptionPartition) *pb.StreamingCommitCursorRequest {
	return &pb.StreamingCommitCursorRequest{
		Request: &pb.StreamingCommitCursorRequest_Initial{
			Initial: &pb.InitialCommitCursorRequest{
				Subscription: subscription.Path,
				Partition:    int64(subscription.Partition),
			},
		},
	}
}

func initCommitResp() *pb.StreamingCommitCursorResponse {
	return &pb.StreamingCommitCursorResponse{
		Request: &pb.StreamingCommitCursorResponse_Initial{
			Initial: &pb.InitialCommitCursorResponse{},
		},
	}
}

func commitReq(offset int64) *pb.StreamingCommitCursorRequest {
	return &pb.StreamingCommitCursorRequest{
		Request: &pb.StreamingCommitCursorRequest_Commit{
			Commit: &pb.SequencedCommitCursorRequest{
				Cursor: &pb.Cursor{Offset: offset},
			},
		},
	}
}

func commitResp(numAck int) *pb.StreamingCommitCursorResponse {
	return &pb.StreamingCommitCursorResponse{
		Request: &pb.StreamingCommitCursorResponse_Commit{
			Commit: &pb.SequencedCommitCursorResponse{
				AcknowledgedCommits: int64(numAck),
			},
		},
	}
}

// PublisherService

func initPubReq(topic topicPartition) *pb.PublishRequest {
	return &pb.PublishRequest{
		RequestType: &pb.PublishRequest_InitialRequest{
			InitialRequest: &pb.InitialPublishRequest{
				Topic:     topic.Path,
				Partition: int64(topic.Partition),
			},
		},
	}
}

func initPubResp() *pb.PublishResponse {
	return &pb.PublishResponse{
		ResponseType: &pb.PublishResponse_InitialResponse{
			InitialResponse: &pb.InitialPublishResponse{},
		},
	}
}

func msgPubReq(msgs ...*pb.PubSubMessage) *pb.PublishRequest {
	return &pb.PublishRequest{
		RequestType: &pb.PublishRequest_MessagePublishRequest{
			MessagePublishRequest: &pb.MessagePublishRequest{
				Messages: msgs,
			},
		},
	}
}

func msgPubResp(cursor int64) *pb.PublishResponse {
	return &pb.PublishResponse{
		ResponseType: &pb.PublishResponse_MessageResponse{
			MessageResponse: &pb.MessagePublishResponse{
				StartCursor: &pb.Cursor{
					Offset: cursor,
				},
			},
		},
	}
}
