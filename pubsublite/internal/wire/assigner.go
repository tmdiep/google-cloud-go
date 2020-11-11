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
  "errors"
  "fmt"
  "log"
  "reflect"

  "github.com/google/uuid"
  "google.golang.org/grpc"

  vkit "cloud.google.com/go/pubsublite/apiv1"
  pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

type partitionAssignment struct {
  // A set of partition numbers.
  partitions map[int]struct{}
  void       struct{}
}

func newPartitionAssignment(assignmentpb *pb.PartitionAssignment) *partitionAssignment {
  pa := &partitionAssignment{
    partitions: make(map[int]struct{}),
  }
  for _, p := range assignmentpb.Partitions {
    pa.partitions[int(p)] = pa.void
  }
  return pa
}

func (pa *partitionAssignment) Partitions() (partitions []int) {
  for p := range pa.partitions {
    partitions = append(partitions, p)
  }
  return
}

func (pa *partitionAssignment) Contains(partition int) bool {
  _, exists := pa.partitions[partition]
  return exists
}

// A function that generates a 16-byte UUID.
type generateUUIDFunc func() (uuid.UUID, error)

// partitionAssignmentReceiver must enact the received partition assignment from
// the server, or otherwise return an error, which will break the stream. The
// receiver must not call the assigner, as this would result in a deadlock.
type partitionAssignmentReceiver func(*partitionAssignment) error

type assigner struct {
  // Immutable after creation.
  partitionClient   *vkit.PartitionAssignmentClient
  initialReq        *pb.PartitionAssignmentRequest
  receiveAssignment partitionAssignmentReceiver

  // Fields below must be guarded with mutex.
  stream *retryableStream

  abstractService
}

func newAssigner(ctx context.Context, partitionClient *vkit.PartitionAssignmentClient, genUUID generateUUIDFunc, settings ReceiveSettings, subscriptionPath string, receiver partitionAssignmentReceiver) (*assigner, error) {
  clientID, err := genUUID()
  if err != nil {
    return nil, fmt.Errorf("pubsublite: failed to generate client UUID: %v", err)
  }
  log.Printf("pubsublite: subscription %s using UUID %v for assignment", subscriptionPath, clientID)

  a := &assigner{
    partitionClient: partitionClient,
    initialReq: &pb.PartitionAssignmentRequest{
      Request: &pb.PartitionAssignmentRequest_Initial{
        Initial: &pb.InitialPartitionAssignmentRequest{
          Subscription: subscriptionPath,
          ClientId:     clientID[:],
        },
      },
    },
    receiveAssignment: receiver,
  }
  a.stream = newRetryableStream(ctx, a, settings.Timeout, reflect.TypeOf(pb.PartitionAssignment{}))
  return a, nil
}

func (a *assigner) Start() {
  a.mu.Lock()
  defer a.mu.Unlock()

  if a.unsafeUpdateStatus(serviceStarting, nil) {
    a.stream.Start()
  }
}

func (a *assigner) Stop() {
  a.mu.Lock()
  defer a.mu.Unlock()
  a.unsafeInitiateShutdown(serviceTerminating, nil)
}

func (a *assigner) newStream(ctx context.Context) (grpc.ClientStream, error) {
  return a.partitionClient.AssignPartitions(ctx)
}

func (a *assigner) initialRequest() (interface{}, bool) {
  // No initial response expected.
  return a.initialReq, false
}

func (a *assigner) validateInitialResponse(_ interface{}) error {
  // Should not be called.
  return errors.New("pubsublite: unexpected initial response")
}

func (a *assigner) onStreamStatusChange(status streamStatus) {
  a.mu.Lock()
  defer a.mu.Unlock()

  switch status {
  case streamConnected:
    a.unsafeUpdateStatus(serviceActive, nil)

  case streamTerminated:
    a.unsafeInitiateShutdown(serviceTerminated, a.stream.Error())
  }
}

func (a *assigner) onResponse(response interface{}) {
  a.mu.Lock()
  defer a.mu.Unlock()

  assignment, _ := response.(*pb.PartitionAssignment)
  if err := a.handleAssignment(assignment); err != nil {
    a.unsafeInitiateShutdown(serviceTerminated, err)
  }
}

func (a *assigner) handleAssignment(assignment *pb.PartitionAssignment) error {
  if err := a.receiveAssignment(newPartitionAssignment(assignment)); err != nil {
    return err
  }

  log.Printf("pubsublite: subscriber partition assignments: %v", assignment.Partitions)
  a.stream.Send(&pb.PartitionAssignmentRequest{
    Request: &pb.PartitionAssignmentRequest_Ack{
      Ack: &pb.PartitionAssignmentAck{},
    },
  })
  return nil
}

func (a *assigner) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
  if !a.unsafeUpdateStatus(targetStatus, err) {
    return
  }
  // No data to send. Immediately terminate the stream.
  a.stream.Stop()
}
