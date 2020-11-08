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

var (
  errOutstandingAssignment = errors.New("pubsublite: received partition assignment from the server while there was an assignment outstanding")
)

type partitionAssignment struct {
  // A set of partition numbers.
  partitions map[int]bool
}

func newPartitionAssignment(assignmentpb *pb.PartitionAssignment) *partitionAssignment {
  pa := &partitionAssignment{
    partitions: make(map[int]bool),
  }
  for _, p := range assignmentpb.Partitions {
    pa.partitions[int(p)] = true
  }
  return pa
}

func (pa *partitionAssignment) Partitions() (partitions []int) {
  for p, inSet := range pa.partitions {
    if inSet {
      partitions = append(partitions, p)
    }
  }
  return
}

func (pa *partitionAssignment) Contains(partition int) bool {
  return pa.partitions[partition]
}

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

  // An assignment was received from the server and needs to be acked by the
  // receiver.
  pendingAssignment *pb.PartitionAssignment

  abstractService
}

func newAssigner(ctx context.Context, partitionClient *vkit.PartitionAssignmentClient, settings ReceiveSettings, subscriptionPath string, receiver partitionAssignmentReceiver) (*assigner, error) {
  clientID, err := uuid.NewRandom()
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

func (a *assigner) initialRequest() interface{} {
  return a.initialReq
}

func (a *assigner) validateInitialResponse(response interface{}) error {
  a.mu.Lock()
  defer a.mu.Unlock()

  // The initial stream response returns an assignment. Store and notify
  // receiver once the stream transitions to the streamConnected state.
  a.pendingAssignment, _ = response.(*pb.PartitionAssignment)
  return nil
}

func (a *assigner) onStreamStatusChange(status streamStatus) {
  a.mu.Lock()
  defer a.mu.Unlock()

  switch status {
  case streamConnected:
    a.unsafeUpdateStatus(serviceActive, nil)
    if err := a.unsafeHandlePendingAssignment(); err != nil {
      a.unsafeInitiateShutdown(serviceTerminated, err)
    }

  case streamTerminated:
    a.unsafeInitiateShutdown(serviceTerminated, a.stream.Error())
  }
}

func (a *assigner) onResponse(response interface{}) {
  a.mu.Lock()
  defer a.mu.Unlock()

  processResponse := func() error {
    if a.pendingAssignment != nil {
      return errOutstandingAssignment
    }
    a.pendingAssignment, _ = response.(*pb.PartitionAssignment)
    return a.unsafeHandlePendingAssignment()
  }
  if err := processResponse(); err != nil {
    a.unsafeInitiateShutdown(serviceTerminated, err)
  }
}

func (a *assigner) unsafeHandlePendingAssignment() error {
  if err := a.receiveAssignment(newPartitionAssignment(a.pendingAssignment)); err != nil {
    return err
  }

  log.Printf("pubsublite: subscriber partition assignments: %v", a.pendingAssignment)
  a.stream.Send(&pb.PartitionAssignmentRequest{
    Request: &pb.PartitionAssignmentRequest_Ack{
      Ack: &pb.PartitionAssignmentAck{},
    },
  })
  a.pendingAssignment = nil
  return nil
}

func (a *assigner) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
  if !a.unsafeUpdateStatus(targetStatus, err) {
    return
  }

  // No data to send. Immediately terminate the stream.
  a.stream.Stop()
}
