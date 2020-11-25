// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain p copy of the License at
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

	vkit "cloud.google.com/go/pubsublite/apiv1"
	gax "github.com/googleapis/gax-go/v2"
	pb "google.golang.org/genproto/googleapis/cloud/pubsublite/v1"
)

type partitionCountReceiver func(partitionCount int, err error)

// partitionCountWatcher periodically retrieves the number of partitions for a
// topic and notifies a receiver if it changes.
type partitionCountWatcher struct {
	// Immutable after creation.
	ctx         context.Context
	adminClient *vkit.AdminClient
	topicPath   string
	receiver    partitionCountReceiver
	callOption  gax.CallOption

	// Fields below must be guarded with mu.
	partitionCount int
	pollUpdate     *periodicTask

	abstractService
}

func newPartitionCountWatcher(ctx context.Context, adminClient *vkit.AdminClient,
	settings PublishSettings, topicPath string, receiver partitionCountReceiver) *partitionCountWatcher {

	p := &partitionCountWatcher{
		ctx:         ctx,
		adminClient: adminClient,
		topicPath:   topicPath,
		receiver:    receiver,
		callOption:  readOnlyRetryableCallOption(),
	}

	// Polling the topic partition count can be disabled in settings if the period
	// is <= 0.
	backgroundTask := p.UpdatePartitionCount
	if settings.ConfigPollPeriod <= 0 {
		backgroundTask = func() {}
	}
	p.pollUpdate = newPeriodicTask(settings.ConfigPollPeriod, backgroundTask)
	return p
}

func (p *partitionCountWatcher) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.unsafeUpdateStatus(serviceActive, nil) {
		p.pollUpdate.Start()
	}
}

func (p *partitionCountWatcher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unsafeInitiateShutdown(serviceTerminated, nil)
}

// UpdatePartitionCount will trigger an update and notify the receiver in this
// goroutine. Thus the receiver's mutex should not be held when called.
func (p *partitionCountWatcher) UpdatePartitionCount() {
	p.mu.Lock()
	prevPartitionCount := p.partitionCount
	p.mu.Unlock()

	newPartitionCount, err := p.doUpdatePartitionCount()
	if prevPartitionCount != newPartitionCount || err != nil {
		p.receiver(newPartitionCount, err)
	}
}

func (p *partitionCountWatcher) doUpdatePartitionCount() (int, error) {
	req := &pb.GetTopicPartitionsRequest{Name: p.topicPath}
	resp, err := p.adminClient.GetTopicPartitions(p.ctx, req, p.callOption)

	p.mu.Lock()
	defer p.mu.Unlock()

	if err != nil {
		err = fmt.Errorf("pubsublite: failed to update topic partition count: %v", err)
		p.unsafeInitiateShutdown(serviceTerminated, err)
		return 0, err
	}
	if resp.GetPartitionCount() <= 0 {
		err := fmt.Errorf("pubsublite: topic has invalid number of partitions %d", resp.GetPartitionCount())
		p.unsafeInitiateShutdown(serviceTerminated, err)
		return 0, err
	}

	p.partitionCount = int(resp.GetPartitionCount())
	return p.partitionCount, nil
}

func (p *partitionCountWatcher) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	if !p.unsafeUpdateStatus(targetStatus, err) {
		return
	}
	p.pollUpdate.Stop()
}
