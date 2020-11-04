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
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsublite/internal/test"
)

const receiveStatusTimeout = 5 * time.Second

type testService struct {
	abstractService
}

func (ts *testService) Start() { ts.UpdateStatus(serviceStarting, nil) }
func (ts *testService) Stop()  { ts.UpdateStatus(serviceTerminating, nil) }

func (ts *testService) UpdateStatus(targetStatus serviceStatus, err error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.unsafeUpdateStatus(targetStatus, err)
}

type testStatusChangeReceiver struct {
	// Status change notifications are fired asynchronously, so a channel receives
	// the statuses.
	StatusC    chan serviceStatus
	LastStatus serviceStatus
	Name       string
}

func newTestStatusChangeReceiver(name string) *testStatusChangeReceiver {
	return &testStatusChangeReceiver{
		StatusC: make(chan serviceStatus, 1),
		Name:    name,
	}
}

func (sr *testStatusChangeReceiver) Handle() interface{} { return sr }

func (sr *testStatusChangeReceiver) OnStatusChange(handle serviceHandle, status serviceStatus, err error) {
	sr.StatusC <- status
}

func (sr *testStatusChangeReceiver) VerifyStatus(t *testing.T, want serviceStatus) {
	select {
	case status := <-sr.StatusC:
		if status <= sr.LastStatus {
			t.Errorf("%s: Duplicate service status: %d, last status: %d", sr.Name, status, sr.LastStatus)
		}
		if status != want {
			t.Errorf("%s: Got service status: %d, want: %d", sr.Name, status, want)
		}
		sr.LastStatus = status
	case <-time.After(receiveStatusTimeout):
		t.Errorf("%s: Did not receive status within %v", sr.Name, receiveStatusTimeout)
	}
}

func (sr *testStatusChangeReceiver) VerifyNoStatusChanges(t *testing.T) {
	select {
	case status := <-sr.StatusC:
		t.Errorf("%s: Unexpected service status: %d", sr.Name, status)
	default:
	}
}

func TestServiceUpdateStatusIsLinear(t *testing.T) {
	receiver := newTestStatusChangeReceiver("receiver")

	err1 := errors.New("error1")
	err2 := errors.New("error2")

	service := new(testService)
	service.AddStatusChangeReceiver(receiver.Handle(), receiver.OnStatusChange)
	service.UpdateStatus(serviceStarting, nil)
	receiver.VerifyStatus(t, serviceStarting)

	service.UpdateStatus(serviceActive, nil)
	service.UpdateStatus(serviceActive, nil)
	receiver.VerifyStatus(t, serviceActive)

	service.UpdateStatus(serviceTerminating, err1)
	service.UpdateStatus(serviceStarting, nil)
	service.UpdateStatus(serviceTerminating, nil)
	receiver.VerifyStatus(t, serviceTerminating)

	service.UpdateStatus(serviceTerminated, err2)
	service.UpdateStatus(serviceTerminated, nil)
	receiver.VerifyStatus(t, serviceTerminated)

	// Verify that the first error is not clobbered by the second.
	if got, want := service.Error(), err1; !test.ErrorEqual(got, want) {
		t.Errorf("service.Error(): got (%v), want (%v)", got, want)
	}
}

func TestServiceCheckServiceStatus(t *testing.T) {
	for _, tc := range []struct {
		status  serviceStatus
		wantErr error
	}{
		{
			status:  serviceUninitialized,
			wantErr: ErrServiceUninitialized,
		},
		{
			status:  serviceStarting,
			wantErr: ErrServiceStarting,
		},
		{
			status:  serviceActive,
			wantErr: nil,
		},
		{
			status:  serviceTerminating,
			wantErr: ErrServiceStopped,
		},
		{
			status:  serviceTerminated,
			wantErr: ErrServiceStopped,
		},
	} {
		t.Run(fmt.Sprintf("Status=%v", tc.status), func(t *testing.T) {
			s := new(testService)
			s.UpdateStatus(tc.status, nil)
			if gotErr := s.unsafeCheckServiceStatus(); !test.ErrorEqual(gotErr, tc.wantErr) {
				t.Errorf("service.unsafeCheckServiceStatus(): got (%v), want (%v)", gotErr, tc.wantErr)
			}
		})
	}
}

func TestServiceAddRemoveStatusChangeReceiver(t *testing.T) {
	receiver1 := newTestStatusChangeReceiver("receiver1")
	receiver2 := newTestStatusChangeReceiver("receiver2")
	receiver3 := newTestStatusChangeReceiver("receiver3")

	service := new(testService)
	service.AddStatusChangeReceiver(receiver1.Handle(), receiver1.OnStatusChange)
	service.AddStatusChangeReceiver(receiver2.Handle(), receiver2.OnStatusChange)
	service.AddStatusChangeReceiver(receiver3.Handle(), receiver3.OnStatusChange)
	service.UpdateStatus(serviceActive, nil)

	service.RemoveStatusChangeReceiver(receiver1.Handle())
	service.UpdateStatus(serviceTerminating, nil)

	service.RemoveStatusChangeReceiver(receiver2.Handle())
	service.UpdateStatus(serviceTerminated, nil)

	// receiver3 is the only one that should receive all events. The others were
	// removed.
	receiver1.VerifyStatus(t, serviceActive)
	receiver2.VerifyStatus(t, serviceActive)
	receiver2.VerifyStatus(t, serviceTerminating)
	receiver3.VerifyStatus(t, serviceActive)
	receiver3.VerifyStatus(t, serviceTerminating)
	receiver3.VerifyStatus(t, serviceTerminated)

	receiver1.VerifyNoStatusChanges(t)
	receiver2.VerifyNoStatusChanges(t)
	receiver3.VerifyNoStatusChanges(t)
}

type testCompositeService struct {
	compositeService
}

func (ts *testCompositeService) AddServices(services ...service) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.unsafeAddServices(services...)
}

func (ts *testCompositeService) RemoveService(service service) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.unsafeRemoveService(service)
}

func TestCompositeServiceNormalStop(t *testing.T) {
	child1 := new(testService)
	child2 := new(testService)
	child3 := new(testService)
	parent := new(testCompositeService)
	parent.init()
	parent.AddServices(child1, child2)

	parentReceiver := newTestStatusChangeReceiver("parentReceiver")
	parent.AddStatusChangeReceiver(parentReceiver.Handle(), parentReceiver.OnStatusChange)
	child1Receiver := newTestStatusChangeReceiver("child1Receiver")
	child1.AddStatusChangeReceiver(child1Receiver.Handle(), child1Receiver.OnStatusChange)
	child2Receiver := newTestStatusChangeReceiver("child2Receiver")
	child2.AddStatusChangeReceiver(child2Receiver.Handle(), child2Receiver.OnStatusChange)
	child3Receiver := newTestStatusChangeReceiver("child3Receiver")
	child3.AddStatusChangeReceiver(child3Receiver.Handle(), child3Receiver.OnStatusChange)

	t.Run("Starting", func(t *testing.T) {
		parent.Start()
		child1Receiver.VerifyStatus(t, serviceStarting)
		child2Receiver.VerifyStatus(t, serviceStarting)
		parentReceiver.VerifyStatus(t, serviceStarting)

		// child3 is added after Start() and should be automatically started.
		parent.AddServices(child3)
		child3Receiver.VerifyStatus(t, serviceStarting)
	})

	t.Run("Active", func(t *testing.T) {
		// parent service is active once all children are active.
		child1.UpdateStatus(serviceActive, nil)
		child2.UpdateStatus(serviceActive, nil)
		parentReceiver.VerifyNoStatusChanges(t)
		child3.UpdateStatus(serviceActive, nil)

		child1Receiver.VerifyStatus(t, serviceActive)
		child2Receiver.VerifyStatus(t, serviceActive)
		child3Receiver.VerifyStatus(t, serviceActive)
		parentReceiver.VerifyStatus(t, serviceActive)
		if err := parent.WaitStarted(); err != nil {
			t.Errorf("compositeService.WaitStarted() got err: %v", err)
		}
	})

	t.Run("Stopping", func(t *testing.T) {
		parent.Stop()
		child1Receiver.VerifyStatus(t, serviceTerminating)
		child2Receiver.VerifyStatus(t, serviceTerminating)
		child3Receiver.VerifyStatus(t, serviceTerminating)
		parentReceiver.VerifyStatus(t, serviceTerminating)

		// parent service is terminated once all children have terminated.
		child1.UpdateStatus(serviceTerminated, nil)
		child2.UpdateStatus(serviceTerminated, nil)
		parentReceiver.VerifyNoStatusChanges(t)
		child3.UpdateStatus(serviceTerminated, nil)

		child1Receiver.VerifyStatus(t, serviceTerminated)
		child2Receiver.VerifyStatus(t, serviceTerminated)
		child3Receiver.VerifyStatus(t, serviceTerminated)
		parentReceiver.VerifyStatus(t, serviceTerminated)
		if err := parent.WaitStopped(); err != nil {
			t.Errorf("compositeService.WaitStopped() got err: %v", err)
		}
	})
}

func TestCompositeServiceErrorDuringStartup(t *testing.T) {
	child1 := new(testService)
	child2 := new(testService)
	parent := new(testCompositeService)
	parent.init()
	parent.AddServices(child1, child2)

	parentReceiver := newTestStatusChangeReceiver("parentReceiver")
	parent.AddStatusChangeReceiver(parentReceiver.Handle(), parentReceiver.OnStatusChange)
	child1Receiver := newTestStatusChangeReceiver("child1Receiver")
	child1.AddStatusChangeReceiver(child1Receiver.Handle(), child1Receiver.OnStatusChange)
	child2Receiver := newTestStatusChangeReceiver("child2Receiver")
	child2.AddStatusChangeReceiver(child2Receiver.Handle(), child2Receiver.OnStatusChange)

	t.Run("Starting", func(t *testing.T) {
		parent.Start()
		parentReceiver.VerifyStatus(t, serviceStarting)
		child1Receiver.VerifyStatus(t, serviceStarting)
		child2Receiver.VerifyStatus(t, serviceStarting)
	})

	t.Run("Terminating", func(t *testing.T) {
		// child1 now errors.
		wantErr := errors.New("err during startup")
		child1.UpdateStatus(serviceTerminated, wantErr)
		child1Receiver.VerifyStatus(t, serviceTerminated)

		// This causes parent and child2 to start terminating.
		parentReceiver.VerifyStatus(t, serviceTerminating)
		child2Receiver.VerifyStatus(t, serviceTerminating)

		// parent has terminated once child2 has terminated.
		child2.UpdateStatus(serviceTerminated, nil)
		child2Receiver.VerifyStatus(t, serviceTerminated)
		parentReceiver.VerifyStatus(t, serviceTerminated)
		if gotErr := parent.WaitStarted(); !test.ErrorEqual(gotErr, wantErr) {
			t.Errorf("compositeService.WaitStarted() got err: (%v), want err: (%v)", gotErr, wantErr)
		}
	})
}

func TestCompositeServiceErrorWhileActive(t *testing.T) {
	child1 := new(testService)
	child2 := new(testService)
	parent := new(testCompositeService)
	parent.init()
	parent.AddServices(child1, child2)

	parentReceiver := newTestStatusChangeReceiver("parentReceiver")
	parent.AddStatusChangeReceiver(parentReceiver.Handle(), parentReceiver.OnStatusChange)
	child1Receiver := newTestStatusChangeReceiver("child1Receiver")
	child1.AddStatusChangeReceiver(child1Receiver.Handle(), child1Receiver.OnStatusChange)
	child2Receiver := newTestStatusChangeReceiver("child2Receiver")
	child2.AddStatusChangeReceiver(child2Receiver.Handle(), child2Receiver.OnStatusChange)

	t.Run("Starting", func(t *testing.T) {
		parent.Start()
		child1Receiver.VerifyStatus(t, serviceStarting)
		child2Receiver.VerifyStatus(t, serviceStarting)
		parentReceiver.VerifyStatus(t, serviceStarting)
	})

	t.Run("Active", func(t *testing.T) {
		child1.UpdateStatus(serviceActive, nil)
		child2.UpdateStatus(serviceActive, nil)
		child1Receiver.VerifyStatus(t, serviceActive)
		child2Receiver.VerifyStatus(t, serviceActive)
		parentReceiver.VerifyStatus(t, serviceActive)
		if err := parent.WaitStarted(); err != nil {
			t.Errorf("compositeService.WaitStarted() got err: %v", err)
		}
	})

	t.Run("Terminating", func(t *testing.T) {
		// child2 now errors.
		wantErr := errors.New("err while active")
		child2.UpdateStatus(serviceTerminating, wantErr)
		child2Receiver.VerifyStatus(t, serviceTerminating)

		// This causes parent and child1 to start terminating.
		child1Receiver.VerifyStatus(t, serviceTerminating)
		parentReceiver.VerifyStatus(t, serviceTerminating)

		// parent has terminated once both children have terminated.
		child1.UpdateStatus(serviceTerminated, nil)
		child2.UpdateStatus(serviceTerminated, nil)
		child1Receiver.VerifyStatus(t, serviceTerminated)
		child2Receiver.VerifyStatus(t, serviceTerminated)
		parentReceiver.VerifyStatus(t, serviceTerminated)
		if gotErr := parent.WaitStopped(); !test.ErrorEqual(gotErr, wantErr) {
			t.Errorf("compositeService.WaitStopped() got err: (%v), want err: (%v)", gotErr, wantErr)
		}
	})
}

func TestCompositeServiceRemoveService(t *testing.T) {
	child1 := new(testService)
	child2 := new(testService)
	parent := new(testCompositeService)
	parent.init()
	parent.AddServices(child1, child2)

	parentReceiver := newTestStatusChangeReceiver("parentReceiver")
	parent.AddStatusChangeReceiver(parentReceiver.Handle(), parentReceiver.OnStatusChange)
	child1Receiver := newTestStatusChangeReceiver("child1Receiver")
	child1.AddStatusChangeReceiver(child1Receiver.Handle(), child1Receiver.OnStatusChange)
	child2Receiver := newTestStatusChangeReceiver("child2Receiver")
	child2.AddStatusChangeReceiver(child2Receiver.Handle(), child2Receiver.OnStatusChange)

	t.Run("Starting", func(t *testing.T) {
		parent.Start()
		child1Receiver.VerifyStatus(t, serviceStarting)
		child2Receiver.VerifyStatus(t, serviceStarting)
		parentReceiver.VerifyStatus(t, serviceStarting)
	})

	t.Run("Active", func(t *testing.T) {
		child1.UpdateStatus(serviceActive, nil)
		child2.UpdateStatus(serviceActive, nil)
		child1Receiver.VerifyStatus(t, serviceActive)
		child2Receiver.VerifyStatus(t, serviceActive)
		parentReceiver.VerifyStatus(t, serviceActive)
	})

	t.Run("Remove service", func(t *testing.T) {
		// Removing child1 should stop it, but leave everything else active.
		parent.RemoveService(child1)
		parent.mu.Lock()
		if got, want := len(parent.dependencies), 1; got != want {
			t.Errorf("compositeService.dependencies: got len %d, want %d", got, want)
		}
		if got, want := len(parent.removed), 1; got != want {
			t.Errorf("compositeService.removed: got len %d, want %d", got, want)
		}
		parent.mu.Unlock()
		child1Receiver.VerifyStatus(t, serviceTerminating)
		child2Receiver.VerifyNoStatusChanges(t)
		parentReceiver.VerifyNoStatusChanges(t)

		// After child1 has terminated, it should be removed.
		child1.UpdateStatus(serviceTerminated, nil)
		child1Receiver.VerifyStatus(t, serviceTerminated)
		child2Receiver.VerifyNoStatusChanges(t)
		parentReceiver.VerifyNoStatusChanges(t)
	})

	t.Run("Terminating", func(t *testing.T) {
		// Now stop the composite service.
		parent.Stop()
		child2Receiver.VerifyStatus(t, serviceTerminating)
		parentReceiver.VerifyStatus(t, serviceTerminating)

		child2.UpdateStatus(serviceTerminated, nil)
		child2Receiver.VerifyStatus(t, serviceTerminated)
		parentReceiver.VerifyStatus(t, serviceTerminated)
		if err := parent.WaitStopped(); err != nil {
			t.Errorf("compositeService.WaitStopped() got err: %v", err)
		}

		parent.mu.Lock()
		if got, want := len(parent.dependencies), 1; got != want {
			t.Errorf("compositeService.dependencies: got len %d, want %d", got, want)
		}
		if got, want := len(parent.removed), 0; got != want {
			t.Errorf("compositeService.removed: got len %d, want %d", got, want)
		}
		parent.mu.Unlock()
	})
}
