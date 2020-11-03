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

type mockService struct {
	abstractService
}

func (ms *mockService) Start() { ms.UpdateStatus(serviceStarting, nil) }
func (ms *mockService) Stop()  { ms.UpdateStatus(serviceTerminating, nil) }

func (ms *mockService) UpdateStatus(targetStatus serviceStatus, err error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.unsafeUpdateStatus(targetStatus, err)
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

func (sr *testStatusChangeReceiver) ValidateStatus(t *testing.T, want serviceStatus) {
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

func (sr *testStatusChangeReceiver) EnsureNoMoreStatusChanges(t *testing.T) {
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

	service := &mockService{}
	service.AddStatusChangeReceiver(receiver.Handle(), receiver.OnStatusChange)
	service.UpdateStatus(serviceStarting, nil)
	receiver.ValidateStatus(t, serviceStarting)

	service.UpdateStatus(serviceActive, nil)
	service.UpdateStatus(serviceActive, nil)
	receiver.ValidateStatus(t, serviceActive)

	service.UpdateStatus(serviceTerminating, err1)
	service.UpdateStatus(serviceStarting, nil)
	service.UpdateStatus(serviceTerminating, nil)
	receiver.ValidateStatus(t, serviceTerminating)

	service.UpdateStatus(serviceTerminated, err2)
	service.UpdateStatus(serviceTerminated, nil)
	receiver.ValidateStatus(t, serviceTerminated)

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
			s := &mockService{}
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

	service := &mockService{}
	service.AddStatusChangeReceiver(receiver1.Handle(), receiver1.OnStatusChange)
	service.AddStatusChangeReceiver(receiver2.Handle(), receiver2.OnStatusChange)
	service.AddStatusChangeReceiver(receiver3.Handle(), receiver3.OnStatusChange)
	service.UpdateStatus(serviceActive, nil)

	service.RemoveStatusChangeReceiver(receiver1.Handle())
	service.UpdateStatus(serviceTerminating, nil)

	service.RemoveStatusChangeReceiver(receiver2.Handle())
	service.UpdateStatus(serviceTerminated, nil)

	receiver1.ValidateStatus(t, serviceActive)
	receiver2.ValidateStatus(t, serviceActive)
	receiver2.ValidateStatus(t, serviceTerminating)
	receiver3.ValidateStatus(t, serviceActive)
	receiver3.ValidateStatus(t, serviceTerminating)
	receiver3.ValidateStatus(t, serviceTerminated)

	receiver1.EnsureNoMoreStatusChanges(t)
	receiver2.EnsureNoMoreStatusChanges(t)
	receiver3.EnsureNoMoreStatusChanges(t)
}

func TestServiceAddRemoveStatusChangeReceiver(t *testing.T) {

}
