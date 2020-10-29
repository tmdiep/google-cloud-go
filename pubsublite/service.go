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
	"sync"
)

// service is the interface that must be implemented by services (essentially
// gRPC client stream wrappers) that can be dependencies of a compositeService.
type service interface {
	// Start the service.
	Start()
	// Stop the service gracefully, flushing any pending data.
	Stop()
}

// serviceStatus specifies the current status of the service. The order of the
// values reflects the lifecycle of services. Note that some statuses may be
// skipped.
type serviceStatus int

const (
	// Service has not been started.
	serviceUninitialized serviceStatus = 0
	// Service is active and accepting new data. Note that the underlying stream
	// may be reconnecting due to retryable errors.
	serviceActive serviceStatus = 1
	// Service is gracefully shutting down by flushing all pending data. No new
	// data is accepted.
	serviceTerminating serviceStatus = 2
	// Service has terminated. No new data is accepted.
	serviceTerminated serviceStatus = 3
)

// serviceStatusChangeFunc notifies the parent of service status changes.
// `serviceTerminating` and `serviceTerminated` have an associated error. This
// error may be nil if the user called Stop().
type serviceStatusChangeFunc func(service, serviceStatus, error)

// abstractService can be embedded into other structs to provide common
// functionality for managing service status.
type abstractService struct {
	onStatusChange serviceStatusChangeFunc
	status         serviceStatus
	finalErr       error
}

// unsafeUpdateStatus assumes the service is already holding a mutex when
// called. `s` must be a pointer to the service embedding the abstractService
// for the services to be equal in compositeService.onServiceStatusChange.
func (as *abstractService) unsafeUpdateStatus(s service, targetStatus serviceStatus, err error) bool {
	if as.status >= targetStatus {
		// Already at the same or later stage of the service lifecycle.
		return false
	}

	as.status = targetStatus
	if err != nil {
		// Prevent nil clobbering an original error.
		as.finalErr = err
	}
	if as.onStatusChange != nil {
		// Notify in a goroutine to prevent deadlocks if the parent is holding a
		// locked mutex.
		go as.onStatusChange(s, targetStatus, as.finalErr)
	}
	return true
}

type serviceHolder struct {
	service    service
	lastStatus serviceStatus
}

// compositeService can be embedded into other structs to manage child services.
type compositeService struct {
	mu sync.Mutex

	// Used to block until all dependencies have started or terminated.
	waitStarted    sync.WaitGroup
	waitTerminated sync.WaitGroup

	dependencies []*serviceHolder
	finalErr     error
}

// Start up dependencies.
func (cs *compositeService) Start() {
	for _, s := range cs.services() {
		s.service.Start()
	}
}

// WaitStarted waits for all dependencies to start.
func (cs *compositeService) WaitStarted() error {
	cs.waitStarted.Wait()
	return cs.Error()
}

// Stop all dependencies.
func (cs *compositeService) Stop() {
	for _, s := range cs.services() {
		s.service.Stop()
	}
}

// WaitStopped waits for all dependencies to stop.
func (cs *compositeService) WaitStopped() error {
	cs.waitTerminated.Wait()
	return cs.Error()
}

// Error is the first error encountered, which caused all services to stop.
func (cs *compositeService) Error() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.finalErr
}

// unsafeAddService assumes the composite service is already holding the mutex
// when called.
func (cs *compositeService) unsafeAddService(service service) {
	cs.waitStarted.Add(1)
	cs.waitTerminated.Add(1)
	cs.dependencies = append(cs.dependencies, &serviceHolder{service: service})
}

func (cs *compositeService) services() []*serviceHolder {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.dependencies
}

func (cs *compositeService) onServiceStatusChange(service service, status serviceStatus, err error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if status > serviceActive && cs.finalErr == nil {
		cs.finalErr = err
	}

	for _, s := range cs.dependencies {
		if s.service == service {
			if (status == serviceActive || status == serviceTerminated) && s.lastStatus < serviceActive {
				cs.waitStarted.Done()
			}
			if status == serviceTerminated && s.lastStatus < serviceTerminated {
				cs.waitTerminated.Done()
			}
			s.lastStatus = status
		} else if status >= serviceTerminating && s.lastStatus < serviceTerminating {
			// If a single service terminates, stop them all, but allow the others to
			// flush pending data.
			s.service.Stop()
		}
	}
}
