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
	Start()
	Stop()
	// Allows compositeService.onServiceStatusChange to work for multiple layers
	// of embedding.
	handle() *abstractService
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
type serviceStatusChangeFunc func(*abstractService, serviceStatus, error)

// abstractService can be embedded into other structs to provide common
// functionality for managing service status.
type abstractService struct {
	mu sync.Mutex

	onStatusChange serviceStatusChangeFunc
	status         serviceStatus
	// The error that cause the service to terminate.
	err error
}

func (as *abstractService) Error() error {
	as.mu.Lock()
	defer as.mu.Unlock()
	return as.err
}

// Virtual constructor.
func (as *abstractService) init(onStatusChange serviceStatusChangeFunc) {
	as.onStatusChange = onStatusChange
}

func (as *abstractService) handle() *abstractService {
	return as
}

func (as *abstractService) unsafeCheckServiceStatus() error {
	switch {
	case as.status == serviceUninitialized:
		return ErrServiceUninitialized
	case as.status > serviceActive:
		return ErrServiceStopped
	default:
		return nil
	}
}

// unsafeUpdateStatus assumes the service is already holding a mutex when
// called.
func (as *abstractService) unsafeUpdateStatus(targetStatus serviceStatus, err error) bool {
	if as.status >= targetStatus {
		// Already at the same or later stage of the service lifecycle.
		return false
	}

	as.status = targetStatus
	if as.err == nil {
		// Prevent clobbering an original error.
		as.err = err
	}
	if as.onStatusChange != nil {
		// Notify in a goroutine to prevent deadlocks if the parent is holding a
		// locked mutex.
		go as.onStatusChange(as.handle(), targetStatus, as.err)
	}
	return true
}

type serviceHolder struct {
	service    service
	lastStatus serviceStatus
}

// compositeService can be embedded into other structs to manage child services.
type compositeService struct {
	// Used to block until all dependencies have started or terminated.
	waitStarted    chan struct{}
	waitTerminated chan struct{}

	dependencies []*serviceHolder

	abstractService
}

// Start up dependencies.
func (cs *compositeService) Start() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, s := range cs.dependencies {
		s.service.Start()
	}
}

// WaitStarted waits for all dependencies to start.
func (cs *compositeService) WaitStarted() error {
	<-cs.waitStarted
	return cs.Error()
}

// Stop all dependencies.
func (cs *compositeService) Stop() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.unsafeInitiateShutdown(serviceTerminating, nil)
}

// WaitStopped waits for all dependencies to stop.
func (cs *compositeService) WaitStopped() error {
	<-cs.waitTerminated
	return cs.Error()
}

func (cs *compositeService) init(onStatusChange serviceStatusChangeFunc) {
	cs.waitStarted = make(chan struct{})
	cs.waitTerminated = make(chan struct{})
	cs.abstractService.init(onStatusChange)
}

func (cs *compositeService) unsafeAddService(service service) {
	cs.dependencies = append(cs.dependencies, &serviceHolder{service: service})
}

func (cs *compositeService) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	for _, s := range cs.dependencies {
		if s.lastStatus < serviceTerminating {
			s.service.Stop()
		}
	}
	cs.unsafeUpdateStatus(targetStatus, err)
}

func (cs *compositeService) unsafeUpdateStatus(targetStatus serviceStatus, err error) bool {
	previousStatus := cs.status
	if cs.abstractService.unsafeUpdateStatus(targetStatus, err) {
		if previousStatus == serviceUninitialized {
			close(cs.waitStarted)
		}
		if targetStatus == serviceTerminated {
			close(cs.waitTerminated)
		}
		return true
	}
	return false
}

func (cs *compositeService) onServiceStatusChange(handle *abstractService, status serviceStatus, err error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	numStarted := 0
	numTerminated := 0

	for _, s := range cs.dependencies {
		if s.service.handle() == handle {
			s.lastStatus = status
		} else if status >= serviceTerminating && s.lastStatus < serviceTerminating {
			// If a single service terminates, stop them all, but allow the others to
			// flush pending data.
			s.service.Stop()
		}
		if s.lastStatus >= serviceActive {
			numStarted++
		}
		if s.lastStatus == serviceTerminated {
			numTerminated++
		}
	}

	switch {
	case numTerminated == len(cs.dependencies):
		cs.unsafeUpdateStatus(serviceTerminated, err)
	case status >= serviceTerminating:
		cs.unsafeUpdateStatus(serviceTerminating, err)
	case numStarted == len(cs.dependencies):
		cs.unsafeUpdateStatus(serviceActive, err)
	}
}
