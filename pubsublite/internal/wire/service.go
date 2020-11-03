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
	"fmt"
	"sync"
	"time"
)

// serviceStatus specifies the current status of the service. The order of the
// values reflects the lifecycle of services. Note that some statuses may be
// skipped.
type serviceStatus int

const (
	// Service has not been started.
	serviceUninitialized serviceStatus = 0
	// Service is starting up.
	serviceStarting serviceStatus = 1
	// Service is active and accepting new data. Note that the underlying stream
	// may be reconnecting due to retryable errors.
	serviceActive serviceStatus = 2
	// Service is gracefully shutting down by flushing all pending data. No new
	// data is accepted.
	serviceTerminating serviceStatus = 3
	// Service has terminated. No new data is accepted.
	serviceTerminated serviceStatus = 4
)

// serviceHandle is used to compare pointers to service instances.
type serviceHandle interface{}

// service is the interface that must be implemented by services (essentially
// gRPC client stream wrappers, e.g. subscriber, publisher) that can be
// dependencies of a compositeService.
type service interface {
	Start()
	Stop()

	// Methods below are implemented by abstractService.
	AddStatusChangeReceiver(serviceHandle, serviceStatusChangeFunc)
	RemoveStatusChangeReceiver(serviceHandle)
	Handle() serviceHandle
}

// serviceStatusChangeFunc notifies the parent of service status changes.
// `serviceTerminating` and `serviceTerminated` have an associated error. This
// error may be nil if the user called Stop().
type serviceStatusChangeFunc func(serviceHandle, serviceStatus, error)

type statusChangeReceiver struct {
	handle         serviceHandle // For removing the receiver.
	onStatusChange serviceStatusChangeFunc
}

// abstractService can be embedded into other structs to provide common
// functionality for managing service status and status change receivers.
type abstractService struct {
	mu sync.Mutex

	statusChangeReceivers []*statusChangeReceiver
	status                serviceStatus
	// The error that cause the service to terminate.
	err error
}

func (as *abstractService) Error() error {
	as.mu.Lock()
	defer as.mu.Unlock()
	return as.err
}

func (as *abstractService) AddStatusChangeReceiver(handle serviceHandle, onStatusChange serviceStatusChangeFunc) {
	as.mu.Lock()
	defer as.mu.Unlock()
	as.statusChangeReceivers = append(
		as.statusChangeReceivers,
		&statusChangeReceiver{handle, onStatusChange})
}

func (as *abstractService) RemoveStatusChangeReceiver(handle serviceHandle) {
	as.mu.Lock()
	defer as.mu.Unlock()

	for i := len(as.statusChangeReceivers) - 1; i >= 0; i-- {
		r := as.statusChangeReceivers[i]
		if r.handle == handle {
			// Swap with last element, erase last element and truncate the slice.
			lastIdx := len(as.statusChangeReceivers) - 1
			if i != lastIdx {
				as.statusChangeReceivers[i] = as.statusChangeReceivers[lastIdx]
			}
			as.statusChangeReceivers[lastIdx] = nil
			as.statusChangeReceivers = as.statusChangeReceivers[:lastIdx]
		}
	}
}

// Handle identifies this service instance, even when there are multiple layers
// of embedding.
func (as *abstractService) Handle() serviceHandle {
	return as
}

func (as *abstractService) unsafeCheckServiceStatus() error {
	switch {
	case as.status == serviceUninitialized:
		return ErrServiceUninitialized
	case as.status == serviceStarting:
		return ErrServiceStarting
	case as.status >= serviceTerminating:
		return ErrServiceStopped
	default:
		return nil
	}
}

// unsafeUpdateStatus assumes the service is already holding a mutex when
// called, as it often needs to be atomic with other operations.
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

	for _, receiver := range as.statusChangeReceivers {
		// Notify in a goroutine to prevent deadlocks if the receiver is holding a
		// locked mutex.
		go receiver.onStatusChange(as.Handle(), as.status, as.err)
	}
	return true
}

type serviceHolder struct {
	service    service
	lastStatus serviceStatus
	remove     bool
}

// compositeService can be embedded into other structs to manage child services.
// It implements the service interface and can itself be a dependency of another
// compositeService.
type compositeService struct {
	// Used to block until all dependencies have started or terminated.
	waitStarted    chan struct{}
	waitTerminated chan struct{}

	dependencies []*serviceHolder

	abstractService
}

// init must be called after creation of the derived struct.
func (cs *compositeService) init() {
	cs.waitStarted = make(chan struct{})
	cs.waitTerminated = make(chan struct{})
}

// Start up dependencies.
func (cs *compositeService) Start() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.abstractService.unsafeUpdateStatus(serviceStarting, nil) {
		for _, s := range cs.dependencies {
			s.service.Start()
		}
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

func (cs *compositeService) unsafeAddServices(services ...service) {
	for _, s := range services {
		s.AddStatusChangeReceiver(cs.Handle(), cs.onServiceStatusChange)
		cs.dependencies = append(cs.dependencies, &serviceHolder{service: s})
		if cs.status > serviceUninitialized {
			s.Start()
		}
	}
}

func (cs *compositeService) unsafeRemoveService(service service) {
	for _, s := range cs.dependencies {
		if s.service.Handle() == service.Handle() {
			// Remove the service from the list of dependencies after it has actually
			// terminated.
			s.remove = true
			if s.lastStatus < serviceTerminating {
				s.service.Stop()
			}
			break
		}
	}
}

func (cs *compositeService) unsafeInitiateShutdown(targetStatus serviceStatus, err error) {
	for _, s := range cs.dependencies {
		if s.lastStatus < serviceTerminating {
			s.service.Stop()
		}
	}
	cs.unsafeUpdateStatus(targetStatus, err)
}

func (cs *compositeService) unsafeUpdateStatus(targetStatus serviceStatus, err error) (ret bool) {
	previousStatus := cs.status
	if ret = cs.abstractService.unsafeUpdateStatus(targetStatus, err); ret {
		// Note: the waitStarted channel must be closed when the service fails to
		// start.
		if previousStatus == serviceStarting {
			close(cs.waitStarted)
		}
		if targetStatus == serviceTerminated {
			close(cs.waitTerminated)
		}
	}
	return
}

func (cs *compositeService) onServiceStatusChange(handle serviceHandle, status serviceStatus, err error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	numStarted := 0
	numTerminated := 0
	removeIdx := -1

	for i, s := range cs.dependencies {
		if s.service.Handle() == handle {
			if status > s.lastStatus {
				s.lastStatus = status
			}
			if s.lastStatus == serviceTerminated && s.remove {
				s.service.RemoveStatusChangeReceiver(cs.Handle())
				removeIdx = i
			}
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

	if removeIdx >= 0 {
		// Swap with last element, erase last element and truncate the slice.
		lastIdx := len(cs.dependencies) - 1
		if removeIdx != lastIdx {
			cs.dependencies[removeIdx] = cs.dependencies[lastIdx]
		}
		cs.dependencies[lastIdx] = nil
		cs.dependencies = cs.dependencies[:lastIdx]
	}

	switch {
	case numTerminated >= len(cs.dependencies):
		cs.unsafeUpdateStatus(serviceTerminated, err)
	case status >= serviceTerminating:
		cs.unsafeUpdateStatus(serviceTerminating, err)
	case numStarted >= len(cs.dependencies):
		cs.unsafeUpdateStatus(serviceActive, err)
	}
}

type periodicTask struct {
	period  time.Duration
	ticker  *time.Ticker
	stop    chan struct{}
	stopped bool
	task    func()
	name    string
}

func newPeriodicTask(period time.Duration, task func(), name string) *periodicTask {
	return &periodicTask{
		ticker: time.NewTicker(period),
		stop:   make(chan struct{}),
		period: period,
		task:   task,
		name:   name,
	}
}

func (pt *periodicTask) Start() {
	go pt.poll()
}

func (pt *periodicTask) Resume() {
	pt.ticker.Reset(pt.period)
}

func (pt *periodicTask) Pause() {
	pt.ticker.Stop()
}

// Stop permanently stops the periodic task.
func (pt *periodicTask) Stop() {
	// Prevent a panic if the stop channel has already been stopped.
	if !pt.stopped {
		close(pt.stop)
		pt.stopped = true
	}
}

func (pt *periodicTask) poll() {
	fmt.Printf("periodicTask(%s).Started: %v\n", pt.name, time.Now())
	for {
		select {
		case <-pt.stop:
			fmt.Printf("periodicTask(%s).Stopped: %v\n", pt.name, time.Now())
			// Ends the goroutine.
			return
		case <-pt.ticker.C:
			pt.task()
		}
	}
}
