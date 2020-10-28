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

import "sync"

// service is the interface that must be implemented by services (essentially
// gRPC client stream wrappers) that can be dependencies of a compositeService.
type service interface {
	// Start the service and attempt to open the gRPC client stream.
	Start()
	// Stop the service gracefully, flushing any pending data.
	Stop()
}

// serviceEvent are events that the service must send notifications for.
type serviceEvent int

const (
	// Service successfully started.
	serviceStarted serviceEvent = 1
	// Service has started shutting down and is flushing pending data, but not
	// accepting new data. It must send the `serviceTerminated` event once done.
	serviceTerminating serviceEvent = 2
	// Service has permanently terminated and no longer active.
	serviceTerminated serviceEvent = 3
)

// serviceEventFunc notifies the parent of service events. `serviceTerminating`
// and `serviceTerminated` have an associated error. This error may be nil if
// the user called Stop().
type serviceEventFunc func(service, serviceEvent, error)

type serviceHolder struct {
	service   service
	lastEvent serviceEvent
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

// Start starts up dependencies and waits for them to finish.
func (cs *compositeService) Start() error {
	for _, s := range cs.services() {
		s.service.Start()
	}
	cs.waitStarted.Wait()
	return cs.Error()
}

// Stop stops all dependencies and waits for them to terminate.
func (cs *compositeService) Stop() {
	for _, s := range cs.services() {
		s.service.Stop()
	}
	cs.waitTerminated.Wait()
}

func (cs *compositeService) Error() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.finalErr
}

// addService does not acquire the mutex as it would likely be called while it
// is held.
func (cs *compositeService) addService(service service) {
	cs.waitStarted.Add(1)
	cs.waitTerminated.Add(1)
	cs.dependencies = append(cs.dependencies, &serviceHolder{service: service})
}

func (cs *compositeService) services() []*serviceHolder {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.dependencies
}

func (cs *compositeService) onServiceEvent(service service, event serviceEvent, err error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if event > serviceStarted && cs.finalErr == nil {
		cs.finalErr = err
	}

	for _, s := range cs.dependencies {
		if s.service == service {
			if (event == serviceStarted || event == serviceTerminated) && s.lastEvent < serviceStarted {
				cs.waitStarted.Done()
			}
			if event == serviceTerminated && s.lastEvent < serviceTerminated {
				cs.waitTerminated.Done()
			}
			s.lastEvent = event
		} else if event >= serviceTerminating && s.lastEvent < serviceTerminating {
			// If a single service terminates, stop them all, but allow the others to
			// flush pending data.
			s.service.Stop()
		}
	}
}
