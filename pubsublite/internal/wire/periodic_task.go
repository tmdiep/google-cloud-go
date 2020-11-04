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
	"time"
)

// periodicTask is an interface for a recurring background task.
type periodicTask interface {
	// Start polling goroutines.
	Start()
	// Pause temporarily suspends the polling.
	Pause()
	// Resume polling.
	Resume()
	// Stop permanently stops the periodic task.
	Stop()
}

type periodicTaskFactory interface {
	New(period time.Duration, task func()) periodicTask
}

// pollingPeriodicTask is the concrete implementation for periodicTask.
type pollingPeriodicTask struct {
	period  time.Duration
	ticker  *time.Ticker
	stop    chan struct{}
	stopped bool
	task    func()
}

type pollingPeriodicTaskFactory struct{}

func (pf *pollingPeriodicTaskFactory) New(period time.Duration, task func()) periodicTask {
	return &pollingPeriodicTask{
		ticker: time.NewTicker(period),
		stop:   make(chan struct{}),
		period: period,
		task:   task,
	}
}

func (pt *pollingPeriodicTask) Start() {
	go pt.poll()
}

func (pt *pollingPeriodicTask) Resume() {
	pt.ticker.Reset(pt.period)
}

func (pt *pollingPeriodicTask) Pause() {
	pt.ticker.Stop()
}

func (pt *pollingPeriodicTask) Stop() {
	// Prevent a panic if the stop channel has already been stopped.
	if !pt.stopped {
		close(pt.stop)
		pt.stopped = true
	}
}

func (pt *pollingPeriodicTask) poll() {
	for {
		select {
		case <-pt.stop:
			// Ends the goroutine.
			return
		case <-pt.ticker.C:
			pt.task()
		}
	}
}

// nullPeriodicTask can be used by unit tests to disable the background task
// polling and have more control over when the task executes.
type nullPeriodicTask struct{}

func (nt *nullPeriodicTask) Start()  {}
func (nt *nullPeriodicTask) Pause()  {}
func (nt *nullPeriodicTask) Resume() {}
func (nt *nullPeriodicTask) Stop()   {}

type nullPeriodicTaskFactory struct{}

func (nf *nullPeriodicTaskFactory) New(period time.Duration, task func()) periodicTask {
	return new(nullPeriodicTask)
}
