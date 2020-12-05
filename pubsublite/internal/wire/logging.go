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
)

// LogFunc is a user-provided function to receive information log messages.
type LogFunc func(string)

// logger wraps a user-provided log message receiver function. It does not log
// if function is not provided.
type logger struct {
	log LogFunc
}

func newLogger(log LogFunc) *logger {
	return &logger{log: log}
}

func nilLogger() *logger {
	return new(logger)
}

func (l *logger) Printf(format string, v ...interface{}) {
	if l.log != nil {
		l.log(fmt.Sprintf(format, v...))
	}
}

func (l *logger) Print(v ...interface{}) {
	if l.log != nil {
		l.log(fmt.Sprint(v...))
	}
}

func (l *logger) Println(v ...interface{}) {
	if l.log != nil {
		l.log(fmt.Sprintln(v...))
	}
}
