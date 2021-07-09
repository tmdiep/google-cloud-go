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

package integration

import (
	"fmt"
	"strings"
	"time"
)

type counter struct {
	Threshold time.Duration
	Count     int64
}

type LatencyHistogram struct {
	counters []*counter
	Total    int64
}

func NewLatencyHistogram(thresholds []time.Duration) *LatencyHistogram {
	h := new(LatencyHistogram)
	for _, t := range thresholds {
		h.counters = append(h.counters, &counter{Threshold: t, Count: 0})
	}
	return h
}

func (h *LatencyHistogram) Add(d time.Duration) {
	h.Total++
	for _, counter := range h.counters {
		if d > counter.Threshold {
			counter.Count++
		} else {
			break
		}
	}
}

func (h *LatencyHistogram) Status(label string) string {
	var statuses []string
	for _, counter := range h.counters {
		statuses = append(statuses, fmt.Sprintf(">%v=%d", counter.Threshold, counter.Count))
	}
	return fmt.Sprintf("%s=%d, %s", label, h.Total, strings.Join(statuses, ", "))
}
