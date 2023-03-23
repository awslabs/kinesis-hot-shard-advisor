// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package service

import (
	"sort"
	"time"
)

type Output struct {
	start    time.Time
	limit    int
	top      int
	reporter Reporter
}

func NewOutput(start time.Time, limit, top int, reporter Reporter) *Output {
	return &Output{
		start:    start,
		limit:    limit,
		top:      top,
		reporter: reporter,
	}
}

func (o *Output) Write(processOutput []*ProcessOutput) error {
	reportData := make(map[string]map[string]interface{})
	processOutput = o.chooseTopN(processOutput)
	for _, s := range processOutput {
		r := make(map[string]interface{})
		for _, a := range s.Aggregators {
			r[a.Name()] = a.Result()
		}
		reportData[s.ShardID] = r
	}

	return o.reporter.Report(o.start, reportData, o.limit)
}

func (o *Output) chooseTopN(results []*ProcessOutput) []*ProcessOutput {
	if o.top == 0 || o.top > len(results) {
		return results
	}

	sort.Slice(results, func(i, j int) bool {
		a := results[i]
		b := results[j]
		for i := range a.Aggregators {
			if tma, ok := a.Aggregators[i].(throttledMetric); ok {
				tmb := b.Aggregators[i].(throttledMetric)
				if tma.MaxUtilisation() > tmb.MaxUtilisation() {
					return true
				}
			}
		}
		return false
	})

	return results[0:o.top]
}
