// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aggregator

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// CountPerSecond is an Aggregator to count number of records
// received per second (based on ApproximateArrivalTimestamp).
type CountPerSecond struct {
	min             int64 // Start time of aggregation in Unix time format
	max             int64 // End time of aggregation in Unix time format
	timeSeries      []int // Store usage value for each shard as an array. Array index is the ordinal value of second within the specified range.
	sum             int
	maxIngressCount int
}

type IngressCountStats struct {
	TimeSeries []int `json:"timeSeries"`
	Sum        int   `json:"sum"`
	Max        int   `json:"max"`
}

func (i *CountPerSecond) Name() string {
	return "ingress-count"
}

func (i *CountPerSecond) Aggregate(record *types.Record) {
	an := record.ApproximateArrivalTimestamp.Unix()
	offset := (an - i.min)
	if offset < 0 || an > i.max {
		return
	}
	i.timeSeries[offset] = i.timeSeries[offset] + 1
	i.sum = i.sum + 1
	if i.maxIngressCount < i.timeSeries[offset] {
		i.maxIngressCount = i.timeSeries[offset]
	}
}

func (i *CountPerSecond) Result() interface{} {
	return IngressCountStats{
		i.timeSeries,
		i.sum,
		i.maxIngressCount,
	}
}

func NewCountPerSecond(start, end time.Time) *CountPerSecond {
	min := start.Unix()
	max := end.Unix()
	return &CountPerSecond{
		min:        min,
		max:        max,
		timeSeries: make([]int, int(max-min)+1),
	}
}
