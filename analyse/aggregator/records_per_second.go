// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aggregator

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// RecordsPerSecond is an Aggregator to count number of records
// received per second (based on ApproximateArrivalTimestamp).
type RecordsPerSecond struct {
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

func (i *RecordsPerSecond) Name() string {
	return "ingress-count"
}

func (i *RecordsPerSecond) Aggregate(record *types.Record) {
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

func (i *RecordsPerSecond) Result() interface{} {
	return IngressCountStats{
		i.timeSeries,
		i.sum,
		i.maxIngressCount,
	}
}

func (i *RecordsPerSecond) MaxUtilisation() float32 {
	const maxRecordsPerSecond = float32(1000)
	return float32(i.maxIngressCount) / maxRecordsPerSecond
}

func NewCountPerSecond(start, end time.Time) *RecordsPerSecond {
	min := start.Unix()
	max := end.Unix()
	return &RecordsPerSecond{
		min:        min,
		max:        max,
		timeSeries: make([]int, int(max-min)+1),
	}
}
