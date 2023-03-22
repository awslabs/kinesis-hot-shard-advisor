// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aggregator

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// BytesPerSecond is an Aggregator to count number of bytes
// received per second (based on ApproximateArrivalTimestamp).
type BytesPerSecond struct {
	min        int64 // Start time of aggregation in Unix time format
	max        int64 // End time of aggregation in Unix time format
	timeSeries []int // Store usage value for each shard as an array. Array index is the ordinal value of second within the specified range.
	sum        int
	maxIngress int
}

type IngressBytesStats struct {
	TimeSeries []int `json:"timeSeries"`
	Sum        int   `json:"sum"`
	Max        int   `json:"max"`
}

func (i *BytesPerSecond) Name() string {
	return "ingress-bytes"
}

func (i *BytesPerSecond) Aggregate(record *types.Record) {
	an := record.ApproximateArrivalTimestamp.Unix()
	offset := (an - i.min)
	if offset < 0 || an > i.max {
		return
	}
	i.timeSeries[offset] = i.timeSeries[offset] + len(record.Data)
	i.sum = i.sum + len(record.Data)
	if i.maxIngress < i.timeSeries[offset] {
		i.maxIngress = i.timeSeries[offset]
	}
}

func (i *BytesPerSecond) Result() interface{} {
	return IngressBytesStats{
		i.timeSeries,
		i.sum,
		i.maxIngress,
	}
}

func (a *BytesPerSecond) MaxUtilisation() float32 {
	const maxBytesPerSecond = float32(1024 * 1024)
	return float32(a.maxIngress) / maxBytesPerSecond
}

func NewBytesPerSecond(start, end time.Time) *BytesPerSecond {
	min := start.Unix()
	max := end.Unix()
	return &BytesPerSecond{
		min:        min,
		max:        max,
		timeSeries: make([]int, int(max-min)+1),
	}
}
