package main

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// ingress is an Aggregator to count number of bytes
// received per second (based on ApproximateArrivalTimestamp).
type ingress struct {
	min        int64 // Start time of aggregation in Unix time format
	max        int64 // End time of aggregation in Unix time format
	timeSeries []int // Store usage value for each shard as an array. Array index is the ordinal value of second within the specified range.
	sum        int
	maxIngress int
}

type ingressStats struct {
	TimeSeries []int `json:"timeSeries"`
	Sum        int   `json:"sum"`
	Max        int   `json:"max"`
}

func (i *ingress) Name() string {
	return "ingress"
}

func (i *ingress) Aggregate(record *types.Record) {
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

func (i *ingress) Result() interface{} {
	return ingressStats{
		i.timeSeries,
		i.sum,
		i.maxIngress,
	}
}

func newIngress(start, end time.Time) *ingress {
	min := start.Unix()
	max := end.Unix()
	return &ingress{
		min:        min,
		max:        max,
		timeSeries: make([]int, int(max-min)+1),
	}
}
