package main

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// ingress is an Aggregator to count number of bytes
// received per second (based on ApproximateArrivalTimestamp).
type ingress struct {
	min   int64 // Start time of aggregation in Unix time format
	max   int64 // End time of aggregation in Unix time format
	usage []int // Store usage value for each shard as an array. Array index is the ordinal value of second within the specified range.
}

func (i *ingress) Name() string {
	return "ingress"
}

func (i *ingress) Aggregate(record *types.Record) {
	an := record.ApproximateArrivalTimestamp.Unix()
	// At this point we have t which is a value between min and max
	// seconds in our series
	// Use the formula an = a + (n – 1)d to workout n
	// in this case d = 1 because we aggregate data one second intervals
	n := (an - i.min)
	i.usage[n] = i.usage[n] + len(record.Data)
}

func (i *ingress) Result() interface{} {
	return i.usage
}

func newIngress(start, end time.Time) *ingress {
	min := start.Unix()
	max := end.Unix()
	return &ingress{
		min:   min,
		max:   max,
		usage: make([]int, int(max-min)+1),
	}
}
