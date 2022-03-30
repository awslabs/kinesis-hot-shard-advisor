package main

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// ingress is an Aggregator to count number of bytes
// received per second (based on ApproximateArrivalTimestamp).
type ingress struct {
	min   int64            // Start time of aggregation in Unix time format
	max   int64            // End time of aggregation in Unix time format
	usage map[string][]int // Store usage value for each shard as an array. Array index is the ordinal value of second within the specified range.
}

func (i *ingress) Name() string {
	return "ingress"
}

func (i *ingress) Aggregate(shardID string, record *types.Record) {
	if _, ok := i.usage[shardID]; !ok {
		//last second is inclusive, so to handle the out of index issue
		i.usage[shardID] = make([]int, int(i.max-i.min)+1)
	}
	an := record.ApproximateArrivalTimestamp.Round(time.Second).Unix()
	// At this point we have t which is a value between min and max
	// seconds in our series
	// Use the formula an = a + (n – 1)d to workout n
	// in this case d = 1 because we aggregate data one second intervals
	n := (an - i.min)
	i.usage[shardID][n] = i.usage[shardID][n] + len(record.Data)

}

func (i *ingress) Result(shardTree map[string][]string, limit int) map[string]interface{} {
	results := make(map[string]interface{})
	for k, v := range i.usage {
		results[k] = v
	}
	return results
}

func newIngress(start, end time.Time) *ingress {
	min := start.Unix()
	max := end.Unix()

	return &ingress{
		min:   min,
		max:   max,
		usage: make(map[string][]int),
	}
}
