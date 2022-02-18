package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type ingress struct {
	min   int64
	usage []float64
}

func (i *ingress) Aggregate(shardID string, record *types.Record) {
	an := record.ApproximateArrivalTimestamp.Round(time.Second).Unix()
	// At this point we have t which is a value between min and max
	// seconds in our series
	// Use the formula an = a + (n – 1)d to workout n
	// in this case d = 1 because we aggregate data one second intervals
	n := (an - i.min) + 1
	i.usage[n] = i.usage[n] + float64(len(record.Data))
}

func (i *ingress) Print(shardTree map[string][]string, limit int) {
	for idx, bps := range i.usage {
		if bps > 0 {
			fmt.Printf("%d - %f bytes\n", int64(idx)+i.min, bps)
		}
	}
}

func newIngress(start, end time.Time) *ingress {
	min := start.Round(time.Second).Unix()
	max := end.Round(time.Second).Unix()
	d := max - min
	return &ingress{
		min:   min,
		usage: make([]float64, d),
	}
}
