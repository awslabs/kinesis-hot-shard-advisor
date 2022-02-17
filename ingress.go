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
	t := record.ApproximateArrivalTimestamp.Round(time.Second).Unix()
	c := int64(len(i.usage))
	s := int64(c - i.min%c)
	i.usage[(t+s)%c] = i.usage[(t+s)%c] + float64(len(record.Data))
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
