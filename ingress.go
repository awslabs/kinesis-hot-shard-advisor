package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/guptarohit/asciigraph"
)

type ingress struct {
	min   int64
	max   int64
	usage map[string][]float64
}

func (i *ingress) Aggregate(shardID string, record *types.Record) {
	if _, ok := i.usage[shardID]; !ok {
		i.usage[shardID] = make([]float64, int(i.max-i.min))
	}
	an := record.ApproximateArrivalTimestamp.Round(time.Second).Unix()
	// At this point we have t which is a value between min and max
	// seconds in our series
	// Use the formula an = a + (n – 1)d to workout n
	// in this case d = 1 because we aggregate data one second intervals
	n := (an - i.min) + 1
	i.usage[shardID][n] = i.usage[shardID][n] + float64(len(record.Data))
}

func (i *ingress) Print(shardTree map[string][]string, limit int) {
	for shardID, data := range i.usage {
		g := asciigraph.Plot(data, asciigraph.Width(200), asciigraph.Height(10))
		fmt.Println(shardID)
		fmt.Println(g)
	}
}

func newIngress(start, end time.Time) *ingress {
	min := start.Round(time.Second).Unix()
	max := end.Round(time.Second).Unix()
	return &ingress{
		min:   min,
		max:   max,
		usage: make(map[string][]float64),
	}
}
