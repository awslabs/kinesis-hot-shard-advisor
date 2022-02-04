package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type ingress struct {
	count map[string]map[int64]int64
}

func (i *ingress) Aggregate(shardID string, record *types.Record) {
	t := record.ApproximateArrivalTimestamp.Round(time.Second).Unix()
	if _, ok := i.count[shardID]; !ok {
		i.count[shardID] = make(map[int64]int64)
	}
	i.count[shardID][t] = i.count[shardID][t] + int64(len(record.Data))
}

func (i *ingress) Print(shardTree map[string][]string, limit int) {
	for s, bps := range i.count {
		fmt.Println(s)
		for t, c := range bps {
			fmt.Println(t, "-", c, " bytes")
		}
	}
}

func newIngress() *ingress {
	return &ingress{
		count: make(map[string]map[int64]int64),
	}
}
