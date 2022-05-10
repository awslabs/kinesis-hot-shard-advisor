package main

import (
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type count struct {
	store map[string]int
}

func (c *count) Name() string {
	return "count"
}

func (c *count) Aggregate(r *types.Record) {
	c.store[*r.PartitionKey] = c.store[*r.PartitionKey] + 1
}

func (c *count) Result() interface{} {
	type record struct {
		PartitionKey string `json:"partitionKey"`
		Count        int    `json:"count"`
	}
	records := make([]record, 0)
	for partitionKey, count := range c.store {
		records = append(records, record{
			PartitionKey: partitionKey,
			Count:        count,
		})
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Count > records[j].Count
	})
	return records
}

func newCount() *count {
	return &count{
		store: make(map[string]int),
	}
}
