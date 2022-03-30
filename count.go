package main

import (
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type count struct {
	store map[string]map[string]int
}

func (c *count) Name() string {
	return "count"
}

func (c *count) Aggregate(shardID string, r *types.Record) {
	if _, ok := c.store[shardID]; !ok {
		c.store[shardID] = make(map[string]int)
	}
	c.store[shardID][*r.PartitionKey] = c.store[shardID][*r.PartitionKey] + 1
}

func (c *count) Result(shardTree map[string][]string, limit int) map[string]interface{} {
	type record struct {
		PartitionKey string `json:"partitionKey"`
		Count        int    `json:"count"`
	}
	out := make(map[string]interface{})
	for shardID, partitionKeys := range c.store {
		records := make([]record, 0)
		for partitionKey, count := range partitionKeys {
			records = append(records, record{
				PartitionKey: partitionKey,
				Count:        count,
			})
		}
		sort.Slice(records, func(i, j int) bool {
			return records[i].Count > records[j].Count
		})
		out[shardID] = records
	}
	return out
}

func newCount() *count {
	return &count{
		store: make(map[string]map[string]int),
	}
}
