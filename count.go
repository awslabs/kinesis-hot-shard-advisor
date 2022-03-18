package main

import (
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type count struct {
	count         map[string]int
	keyToShardMap map[string]string // tracks each key and last shard id it appeared in
}

func (c *count) Name() string {
	return "count"
}

func (c *count) Aggregate(shardID string, r *types.Record) {
	c.count[*r.PartitionKey] = c.count[*r.PartitionKey] + 1
	c.keyToShardMap[*r.PartitionKey] = shardID
}

func (c *count) Result(shardTree map[string][]string, limit int) interface{} {
	sorted, _ := c.countAndSort(shardTree)
	return sorted
}

func (c *count) countAndSort(shardTree map[string][]string) ([]*PartitionKeyCountByShard, int) {
	t := 0
	out := make([]*PartitionKeyCountByShard, 0)
	for k, v := range c.count {
		out = append(out, &PartitionKeyCountByShard{
			PartitionKey:   k,
			Count:          v,
			SplitCandidate: c.splitCandidate(shardTree, k),
		})
		t += v
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Count > out[j].Count
	})
	return out, t
}

func (c *count) splitCandidate(shardTree map[string][]string, key string) string {
	ls := c.keyToShardMap[key]
	if len(shardTree[ls]) == 0 {
		return ls
	}
	return ""
}

func newCount() *count {
	return &count{
		keyToShardMap: make(map[string]string),
		count:         make(map[string]int),
	}
}
