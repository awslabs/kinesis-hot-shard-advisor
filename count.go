package main

import (
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/fatih/color"
)

type count struct {
	count         map[string]int
	keyToShardMap map[string]string // tracks each key and last shard id it appeared in
}

func (c *count) Aggregate(shardID string, r *types.Record) {
	c.count[*r.PartitionKey] = c.count[*r.PartitionKey] + 1
	c.keyToShardMap[*r.PartitionKey] = shardID
}

func (c *count) Print(shardTree map[string][]string, limit int) {
	sorted, total := c.countAndSort()
	fmt.Println()
	color.Green("Usage     Count      Split Candidate          Key")
	color.Green("――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――")
	dc := len(sorted)
	if dc > limit {
		dc = limit
	}
	for idx := 0; idx < dc; idx++ {
		i := sorted[idx]
		fmt.Printf("%4.1f%%     %-6d     %20s     %s\n", (float32(i.count)/float32(total))*100, i.count, c.splitCandidate(shardTree, i.partitionKey), i.partitionKey)
	}
}

func (c *count) countAndSort() ([]*record, int) {
	t := 0
	out := make([]*record, 0)
	for k, v := range c.count {
		out = append(out, &record{
			partitionKey: k,
			count:        v,
		})
		t += v
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].count > out[j].count
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
