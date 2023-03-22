// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aggregator

import (
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type CountByKey struct {
	store map[string]int
}

func (c *CountByKey) Name() string {
	return "count"
}

func (c *CountByKey) Aggregate(r *types.Record) {
	c.store[*r.PartitionKey] = c.store[*r.PartitionKey] + 1
}

func (c *CountByKey) Result() interface{} {
	records := make([]partitionKeyCount, 0)
	for partitionKey, count := range c.store {
		records = append(records, partitionKeyCount{
			PartitionKey: partitionKey,
			Count:        count,
		})
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Count > records[j].Count
	})
	return records
}

func NewCountByKey() *CountByKey {
	return &CountByKey{
		store: make(map[string]int),
	}
}

// partitionKeyCount is used by counting aggregators
// to report the number of times a given partition key
// appears in a shard.
type partitionKeyCount struct {
	PartitionKey string `json:"partitionKey"`
	Count        int    `json:"count"`
}
