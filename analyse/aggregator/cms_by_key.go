// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aggregator

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"hash/maphash"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// CMSByKey counts the number of times a partition key
// appears in a shard using Count Min Sketch algorithm.
// This is the preferred way to count when the shard
// being analyzed has a high cardinality in partition key.
// Count Min Sketch uses a probability based counting
// method that can be performed using a fixed amount of
// memory. This works well when the number of items
// being counted (i.e. partition keys) is very large.
type CMSByKey struct {
	sketch map[string][]int
	seed   maphash.Seed
	topK   []partitionKeyCount
	count  int64
}

func (c *CMSByKey) Name() string {
	return "cms"
}

func (c *CMSByKey) Aggregate(r *types.Record) {
	u := c.addToSketch(*r.PartitionKey)
	if !c.updateTopK(*r.PartitionKey, u) {
		c.addToTopK(*r.PartitionKey, u)
	}
	c.count++
}

func (c *CMSByKey) Result() interface{} {
	return c.topK
}

// updateTopK finds the partition key in current top list and updates
// its count. If the partition key is not found in the top list, this
// method returns false otherwise true.
func (c *CMSByKey) updateTopK(key string, count int) bool {
	for i := 0; i < len(c.topK); i++ {
		r := &c.topK[i]
		if r.PartitionKey == key {
			r.Count = count
			// Now that we've updated the count for the item for key
			// shift it up so that the list is still sorted.
			for j := i; j > 0; j-- {
				if c.topK[j].Count > c.topK[j-1].Count {
					t := c.topK[j-1]
					c.topK[j-1] = c.topK[j]
					c.topK[j] = t
				}
			}
			return true
		}
	}
	return false
}

func (c *CMSByKey) addToTopK(key string, count int) {
	if c.topK[len(c.topK)-1].Count > count {
		return
	}
	r := partitionKeyCount{key, count}
	c.topK[len(c.topK)-1] = r
	for i := len(c.topK); i > 1; i-- {
		if c.topK[i-1].Count <= c.topK[i-2].Count {
			break
		}
		t := c.topK[i-2]
		c.topK[i-2] = c.topK[i-1]
		c.topK[i-1] = t
	}
}

func (c *CMSByKey) addToSketch(key string) int {
	min := 0
	for h, slots := range c.sketch {
		hash := maphash.Hash{}
		hash.SetSeed(c.seed)
		i := fmt.Sprintf("%s%s", key, h)
		hash.Write([]byte(i))
		v := hash.Sum64()
		idx := v % uint64(len(slots))
		slots[idx] = slots[idx] + 1
		if min == 0 || slots[idx] < min {
			min = slots[idx]
		}
	}
	return min
}

func NewCMSByKey(hashes, slots, limit int) (*CMSByKey, error) {
	sketch := make(map[string][]int)
	buf := make([]byte, 32)
	for i := 0; i < hashes; i++ {
		_, err := rand.Read(buf)
		if err != nil {
			return nil, err
		}
		h := base64.RawStdEncoding.EncodeToString(buf)
		sketch[h] = make([]int, slots)
	}
	return &CMSByKey{
		sketch: sketch,
		seed:   maphash.MakeSeed(),
		topK:   make([]partitionKeyCount, limit),
	}, nil
}
