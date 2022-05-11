package main

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"hash/maphash"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type record struct {
	PartitionKey string `json:"partitionKey"`
	Count        int    `json:"count"`
}

type cms struct {
	sketch map[string][]int
	seed   maphash.Seed
	topK   []record
	count  int64
}

func (c *cms) Name() string {
	return "cms"
}

func (c *cms) Aggregate(r *types.Record) {
	u := c.addToSketch(*r.PartitionKey)
	if !c.updateTopK(*r.PartitionKey, u) {
		c.addToTopK(*r.PartitionKey, u)
	}
	c.count++
}

func (c *cms) Result() interface{} {
	return c.topK
}

// updateTopK finds the partition key in current top list and updates
// its count. If the partition key is not found in the top list, this
// method returns false otherwise true.
func (c *cms) updateTopK(key string, count int) bool {
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

func (c *cms) addToTopK(key string, count int) {
	if c.topK[len(c.topK)-1].Count > count {
		return
	}
	r := record{
		PartitionKey: key,
		Count:        count,
	}
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

func (c *cms) addToSketch(key string) int {
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

func newCMS(hashes, slots, limit int) (*cms, error) {
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
	return &cms{
		sketch: sketch,
		seed:   maphash.MakeSeed(),
		topK:   make([]record, limit),
	}, nil
}
