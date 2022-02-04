package main

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"hash/maphash"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/fatih/color"
)

type cms struct {
	sketch map[string][]int
	seed   maphash.Seed
	topK   []record
	count  int64
}

func (c *cms) Aggregate(shardID string, r *types.Record) {
	u := c.addToSketch(*r.PartitionKey)
	if !c.updateTopK(*r.PartitionKey, shardID, u) {
		c.addToTopK(*r.PartitionKey, shardID, u)
	}
	c.count++
}

func (c *cms) Print(shardTree map[string][]string, limit int) {
	color.Green("Usage     Split Candidate          Key")
	color.Green("――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――")
	for idx := 0; idx < len(c.topK); idx++ {
		i := c.topK[idx]
		if i.count == 0 {
			break
		}
		fmt.Printf("%4.1f%%     %s     %s\n", (float32(i.count)/float32(c.count))*100, c.splitCandidate(shardTree, i.shardID), i.partitionKey)
	}
}

func (c *cms) splitCandidate(shardTree map[string][]string, shardID string) string {
	if len(shardTree[shardID]) == 0 {
		return shardID
	}
	return ""
}

func (c *cms) updateTopK(key, shardID string, count int) bool {
	for i := 0; i < len(c.topK); i++ {
		r := &c.topK[i]
		if r.count == 0 {
			return false
		}
		if r.partitionKey == key {
			r.shardID = shardID
			r.count = count
			for j := i; j > 0; j-- {
				if c.topK[j].count > c.topK[j-1].count {
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

func (c *cms) addToTopK(key, shardID string, count int) {
	if c.topK[len(c.topK)-1].count > count {
		return
	}
	r := record{
		partitionKey: key,
		shardID:      shardID,
		count:        count,
	}
	c.topK[len(c.topK)-1] = r
	for i := len(c.topK); i > 1; i-- {
		if c.topK[i-1].count <= c.topK[i-2].count {
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
