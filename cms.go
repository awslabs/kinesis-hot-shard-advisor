package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"hash/maphash"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
)

type cms struct {
	streamName string
	sketch     map[string][]int
	stream     kds
	shardTree  map[string][]string // tracks each shard id and child shard ids (if there's any)
	since      time.Duration
	seed       maphash.Seed
	topK       []record
	count      int64
}

func (c *cms) Run(ctx context.Context) error {
	fmt.Print(color.YellowString("Listing shards for stream %s...", c.streamName))
	shards, err := listShards(ctx, c.streamName, c.stream)
	if err != nil {
		return err
	}
	color.Yellow("OK!")
	bar := pb.StartNew(len(shards))
	for _, shard := range shards {
		if shard.ParentShardId == nil {
			err := c.enumerate(ctx, shard.ShardId)
			if err != nil {
				return err
			}
		}
		bar.Increment()
	}
	bar.Finish()
	c.print()
	return nil
}

func (c *cms) enumerate(ctx context.Context, shardID *string) error {
	var (
		si *string
	)
	gsii := &kinesis.GetShardIteratorInput{
		StreamName:        &c.streamName,
		ShardId:           shardID,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	}
	if c.since != 0 {
		is := time.Now().UTC().Add(c.since * -1)
		gsii.ShardIteratorType = types.ShardIteratorTypeAtTimestamp
		gsii.Timestamp = &is
	}
	iter, err := c.stream.GetShardIterator(ctx, gsii)
	if err != nil {
		return err
	}
	si = iter.ShardIterator
	for si != nil {
		gro, err := c.stream.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: si,
		})
		if err != nil {
			return err
		}
		si = gro.NextShardIterator
		for _, r := range gro.Records {
			u := c.addToSketch(*r.PartitionKey)
			if !c.updateTopK(*r.PartitionKey, *shardID, u) {
				c.addToTopK(*r.PartitionKey, *shardID, u)
			}
			c.count++
		}
		if len(gro.ChildShards) > 0 {
			c.shardTree[*shardID] = make([]string, len(gro.ChildShards))
			for i, cs := range gro.ChildShards {
				err := c.enumerate(ctx, cs.ShardId)
				if err != nil {
					return err
				}
				c.shardTree[*shardID][i] = *cs.ShardId
			}
		}
		if *gro.MillisBehindLatest == 0 {
			break
		}
	}
	return nil
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

func (c *cms) print() {
	fmt.Println()
	color.Green("Usage     Split Candidate          Key")
	color.Green("――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――")
	for idx := 0; idx < len(c.topK); idx++ {
		i := c.topK[idx]
		if i.count == 0 {
			break
		}
		fmt.Printf("%4.1f%%     %s     %s\n", (float32(i.count)/float32(c.count))*100, c.splitCandidate(i.shardID), i.partitionKey)
	}
}

func (c *cms) splitCandidate(shardID string) string {
	if len(c.shardTree[shardID]) == 0 {
		return shardID
	}
	return ""
}

func newCMS(streamName string, stream kds, since time.Duration, hashes, slots, limit int) (*cms, error) {
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
		streamName: streamName,
		stream:     stream,
		since:      since,
		sketch:     sketch,
		seed:       maphash.MakeSeed(),
		topK:       make([]record, limit),
	}, nil
}
