package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cheggaaa/pb"
)

type cmd struct {
	stream        string
	count         map[string]int
	shardTree     map[string][]string // tracks each shard id and child shard ids (if there's any)
	keyToShardMap map[string]string   // tracks each key and last shard id it appeared in
	kc            *kinesis.Client
	cutoff        time.Time
	since         time.Duration
	limit         int
}

type record struct {
	partitionKey string
	count        int
}

func (c *cmd) Run() error {
	ctx := context.Background()
	fmt.Printf("Listing shards for stream %s...", c.stream)
	shards, err := c.listShards(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("OK!\n")
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

func (c *cmd) listShards(ctx context.Context) ([]types.Shard, error) {
	var (
		ntoken *string
	)
	r := make([]types.Shard, 0)
	for {
		lso, err := c.kc.ListShards(ctx, &kinesis.ListShardsInput{
			StreamName: &c.stream,
			NextToken:  ntoken,
		})
		if err != nil {
			return nil, err
		}
		r = append(r, lso.Shards...)
		ntoken = lso.NextToken
		if ntoken == nil {
			break
		}
	}
	return r, nil
}

func (c *cmd) enumerate(ctx context.Context, shardID *string) error {
	var (
		si *string
	)
	gsii := &kinesis.GetShardIteratorInput{
		StreamName:        &c.stream,
		ShardId:           shardID,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	}
	if c.since != 0 {
		is := time.Now().UTC().Add(c.since * -1)
		gsii.ShardIteratorType = types.ShardIteratorTypeAtTimestamp
		gsii.Timestamp = &is
	}
	iter, err := c.kc.GetShardIterator(ctx, gsii)
	if err != nil {
		return err
	}
	si = iter.ShardIterator
	for si != nil {
		gro, err := c.kc.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: si,
		})
		if err != nil {
			return err
		}
		si = gro.NextShardIterator
		for _, r := range gro.Records {
			c.count[*r.PartitionKey] = c.count[*r.PartitionKey] + 1
			c.keyToShardMap[*r.PartitionKey] = *shardID
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

func (c *cmd) countAndSort() ([]*record, int) {
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

func (c *cmd) print() {
	sorted, total := c.countAndSort()
	fmt.Println()
	fmt.Println("Usage     Count      Split Candidate          Key")
	fmt.Println("――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――")
	for idx := 0; idx < c.limit; idx++ {
		i := sorted[idx]
		fmt.Printf("%4.1f%%     %-6d     %s     %s\n", (float32(i.count)/float32(total))*100, i.count, c.splitCandidate(i.partitionKey), i.partitionKey)
	}
}

func (c *cmd) splitCandidate(key string) string {
	ls := c.keyToShardMap[key]
	if len(c.shardTree[ls]) == 0 {
		return ls
	}
	return ""
}

func newCmd(kc *kinesis.Client, stream string, since time.Duration, limit int) *cmd {
	return &cmd{
		stream:        stream,
		count:         make(map[string]int),
		shardTree:     make(map[string][]string),
		keyToShardMap: make(map[string]string),
		kc:            kc,
		cutoff:        time.Now(),
		since:         since,
		limit:         limit,
	}
}
