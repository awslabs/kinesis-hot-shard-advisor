package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
)

type Aggregator interface {
	Aggregate(shardID string, record *types.Record)
	Print(shardTree map[string][]string, limit int)
}

type record struct {
	partitionKey string
	shardID      string
	count        int
}

type cmd struct {
	streamName  string
	kds         kds
	aggregators []Aggregator
	since       time.Duration
	shardTree   map[string][]string
	limit       int
}

func (i *cmd) Start(ctx context.Context) error {
	fmt.Print(color.YellowString("Listing shards for stream %s...", i.streamName))
	shards, err := i.listShards(ctx, i.streamName)
	if err != nil {
		return err
	}
	color.Yellow("OK!")
	bar := pb.StartNew(len(shards))
	for _, shard := range shards {
		if shard.ParentShardId == nil {
			err := i.enumerate(ctx, shard.ShardId)
			if err != nil {
				return err
			}
		}
		bar.Increment()
	}
	bar.Finish()
	i.print()
	return nil
}

func (i *cmd) enumerate(ctx context.Context, shardID *string) error {
	var (
		si *string
	)
	gsii := &kinesis.GetShardIteratorInput{
		StreamName:        &i.streamName,
		ShardId:           shardID,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	}
	if i.since != 0 {
		is := time.Now().UTC().Add(i.since * -1)
		gsii.ShardIteratorType = types.ShardIteratorTypeAtTimestamp
		gsii.Timestamp = &is
	}
	iter, err := i.kds.GetShardIterator(ctx, gsii)
	if err != nil {
		return err
	}
	si = iter.ShardIterator
	for si != nil {
		gro, err := i.kds.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: si,
		})
		if err != nil {
			return err
		}
		si = gro.NextShardIterator
		for _, r := range gro.Records {
			wg := sync.WaitGroup{}
			wg.Add(len(i.aggregators))
			for _, a := range i.aggregators {
				go func(a Aggregator) {
					a.Aggregate(*shardID, &r)
					wg.Done()
				}(a)
			}
			wg.Wait()
		}
		if len(gro.ChildShards) > 0 {
			i.shardTree[*shardID] = make([]string, len(gro.ChildShards))
			for idx, cs := range gro.ChildShards {
				err := i.enumerate(ctx, cs.ShardId)
				if err != nil {
					return err
				}
				i.shardTree[*shardID][idx] = *cs.ShardId
			}
		}
		if *gro.MillisBehindLatest == 0 {
			break
		}
	}
	return nil
}

func (i *cmd) listShards(ctx context.Context, streamName string) ([]types.Shard, error) {
	var (
		ntoken *string
	)
	r := make([]types.Shard, 0)
	for {
		lso, err := i.kds.ListShards(ctx, &kinesis.ListShardsInput{
			StreamName: &streamName,
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

func (i *cmd) print() {
	for _, a := range i.aggregators {
		a.Print(i.shardTree, i.limit)
	}
}

func newCMD(streamName string, kds kds, aggregators []Aggregator, limit int, since time.Duration) *cmd {
	return &cmd{
		kds:         kds,
		streamName:  streamName,
		aggregators: aggregators,
		limit:       limit,
		since:       since,
	}
}
