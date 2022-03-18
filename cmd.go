package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"sync"
	"time"

	_ "embed"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
)

//go:embed output.html
var outputTemplate string

type Aggregator interface {
	Name() string
	Aggregate(shardID string, record *types.Record)
	Result(shardTree map[string][]string, limit int) interface{}
}

type PartitionKeyCountByShard struct {
	PartitionKey   string `json:"partitionKey"`
	ShardID        string `json:"shardId"`
	Count          int    `json:"count"`
	SplitCandidate string `json:"splitCandidate"`
}

type cmd struct {
	streamName  string
	kds         kds
	aggregators []Aggregator
	shardTree   map[string][]string
	limit       int
	period      *period
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
	if i.period != nil {
		gsii.ShardIteratorType = types.ShardIteratorTypeAtTimestamp
		gsii.Timestamp = &i.period.start
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
			approxtime := r.ApproximateArrivalTimestamp.Round(time.Second).Unix()
			endtime := i.period.end.Unix()
			if approxtime > endtime {
				return nil
			}
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
			return nil
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
	results := make(map[string]interface{})
	for _, a := range i.aggregators {
		results[a.Name()] = a.Result(i.shardTree, i.limit)
	}
	buf, err := json.MarshalIndent(results, "", " ")
	if err != nil {
		panic(err)
	}
	t, err := template.New("output").Parse(outputTemplate)
	if err != nil {
		panic(err)
	}
	fname := time.Now().Format("2006-01-02-15-04.html")
	file, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0400)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	err = t.Execute(file, map[string]interface{}{
		"Date": time.Now(),
		"Data": template.JS(string(buf)),
	})
	if err != nil {
		panic(err)
	}
}

func newCMD(streamName string, kds kds, aggregators []Aggregator, limit int, p *period) *cmd {
	return &cmd{
		kds:         kds,
		streamName:  streamName,
		aggregators: aggregators,
		limit:       limit,
		period:      p,
	}
}
