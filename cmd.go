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

//go:embed template.html
var outputTemplate string

type Aggregator interface {
	Name() string
	Aggregate(shardID string, record *types.Record)
	Result(shardTree map[string][]string, limit int) map[string]interface{}
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
	// Store all shard ids in a map so that we can
	// quickly check if a given shard id exists in the
	// list or not.
	shardsSet := make(map[string]bool)
	for _, shard := range shards {
		shardsSet[*shard.ShardId] = true
	}
	bar := pb.StartNew(len(shards))
	for _, shard := range shards {
		isTopLevelShard := false
		if shard.ParentShardId != nil {
			_, isTopLevelShard = shardsSet[*shard.ParentShardId]
		}
		if !isTopLevelShard {
			err := i.enumerate(ctx, shard.ShardId)
			if err != nil {
				return err
			}
		}
		bar.Increment()
	}
	bar.Finish()
	i.generateReport()
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

func (i *cmd) generateReport() {
	type shardStats struct {
		ShardID string                 `json:"shardId"`
		Stats   map[string]interface{} `json:"stats"`
	}
	type report struct {
		From   int          `json:"from"`
		Shards []shardStats `json:"shards"`
	}
	stats := make(map[string]map[string]interface{})
	for _, a := range i.aggregators {
		result := a.Result(i.shardTree, i.limit)
		for sid, data := range result {
			if _, ok := stats[sid]; !ok {
				stats[sid] = make(map[string]interface{})
			}
			stats[sid][a.Name()] = data
		}
	}
	data := make([]shardStats, 0)
	for sid, v := range stats {
		data = append(data, shardStats{sid, v})
	}
	r := &report{
		From:   int(i.period.start.Unix()),
		Shards: data,
	}
	buf, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	t, err := template.New("output").Funcs(template.FuncMap{
		"trustedJS": func(s string) template.JS {
			return template.JS(s)
		},
		"trustedHTML": func(s string) template.HTML {
			return template.HTML(s)
		},
	}).Parse(outputTemplate)
	if err != nil {
		panic(err)
	}
	fname := "out.html"
	file, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Sync()
	defer file.Close()
	err = t.Execute(file, map[string]interface{}{
		"Date":   time.Now(),
		"Limit":  i.limit,
		"Report": template.JS(string(buf)),
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("output is written to %s\n", fname)
}

func newCMD(streamName string, kds kds, aggregators []Aggregator, limit int, p *period) *cmd {
	return &cmd{
		kds:         kds,
		streamName:  streamName,
		aggregators: aggregators,
		limit:       limit,
		period:      p,
		shardTree:   make(map[string][]string),
	}
}
