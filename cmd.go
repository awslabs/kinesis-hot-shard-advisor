// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"os"
	"time"

	_ "embed"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	smithy "github.com/aws/smithy-go"
	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
)

const efoConsumerName string = "khs-062DE8C182964A218E936DF0938F48B3"

//go:embed template.html
var outputTemplate string

type Aggregator interface {
	Name() string
	Aggregate(record *types.Record)
	Result() interface{}
}

type aggregatedResult struct {
	ShardID     string
	ChildShards []types.ChildShard
	Error       error
	Result      map[string]interface{}
}

type AggregatorBuilder func() []Aggregator

// cmd represents the cli command for analysing kinesis data streams.
type cmd struct {
	streamName        string
	kds               kds
	shardTree         map[string][]string
	limit             int
	out               string
	period            *period
	aggregatorBuilder AggregatorBuilder
}

// Start starts the execution of stream analysis workflow outlined below.
//   - Create a new EFO consumer
//   - Read all shards in the stream
//     Shards are read concurrently without loosing the order of messages
//   - Generate the output
//   - Delete EFO consumer
func (c *cmd) Start(ctx context.Context) error {
	color.Green("Stream: %s\nFrom: %v\nTo: %v", c.streamName, c.period.start, c.period.end)
	fmt.Print(color.YellowString("Creating an EFO consumer..."))
	streamArn, consumerArn, err := c.ensureEFOConsumer(ctx)
	if err != nil {
		return err
	}
	defer c.deregisterConsumer(streamArn, consumerArn)
	color.Yellow(": %s OK!\n", *consumerArn)
	fmt.Print(color.YellowString("Listing shards for stream %s...", c.streamName))
	shards, err := c.listShards(ctx, c.streamName)
	if err != nil {
		return err
	}
	color.Yellow(" OK!")
	// Store all shard ids in a map so that we can
	// quickly check if a given shard id exists in the
	// list or not.
	shardsSet := make(map[string]bool)
	for _, shard := range shards {
		shardsSet[*shard.ShardId] = true
	}
	results := make(map[string]map[string]interface{})
	resultsChan := make(chan *aggregatedResult)
	bar := pb.StartNew(len(shards))
	pendingEnumerations := 0
	for _, shard := range shards {
		// Start aggregating from shards that don't have an
		// active parent.  If there's an active parent, aggregateShard
		// would return child shards after the parent is read. It's important
		// process shards in this manner to respect the delivery order of records.
		hasActiveParent := false
		if shard.ParentShardId != nil {
			_, hasActiveParent = shardsSet[*shard.ParentShardId]
		}
		if !hasActiveParent {
			pendingEnumerations++
			go c.aggregateShard(ctx, resultsChan, *shard.ShardId, *consumerArn)
		}
	}
	for pendingEnumerations > 0 {
		r := <-resultsChan
		pendingEnumerations--
		bar.Increment()
		if r.Error == nil {
			results[r.ShardID] = r.Result
			for _, cs := range r.ChildShards {
				pendingEnumerations++
				go c.aggregateShard(ctx, resultsChan, *cs.ShardId, *consumerArn)
			}
		} else {
			err = r.Error
		}
	}
	close(resultsChan)
	bar.Finish()
	if err != nil {
		return err
	}
	fmt.Print(color.YellowString("Generating output..."))
	c.generateReport(results)
	color.Yellow("OK!")
	return nil
}

func (c *cmd) aggregateShard(ctx context.Context, resultsChan chan<- *aggregatedResult, shardID, consumerArn string) {
	var (
		continuationSequenceNumber *string
		startingPosition           *types.StartingPosition
	)
	aggregators := c.aggregatorBuilder()
	// Kinesis Subscriptions expire after 5 minutes.
	// This loop ensures that we read until end of shard.
	for {
		if continuationSequenceNumber == nil {
			startingPosition = &types.StartingPosition{
				Type:      types.ShardIteratorTypeAtTimestamp,
				Timestamp: &c.period.start,
			}
		} else {
			startingPosition = &types.StartingPosition{
				Type:           types.ShardIteratorTypeAtSequenceNumber,
				SequenceNumber: continuationSequenceNumber,
			}
		}
		subscription, err := c.kds.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
			ConsumerARN:      &consumerArn,
			ShardId:          &shardID,
			StartingPosition: startingPosition,
		})
		if err != nil {
			resultsChan <- &aggregatedResult{Error: err}
			return
		}
		subscribed := true
		for subscribed {
			select {
			case event, ok := <-subscription.GetStream().Events():
				if !ok {
					subscribed = false
				} else {
					if tevent, ok := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent); ok {
						value := tevent.Value
						continuationSequenceNumber = value.ContinuationSequenceNumber
						stop := false
						for _, r := range value.Records {
							if c.period.end.Sub(*r.ApproximateArrivalTimestamp) > 0 {
								for _, a := range aggregators {
									a.Aggregate(&r)
								}
							} else {
								stop = true
								break
							}
						}
						if continuationSequenceNumber == nil || *value.MillisBehindLatest == 0 || stop {
							r := make(map[string]interface{})
							for _, a := range aggregators {
								r[a.Name()] = a.Result()
							}
							resultsChan <- &aggregatedResult{
								ShardID:     shardID,
								ChildShards: value.ChildShards,
								Result:      r,
							}
							return
						}
					}
				}
			case <-ctx.Done():
				resultsChan <- &aggregatedResult{Error: ctx.Err()}
				return
			}
		}
	}
}

func (i *cmd) listShards(ctx context.Context, streamName string) ([]types.Shard, error) {
	var (
		lso *kinesis.ListShardsOutput
		err error
	)
	r := make([]types.Shard, 0)
	for {
		if lso == nil {
			lso, err = i.kds.ListShards(ctx, &kinesis.ListShardsInput{
				StreamName: &streamName,
			})
		} else {
			lso, err = i.kds.ListShards(ctx, &kinesis.ListShardsInput{
				NextToken: lso.NextToken,
			})
		}
		if err != nil {
			return nil, err
		}
		r = append(r, lso.Shards...)
		if lso.NextToken == nil {
			break
		}
	}
	return r, nil
}

func (i *cmd) generateReport(stats map[string]map[string]interface{}) {
	type shardStats struct {
		ShardID string                 `json:"shardId"`
		Stats   map[string]interface{} `json:"stats"`
	}
	type report struct {
		From   int          `json:"from"`
		Shards []shardStats `json:"shards"`
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
	fname := i.out
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
	fmt.Print(color.YellowString(": %s ", fname))
}

func (c *cmd) ensureEFOConsumer(ctx context.Context) (*string, *string, error) {
	stream, err := c.kds.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamName: &c.streamName,
	})
	if err != nil {
		return nil, nil, err
	}
	for {
		consumer, err := c.kds.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
			ConsumerName: aws.String(efoConsumerName),
			StreamARN:    stream.StreamDescriptionSummary.StreamARN,
		})
		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) {
				if _, ok := apiErr.(*types.ResourceNotFoundException); ok {
					_, err := c.kds.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
						ConsumerName: aws.String(efoConsumerName),
						StreamARN:    stream.StreamDescriptionSummary.StreamARN,
					})
					if err != nil {
						return nil, nil, err
					}
				} else {
					return nil, nil, err
				}
			} else {
				return nil, nil, err
			}
		} else {
			// Consumer is not available for servicing until after sometime they are created.
			// Therefore we wait until its status is Active.
			if consumer.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive {
				return stream.StreamDescriptionSummary.StreamARN, consumer.ConsumerDescription.ConsumerARN, nil
			}
			time.Sleep(time.Second * 5)
		}
	}
}

func (c *cmd) deregisterConsumer(streamArn, consumerArn *string) {
	fmt.Print(color.YellowString("Deleting EFO Consumer..."))
	_, err := c.kds.DeregisterStreamConsumer(context.Background(), &kinesis.DeregisterStreamConsumerInput{
		StreamARN:   streamArn,
		ConsumerARN: consumerArn,
	})
	if err != nil {
		color.Cyan("FAILED!")
		color.Red("%v", err)
	} else {
		color.Yellow("OK!")
	}
}

func newCMD(streamName string, kds kds, aggregatorBuilder AggregatorBuilder, limit int, p *period, out string) *cmd {
	return &cmd{
		kds:               kds,
		streamName:        streamName,
		aggregatorBuilder: aggregatorBuilder,
		limit:             limit,
		period:            p,
		shardTree:         make(map[string][]string),
		out:               out,
	}
}
