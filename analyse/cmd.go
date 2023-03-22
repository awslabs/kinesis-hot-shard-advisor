// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package analyse

import (
	"context"
	"errors"
	"fmt"
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

type aggregatedResult struct {
	ShardID     string
	ChildShards []types.ChildShard
	Error       error
	Result      map[string]interface{}
}

// CMD represents the cli command for analysing kinesis data streams.
type CMD struct {
	streamName        string
	kds               KDS
	reporter          Reporter
	limit             int
	start             time.Time
	end               time.Time
	aggregatorBuilder AggregatorBuilder
	shardIDs          []string
}

// Start starts the execution of stream analysis workflow outlined below.
//   - Create a new EFO consumer
//   - Read all shards in the stream
//     Shards are read concurrently without loosing the order of messages
//   - Generate the output
//   - Delete EFO consumer
func (c *CMD) Start(ctx context.Context) error {
	var bar *pb.ProgressBar
	color.Green("Stream: %s\nFrom: %v\nTo: %v", c.streamName, c.start, c.end)
	fmt.Print(color.YellowString("Creating an EFO consumer..."))
	streamArn, consumerArn, err := c.ensureEFOConsumer(ctx)
	if err != nil {
		return err
	}
	defer c.deregisterConsumer(streamArn, consumerArn)
	color.Yellow(": %s OK!\n", *consumerArn)
	resultsChan := make(chan *aggregatedResult)
	pendingEnumerations := 0
	if len(c.shardIDs) > 0 {
		bar = pb.StartNew(len(c.shardIDs))
		for _, shard := range c.shardIDs {
			pendingEnumerations++
			go c.aggregateShard(ctx, resultsChan, shard, *consumerArn)
		}
	} else {
		fmt.Print(color.YellowString("Listing shards for stream %s...", c.streamName))
		shards, err := c.listShards(ctx, c.streamName)
		if err != nil {
			return err
		}
		color.Yellow(" OK!")
		bar = pb.StartNew(len(shards))
		// Store all shard ids in a map so that we can
		// quickly check if a given shard id exists in the
		// list or not.
		shardsSet := make(map[string]bool)
		for _, shard := range shards {
			shardsSet[*shard.ShardId] = true
		}
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
	}

	results := make(map[string]map[string]interface{})
	for pendingEnumerations > 0 {
		r := <-resultsChan
		pendingEnumerations--
		bar.Increment()
		if r.Error == nil {
			results[r.ShardID] = r.Result
			if len(c.shardIDs) == 0 {
				for _, cs := range r.ChildShards {
					pendingEnumerations++
					go c.aggregateShard(ctx, resultsChan, *cs.ShardId, *consumerArn)
				}
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
	err = c.reporter.Report(c.start, results, c.limit)
	if err != nil {
		panic(err)
	}
	color.Yellow("OK!")
	return nil
}

func (c *CMD) aggregateShard(ctx context.Context, resultsChan chan<- *aggregatedResult, shardID, consumerArn string) {
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
				Timestamp: &c.start,
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
							if c.end.Sub(*r.ApproximateArrivalTimestamp) > 0 {
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

func (i *CMD) listShards(ctx context.Context, streamName string) ([]types.Shard, error) {
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

func (c *CMD) ensureEFOConsumer(ctx context.Context) (*string, *string, error) {
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

func (c *CMD) deregisterConsumer(streamArn, consumerArn *string) {
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

func NewCMD(streamName string, kds KDS, reporter Reporter, aggregatorBuilder AggregatorBuilder, limit int, start, end time.Time, shardIDs []string) *CMD {
	return &CMD{
		kds:               kds,
		reporter:          reporter,
		streamName:        streamName,
		aggregatorBuilder: aggregatorBuilder,
		limit:             limit,
		start:             start,
		end:               end,
		shardIDs:          shardIDs,
	}
}
