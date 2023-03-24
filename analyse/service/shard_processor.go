// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package service

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type ProcessOutput struct {
	ShardID     string
	childShards []types.ChildShard
	err         error
	Aggregators []Aggregator
}

type ShardProcessor struct {
	kds               KDS
	aggregatorBuilder AggregatorBuilder
	start             time.Time
	end               time.Time
	streamExtractor   streamExtractor
}

func NewShardProcessor(kds KDS, aggregatorBuilder AggregatorBuilder, start, end time.Time) *ShardProcessor {
	return &ShardProcessor{
		kds:               kds,
		aggregatorBuilder: aggregatorBuilder,
		start:             start,
		end:               end,
		streamExtractor: func(stso *kinesis.SubscribeToShardOutput) <-chan types.SubscribeToShardEventStream {
			return stso.GetStream().Events()
		},
	}
}

func (p *ShardProcessor) Process(ctx context.Context, consumerArn string, parentShardIDs []string, children bool, progress func()) ([]*ProcessOutput, error) {
	return p.aggregateAll(ctx, consumerArn, parentShardIDs, children, progress, p.aggregateShard)
}

func (p *ShardProcessor) aggregateAll(ctx context.Context, consumerArn string, parentShardIDs []string, children bool, progress func(), reader shardReader) ([]*ProcessOutput, error) {
	var err error
	resultsChan := make(chan *ProcessOutput)
	pendingEnumerations := len(parentShardIDs)
	aggregatedShards := make([]*ProcessOutput, 0)
	aggregatedChildren := make(map[string]bool)

	for _, shardID := range parentShardIDs {
		go reader(ctx, resultsChan, shardID, consumerArn)
	}

	for pendingEnumerations > 0 {
		r := <-resultsChan
		pendingEnumerations--
		progress()
		if r.err == nil {
			aggregatedShards = append(aggregatedShards, r)
			if children {
				for _, cs := range r.childShards {
					if _, ok := aggregatedChildren[*cs.ShardId]; !ok {
						aggregatedChildren[*cs.ShardId] = true
						pendingEnumerations++
						go reader(ctx, resultsChan, *cs.ShardId, consumerArn)
					}
				}
			}
		} else {
			err = r.err
		}
	}

	close(resultsChan)

	if err != nil {
		return nil, err
	}
	return aggregatedShards, nil
}

func (p *ShardProcessor) aggregateShard(ctx context.Context, resultsChan chan<- *ProcessOutput, shardID, consumerArn string) {
	var (
		continuationSequenceNumber *string
		startingPosition           *types.StartingPosition
	)
	aggregators := p.aggregatorBuilder()
	// Kinesis Subscriptions expire after 5 minutes.
	// This loop ensures that we read until end of shard.
	for {
		if continuationSequenceNumber == nil {
			startingPosition = &types.StartingPosition{
				Type:      types.ShardIteratorTypeAtTimestamp,
				Timestamp: &p.start,
			}
		} else {
			startingPosition = &types.StartingPosition{
				Type:           types.ShardIteratorTypeAtSequenceNumber,
				SequenceNumber: continuationSequenceNumber,
			}
		}
		subscription, err := p.kds.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
			ConsumerARN:      &consumerArn,
			ShardId:          &shardID,
			StartingPosition: startingPosition,
		})
		if err != nil {
			resultsChan <- &ProcessOutput{err: err}
			return
		}
		stream := p.streamExtractor(subscription)
		subscribed := true
		for subscribed {
			select {
			case event, ok := <-stream:
				if !ok {
					subscribed = false
				} else {
					if tevent, ok := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent); ok {
						value := tevent.Value
						continuationSequenceNumber = value.ContinuationSequenceNumber
						stop := false
						for _, r := range value.Records {
							if p.end.Sub(*r.ApproximateArrivalTimestamp) > 0 {
								for _, a := range aggregators {
									a.Aggregate(&r)
								}
							} else {
								stop = true
								break
							}
						}
						if continuationSequenceNumber == nil || *value.MillisBehindLatest == 0 || stop {
							resultsChan <- &ProcessOutput{
								ShardID:     shardID,
								childShards: value.ChildShards,
								Aggregators: aggregators,
							}
							return
						}
					}
				}
			case <-ctx.Done():
				resultsChan <- &ProcessOutput{err: ctx.Err()}
				return
			}
		}
	}
}

type shardReader func(context.Context, chan<- *ProcessOutput, string, string)
type streamExtractor func(*kinesis.SubscribeToShardOutput) <-chan types.SubscribeToShardEventStream
