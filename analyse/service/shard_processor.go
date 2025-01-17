// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package service

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/pkg/errors"
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
	streamCloser      streamCloser
	maxWorkers        int
}

func NewShardProcessor(kds KDS, aggregatorBuilder AggregatorBuilder, start, end time.Time, maxWorkers int) *ShardProcessor {
	return &ShardProcessor{
		kds:               kds,
		aggregatorBuilder: aggregatorBuilder,
		start:             start,
		end:               end,
		streamExtractor: func(stso *kinesis.SubscribeToShardOutput) <-chan types.SubscribeToShardEventStream {
			return stso.GetStream().Events()
		},
		streamCloser: func(stso *kinesis.SubscribeToShardOutput) {
			// Discard the error because there's nothing
			// we can do about it.
			stso.GetStream().Close()
		},
		maxWorkers: maxWorkers,
	}
}

func (p *ShardProcessor) Process(ctx context.Context, consumerArn string, parentShardIDs []string, children bool, progress func()) ([]*ProcessOutput, error) {
	return p.aggregateAll(ctx, consumerArn, parentShardIDs, children, progress, p.aggregateShard)
}

func (p *ShardProcessor) aggregateAll(ctx context.Context, consumerArn string, parentShardIDs []string, children bool, progress func(), reader shardReader) ([]*ProcessOutput, error) {
	var err error
	resultsChan := make(chan *ProcessOutput)
	pendingEnumerations := len(parentShardIDs)
	output := make([]*ProcessOutput, 0)
	seenShardIDs := make(map[string]bool)
	scheduler := newAsyncScheduler(p.maxWorkers)
	numFailedShards := 0

	for _, shardID := range parentShardIDs {
		shardID := shardID
		seenShardIDs[shardID] = true
		scheduler.Go(func() {
			reader(ctx, resultsChan, shardID, consumerArn)
		})
	}

	for pendingEnumerations > 0 {
		r := <-resultsChan
		pendingEnumerations--
		progress()
		if r.err == nil {
			output = append(output, r)
			if children {
				for _, cs := range r.childShards {
					shardID := *cs.ShardId
					if _, ok := seenShardIDs[shardID]; !ok {
						seenShardIDs[shardID] = true
						pendingEnumerations++
						scheduler.Go(func() {
							reader(ctx, resultsChan, shardID, consumerArn)
						})
					}
				}
			}
		} else {
			err = r.err
			numFailedShards++
		}
	}

	close(resultsChan)
	if err != nil {
		err = errors.WithMessagef(err, "%d shards failed", numFailedShards)
	}
	return output, err
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
		subscription, err := p.subscribeToShardWithRetry(ctx, consumerArn, shardID, startingPosition)
		if err != nil {
			resultsChan <- &ProcessOutput{err: err}
			return
		}
		stream := p.streamExtractor(subscription)
		defer p.streamCloser(subscription)
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

// subscribeToShardWithRetry method calls KDS SubscribeToShard with a simple
// retry mechanism. Retry mechanism is in place to handle rare cases where
// KDS responds to S2S API with a ResourceInUseException even after its status
// has changed to Active.
// When this happens we wait for some seconds and try again.
// Retry is done only once because this condition should not last for too long.
func (p *ShardProcessor) subscribeToShardWithRetry(ctx context.Context, consumerArn, shardID string, startingPosition *types.StartingPosition) (*kinesis.SubscribeToShardOutput, error) {
	retried := false
	for {
		subscription, err := p.kds.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
			ConsumerARN:      &consumerArn,
			ShardId:          &shardID,
			StartingPosition: startingPosition,
		})
		if err != nil {
			var riu *types.ResourceInUseException
			if !errors.As(err, &riu) || retried {
				return subscription, err
			}
			retried = true
			time.Sleep(time.Second)
			continue
		}
		return subscription, err
	}
}

type shardReader func(context.Context, chan<- *ProcessOutput, string, string)
type streamExtractor func(*kinesis.SubscribeToShardOutput) <-chan types.SubscribeToShardEventStream
type streamCloser func(*kinesis.SubscribeToShardOutput)
