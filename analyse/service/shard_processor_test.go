package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/awslabs/kinesis-hot-shard-advisor/analyse/service/mocks"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_aggregateAll(t *testing.T) {
	type testCase struct {
		Name              string
		EnumerateChildren bool
		ShardIDs          []string
		ChildShards       map[string][]types.ChildShard
		Expect            []string
	}

	cases := []testCase{
		{Name: "No shards with children and enumeration is required", ShardIDs: []string{"a", "b"}, EnumerateChildren: true, Expect: []string{"a", "b"}},
		{Name: "No shards with children and enumeration is not required", ShardIDs: []string{"a", "b"}, EnumerateChildren: false, Expect: []string{"a", "b"}},
		{Name: "Shards with children and enumeration is required", ShardIDs: []string{"a", "b"}, EnumerateChildren: true, ChildShards: map[string][]types.ChildShard{"a": {{ShardId: aws.String("c"), ParentShards: []string{"a"}}, {ShardId: aws.String("d"), ParentShards: []string{"a"}}}}, Expect: []string{"a", "b", "c", "d"}},
		{Name: "Shards with children and enumeration is not required", ShardIDs: []string{"a", "b"}, EnumerateChildren: false, ChildShards: map[string][]types.ChildShard{"a": {{ShardId: aws.String("c"), ParentShards: []string{"a"}}, {ShardId: aws.String("d"), ParentShards: []string{"a"}}}}, Expect: []string{"a", "b"}},
		{Name: "Shards with a merged child and enumeration is required", ShardIDs: []string{"a", "b"}, EnumerateChildren: true, ChildShards: map[string][]types.ChildShard{"a": {{ShardId: aws.String("c"), ParentShards: []string{"a", "b"}}}, "b": {{ShardId: aws.String("c"), ParentShards: []string{"a", "b"}}}}, Expect: []string{"a", "b", "c"}},
	}

	for i, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			// Arrange
			ctx := context.TODO()
			consumerArn := uuid.NewString()
			end := time.Now()
			start := end.Add(time.Minute * -1)

			p := NewShardProcessor(nil, nil, start, end, 1)
			tc := cases[i]

			// Act
			output, err := p.aggregateAll(ctx, consumerArn, tc.ShardIDs, c.EnumerateChildren, func() {}, func(ctx context.Context, c chan<- *ProcessOutput, shardID, ca string) {
				assert.Equal(t, consumerArn, ca)
				output := &ProcessOutput{ShardID: shardID}
				if cs, ok := tc.ChildShards[shardID]; ok {
					output.childShards = cs
				}
				c <- output
			})

			// Assert
			sids := make([]string, 0)
			for _, o := range output {
				sids = append(sids, o.ShardID)
			}
			assert.NoError(t, err)
			assert.ElementsMatch(t, tc.Expect, sids)
		})
	}
}

func Test_aggregateAllWhenAggregatorFails(t *testing.T) {
	// Arrange
	ctx := context.TODO()
	consumerArn := uuid.NewString()
	end := time.Now()
	start := end.Add(time.Minute * -1)
	shardIDs := []string{"a", "b"}
	e := errors.New("failed")

	p := NewShardProcessor(nil, nil, start, end, 1)

	// Act
	output, err := p.aggregateAll(ctx, consumerArn, shardIDs, false, func() {}, func(ctx context.Context, c chan<- *ProcessOutput, shardID, ca string) {
		c <- &ProcessOutput{err: e}
	})

	// Assert
	assert.Equal(t, e, err)
	assert.Empty(t, output)
}

func Test_aggregateAllWhenAggregatorFailsForSomeShards(t *testing.T) {
	// Arrange
	ctx := context.TODO()
	consumerArn := uuid.NewString()
	end := time.Now()
	start := end.Add(time.Minute * -1)
	shardIDs := []string{"a", "b"}
	e := errors.New("failed")

	p := NewShardProcessor(nil, nil, start, end, 1)

	// Act
	output, err := p.aggregateAll(ctx, consumerArn, shardIDs, false, func() {}, func(ctx context.Context, c chan<- *ProcessOutput, shardID, ca string) {
		if shardID == "a" {
			c <- &ProcessOutput{
				ShardID:     shardID,
				Aggregators: make([]Aggregator, 0),
			}
		} else {
			c <- &ProcessOutput{err: e}
		}
	})

	// Assert
	assert.Equal(t, e, err)
	assert.Equal(t, "a", output[0].ShardID)
	assert.Len(t, output, 1)
}

func Test_aggregateShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type testCase struct {
		Name     string
		Events   []types.SubscribeToShardEvent
		Expected []string
	}

	consumerArn := uuid.NewString()
	shardID := uuid.NewString()
	end := time.Now()
	start := end.Add(time.Minute * -1)

	cases := []testCase{
		{
			Name: "Should aggregate all records arrived before end time",
			Events: []types.SubscribeToShardEvent{
				{
					Records: []types.Record{
						{PartitionKey: aws.String("1"), ApproximateArrivalTimestamp: aws.Time(start.Add(time.Second))},
						{PartitionKey: aws.String("2"), ApproximateArrivalTimestamp: aws.Time(start.Add(time.Second * 2))},
						{PartitionKey: aws.String("3"), ApproximateArrivalTimestamp: aws.Time(end)},
						{PartitionKey: aws.String("4"), ApproximateArrivalTimestamp: aws.Time(end.Add(1))},
					},
				},
			},
			Expected: []string{"1", "2"},
		},
		{
			Name: "Should aggregate all records in multiple events arrived before end time",
			Events: []types.SubscribeToShardEvent{
				{
					Records: []types.Record{
						{PartitionKey: aws.String("1"), ApproximateArrivalTimestamp: aws.Time(start.Add(time.Second))},
						{PartitionKey: aws.String("2"), ApproximateArrivalTimestamp: aws.Time(start.Add(time.Second * 2))},
					},
					ContinuationSequenceNumber: aws.String(uuid.NewString()),
					MillisBehindLatest:         aws.Int64(1),
				},
				{
					Records: []types.Record{
						{PartitionKey: aws.String("3"), ApproximateArrivalTimestamp: aws.Time(start.Add(time.Second * 3))},
						{PartitionKey: aws.String("4"), ApproximateArrivalTimestamp: aws.Time(end)},
						{PartitionKey: aws.String("5"), ApproximateArrivalTimestamp: aws.Time(end.Add(1))},
					},
				},
			},
			Expected: []string{"1", "2", "3"},
		},
		{
			Name: "Should stop if event does contains the latest record",
			Events: []types.SubscribeToShardEvent{
				{
					Records: []types.Record{
						{PartitionKey: aws.String("1"), ApproximateArrivalTimestamp: aws.Time(start.Add(time.Second))},
						{PartitionKey: aws.String("2"), ApproximateArrivalTimestamp: aws.Time(start.Add(time.Second * 2))},
					},
					ContinuationSequenceNumber: aws.String(uuid.NewString()),
					MillisBehindLatest:         aws.Int64(0),
				},
			},
			Expected: []string{"1", "2"},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			// Arrange
			ctx := context.TODO()
			subscribeToShardOutput := &kinesis.SubscribeToShardOutput{}
			stream := make(chan types.SubscribeToShardEventStream, 1)
			results := make(chan *ProcessOutput)
			aggregatedRecords := make([]string, 0)
			aggregator := mocks.NewMockAggregator(ctrl)
			aggregator.EXPECT().Aggregate(gomock.Any()).AnyTimes().Do(func(record *types.Record) {
				aggregatedRecords = append(aggregatedRecords, *record.PartitionKey)
			})

			kds := mocks.NewMockKDS(ctrl)
			kds.EXPECT().
				SubscribeToShard(ctx, gomock.Any()).
				DoAndReturn(func(ctx context.Context, input *kinesis.SubscribeToShardInput, optFns ...func(kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
					assert.Equal(t, shardID, *input.ShardId)
					assert.Equal(t, consumerArn, *input.ConsumerARN)
					return subscribeToShardOutput, nil
				})

			p := NewShardProcessor(kds, func() []Aggregator { return []Aggregator{aggregator} }, start, end, 1)
			p.streamExtractor = func(stso *kinesis.SubscribeToShardOutput) <-chan types.SubscribeToShardEventStream {
				assert.Same(t, subscribeToShardOutput, stso)
				return stream
			}
			p.streamCloser = func(stso *kinesis.SubscribeToShardOutput) {
				assert.Same(t, subscribeToShardOutput, stso)
			}

			// Act
			go p.aggregateShard(ctx, results, shardID, consumerArn)
			for _, event := range c.Events {
				stream <- &types.SubscribeToShardEventStreamMemberSubscribeToShardEvent{
					Value: event,
				}
			}
			o := <-results

			// Assert
			assert.Equal(t, shardID, o.ShardID)
			assert.ElementsMatch(t, c.Expected, aggregatedRecords)
		})
	}
}

func Test_aggregateShardWhenSubscriptionExpiresBeforeAnyRecords(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Arrange
	shardID := uuid.NewString()
	consumerArn := uuid.NewString()
	end := time.Now()
	start := end.Add(time.Minute * -1)
	ctx := context.TODO()
	subscription1 := &kinesis.SubscribeToShardOutput{}
	subscription2 := &kinesis.SubscribeToShardOutput{}
	stream1 := make(chan types.SubscribeToShardEventStream, 1)
	stream2 := make(chan types.SubscribeToShardEventStream, 1)
	results := make(chan *ProcessOutput)

	kds := mocks.NewMockKDS(ctrl)
	kds.EXPECT().
		SubscribeToShard(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *kinesis.SubscribeToShardInput, optFns ...func(kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
			assert.Equal(t, shardID, *input.ShardId)
			assert.Equal(t, consumerArn, *input.ConsumerARN)
			assert.Equal(t, start, *input.StartingPosition.Timestamp)
			assert.Equal(t, types.ShardIteratorTypeAtTimestamp, input.StartingPosition.Type)
			return subscription1, nil
		})
	kds.EXPECT().
		SubscribeToShard(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *kinesis.SubscribeToShardInput, optFns ...func(kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
			assert.Equal(t, shardID, *input.ShardId)
			assert.Equal(t, consumerArn, *input.ConsumerARN)
			assert.Equal(t, start, *input.StartingPosition.Timestamp)
			assert.Equal(t, types.ShardIteratorTypeAtTimestamp, input.StartingPosition.Type)
			return subscription2, nil
		})

	extractorOutput := []chan types.SubscribeToShardEventStream{stream1, stream2}
	extractorInput := []*kinesis.SubscribeToShardOutput{subscription1, subscription2}
	closerInput := []*kinesis.SubscribeToShardOutput{subscription2, subscription1}
	p := NewShardProcessor(kds, func() []Aggregator { return []Aggregator{} }, start, end, 1)
	p.streamExtractor = func(stso *kinesis.SubscribeToShardOutput) <-chan types.SubscribeToShardEventStream {
		assert.Same(t, extractorInput[0], stso)
		s := extractorOutput[0]
		extractorInput = extractorInput[1:]
		extractorOutput = extractorOutput[1:]
		return s
	}
	p.streamCloser = func(stso *kinesis.SubscribeToShardOutput) {
		assert.Same(t, closerInput[0], stso)
		closerInput = closerInput[1:]
	}

	// Act
	go p.aggregateShard(ctx, results, shardID, consumerArn)
	close(stream1)
	stream2 <- &types.SubscribeToShardEventStreamMemberSubscribeToShardEvent{
		Value: types.SubscribeToShardEvent{
			Records: []types.Record{{PartitionKey: aws.String("a"), ApproximateArrivalTimestamp: &start}},
		},
	}
	o := <-results

	// Assert
	assert.Equal(t, shardID, o.ShardID)
	assert.NoError(t, o.err)
	assert.Empty(t, extractorInput) // Ensure all calls to extractor have been made
	assert.Empty(t, closerInput)    // Ensure that all stream handles disposed
}

func Test_aggregateShardWhenSubscriptionExpiresAfterReadingSomeRecords(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Arrange
	shardID := uuid.NewString()
	consumerArn := uuid.NewString()
	continuationSequenceNumber := uuid.NewString()
	end := time.Now()
	start := end.Add(time.Minute * -1)
	ctx := context.TODO()
	subscription1 := &kinesis.SubscribeToShardOutput{}
	subscription2 := &kinesis.SubscribeToShardOutput{}
	stream1 := make(chan types.SubscribeToShardEventStream, 1)
	stream2 := make(chan types.SubscribeToShardEventStream, 1)
	results := make(chan *ProcessOutput)

	kds := mocks.NewMockKDS(ctrl)
	kds.EXPECT().
		SubscribeToShard(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *kinesis.SubscribeToShardInput, optFns ...func(kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
			assert.Equal(t, shardID, *input.ShardId)
			assert.Equal(t, consumerArn, *input.ConsumerARN)
			assert.Equal(t, start, *input.StartingPosition.Timestamp)
			assert.Equal(t, types.ShardIteratorTypeAtTimestamp, input.StartingPosition.Type)
			return subscription1, nil
		})
	kds.EXPECT().
		SubscribeToShard(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *kinesis.SubscribeToShardInput, optFns ...func(kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
			assert.Equal(t, shardID, *input.ShardId)
			assert.Equal(t, consumerArn, *input.ConsumerARN)
			assert.Equal(t, continuationSequenceNumber, *input.StartingPosition.SequenceNumber)
			assert.Equal(t, types.ShardIteratorTypeAtSequenceNumber, input.StartingPosition.Type)
			return subscription2, nil
		})

	extractorOutput := []chan types.SubscribeToShardEventStream{stream1, stream2}
	extractorInput := []*kinesis.SubscribeToShardOutput{subscription1, subscription2}
	closerInput := []*kinesis.SubscribeToShardOutput{subscription2, subscription1}
	p := NewShardProcessor(kds, func() []Aggregator { return []Aggregator{} }, start, end, 1)
	p.streamExtractor = func(stso *kinesis.SubscribeToShardOutput) <-chan types.SubscribeToShardEventStream {
		assert.Same(t, extractorInput[0], stso)
		s := extractorOutput[0]
		extractorInput = extractorInput[1:]
		extractorOutput = extractorOutput[1:]
		return s
	}
	p.streamCloser = func(stso *kinesis.SubscribeToShardOutput) {
		assert.Same(t, closerInput[0], stso)
		closerInput = closerInput[1:]
	}

	// Act
	go p.aggregateShard(ctx, results, shardID, consumerArn)
	stream1 <- &types.SubscribeToShardEventStreamMemberSubscribeToShardEvent{
		Value: types.SubscribeToShardEvent{
			Records:                    []types.Record{{PartitionKey: aws.String("a"), ApproximateArrivalTimestamp: &start}},
			ContinuationSequenceNumber: &continuationSequenceNumber,
			MillisBehindLatest:         aws.Int64(1),
		},
	}
	close(stream1)
	stream2 <- &types.SubscribeToShardEventStreamMemberSubscribeToShardEvent{
		Value: types.SubscribeToShardEvent{
			Records: []types.Record{{PartitionKey: aws.String("b"), ApproximateArrivalTimestamp: &start}},
		},
	}
	o := <-results

	// Assert
	assert.Equal(t, shardID, o.ShardID)
	assert.NoError(t, o.err)
	assert.Empty(t, extractorInput) // Ensure all calls to extractor have been made
	assert.Empty(t, closerInput)    // Ensure that all streams handles are disposed
}

func Test_aggregateShardWhenSubscriptionFailsOnContinuationAfterExpiry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Arrange
	shardID := uuid.NewString()
	consumerArn := uuid.NewString()
	continuationSequenceNumber := uuid.NewString()
	end := time.Now()
	start := end.Add(time.Minute * -1)
	e := errors.New("failed")
	ctx := context.TODO()
	subscription := &kinesis.SubscribeToShardOutput{}
	stream := make(chan types.SubscribeToShardEventStream, 1)
	results := make(chan *ProcessOutput)

	kds := mocks.NewMockKDS(ctrl)
	kds.EXPECT().
		SubscribeToShard(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *kinesis.SubscribeToShardInput, optFns ...func(kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
			assert.Equal(t, shardID, *input.ShardId)
			assert.Equal(t, consumerArn, *input.ConsumerARN)
			assert.Equal(t, start, *input.StartingPosition.Timestamp)
			assert.Equal(t, types.ShardIteratorTypeAtTimestamp, input.StartingPosition.Type)
			return subscription, nil
		})
	kds.EXPECT().
		SubscribeToShard(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *kinesis.SubscribeToShardInput, optFns ...func(kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
			assert.Equal(t, shardID, *input.ShardId)
			assert.Equal(t, consumerArn, *input.ConsumerARN)
			assert.Equal(t, continuationSequenceNumber, *input.StartingPosition.SequenceNumber)
			assert.Equal(t, types.ShardIteratorTypeAtSequenceNumber, input.StartingPosition.Type)
			return nil, e
		})

	p := NewShardProcessor(kds, func() []Aggregator { return []Aggregator{} }, start, end, 1)
	p.streamExtractor = func(stso *kinesis.SubscribeToShardOutput) <-chan types.SubscribeToShardEventStream {
		assert.Same(t, subscription, stso)
		return stream
	}
	p.streamCloser = func(stso *kinesis.SubscribeToShardOutput) {
		assert.Same(t, subscription, stso)
	}

	// Act
	go p.aggregateShard(ctx, results, shardID, consumerArn)
	stream <- &types.SubscribeToShardEventStreamMemberSubscribeToShardEvent{
		Value: types.SubscribeToShardEvent{
			Records:                    []types.Record{{PartitionKey: aws.String("a"), ApproximateArrivalTimestamp: &start}},
			ContinuationSequenceNumber: &continuationSequenceNumber,
			MillisBehindLatest:         aws.Int64(1),
		},
	}
	close(stream)

	o := <-results

	// Assert
	assert.Equal(t, e, o.err)
}
