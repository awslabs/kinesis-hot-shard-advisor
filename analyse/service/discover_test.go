package service

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/awslabs/kinesis-hot-shard-advisor/analyse/service/mocks"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestParentShardsForNonPagedShardList(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	streamName := uuid.NewString()

	ctx := context.TODO()
	kds := mocks.NewMockKDS(ctrl)
	kds.
		EXPECT().
		ListShards(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			assert.Equal(t, streamName, *input.StreamName)
			assert.Nil(t, input.NextToken)
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{{ShardId: aws.String("a")}, {ShardId: aws.String("b")}},
			}, nil
		})

	d := NewDiscover(streamName, kds)

	// Act
	shards, count, err := d.ParentShards(ctx)

	// Assert
	assert.Equal(t, []string{"a", "b"}, shards)
	assert.Equal(t, 2, count)
	assert.NoError(t, err)
}

func TestParentShardsForPagedShardList(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	streamName := uuid.NewString()
	nextToken := uuid.NewString()
	ctx := context.TODO()
	kds := mocks.NewMockKDS(ctrl)
	kds.
		EXPECT().
		ListShards(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			assert.Equal(t, streamName, *input.StreamName)
			assert.Nil(t, input.NextToken)
			return &kinesis.ListShardsOutput{
				Shards:    []types.Shard{{ShardId: aws.String("a")}, {ShardId: aws.String("b")}},
				NextToken: &nextToken,
			}, nil
		})
	kds.
		EXPECT().
		ListShards(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			assert.Nil(t, input.StreamName)
			assert.Equal(t, nextToken, *input.NextToken)
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{{ShardId: aws.String("c")}, {ShardId: aws.String("d")}},
			}, nil
		})
	d := NewDiscover(streamName, kds)

	// Act
	shards, count, err := d.ParentShards(ctx)

	// Assert
	assert.Equal(t, []string{"a", "b", "c", "d"}, shards)
	assert.Equal(t, 4, count)
	assert.NoError(t, err)
}

func TestParentShardsForShardsWithParents(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	streamName := uuid.NewString()

	ctx := context.TODO()
	kds := mocks.NewMockKDS(ctrl)
	kds.
		EXPECT().
		ListShards(ctx, gomock.Any()).
		Return(&kinesis.ListShardsOutput{Shards: []types.Shard{{ShardId: aws.String("a")}, {ShardId: aws.String("b"), ParentShardId: aws.String("a")}}}, error(nil))

	d := NewDiscover(streamName, kds)

	// Act
	shards, count, err := d.ParentShards(ctx)

	// Assert
	assert.Equal(t, []string{"a"}, shards)
	assert.Equal(t, 2, count)
	assert.NoError(t, err)
}

func TestParentShardsForShardsWithExpiredParents(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	streamName := uuid.NewString()

	ctx := context.TODO()
	kds := mocks.NewMockKDS(ctrl)
	kds.
		EXPECT().
		ListShards(ctx, gomock.Any()).
		Return(&kinesis.ListShardsOutput{Shards: []types.Shard{{ShardId: aws.String("b"), ParentShardId: aws.String("a")}}}, error(nil))

	d := NewDiscover(streamName, kds)

	// Act
	shards, count, err := d.ParentShards(ctx)

	// Assert
	assert.Equal(t, []string{"b"}, shards)
	assert.Equal(t, 1, count)
	assert.NoError(t, err)
}

func TestParentShardsWhenListShardsErrorBeforePaging(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	streamName := uuid.NewString()
	e := errors.New("failed")

	ctx := context.TODO()
	kds := mocks.NewMockKDS(ctrl)
	kds.
		EXPECT().
		ListShards(ctx, gomock.Any()).
		Return(nil, e)

	d := NewDiscover(streamName, kds)

	// Act
	shards, count, err := d.ParentShards(ctx)

	// Assert
	assert.Nil(t, shards)
	assert.Equal(t, 0, count)
	assert.Equal(t, e, err)
}

func TestParentShardsWhenListShardsErrorAfterPaging(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	streamName := uuid.NewString()
	e := errors.New("failed")
	nextToken := uuid.NewString()

	ctx := context.TODO()
	kds := mocks.NewMockKDS(ctrl)
	kds.
		EXPECT().
		ListShards(ctx, gomock.Any()).
		Return(&kinesis.ListShardsOutput{Shards: []types.Shard{{ShardId: aws.String("a")}}, NextToken: &nextToken}, nil)
	kds.
		EXPECT().
		ListShards(ctx, gomock.Any()).
		Return(nil, e)

	d := NewDiscover(streamName, kds)

	// Act
	shards, count, err := d.ParentShards(ctx)

	// Assert
	assert.Nil(t, shards)
	assert.Equal(t, 0, count)
	assert.Equal(t, e, err)
}
