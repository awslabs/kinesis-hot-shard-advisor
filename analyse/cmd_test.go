package analyse

import (
	"context"
	"errors"
	"testing"

	"github.com/awslabs/kinesis-hot-shard-advisor/analyse/mocks"
	"github.com/awslabs/kinesis-hot-shard-advisor/analyse/service"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStartForFailureInRegisterEFOConsumer(t *testing.T) {
	// Arrange
	streamName := "test"
	ctx := context.TODO()
	e := errors.New("failed")
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	efo := mocks.NewMockefo(ctrl)
	efo.EXPECT().EnsureEFOConsumer(ctx).Return(nil, nil, e)

	cmd := newCMD(streamName, nil, nil, efo, nil, nil)

	// Act
	err := cmd.Start(ctx)

	// Assert
	assert.Equal(t, e, err)
}

func TestStartForProcessingSpecificShardIDs(t *testing.T) {
	// Arrange
	streamName := "test"
	streamArn := uuid.NewString()
	consumerArn := uuid.NewString()

	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	efo := mocks.NewMockefo(ctrl)
	efo.EXPECT().EnsureEFOConsumer(ctx).Return(&streamArn, &consumerArn, error(nil))
	efo.EXPECT().DeregisterConsumer(&streamArn, &consumerArn).Return(error(nil))

	shardIDs := []string{"a", "b"}
	po := []*service.ProcessOutput{{ShardID: "a"}, {ShardID: "b"}}
	processor := mocks.NewMockshardProcessor(ctrl)
	processor.EXPECT().Process(ctx, consumerArn, shardIDs, false, gomock.Any()).Return(po, error(nil))

	output := mocks.NewMockoutput(ctrl)
	output.EXPECT().Write(po).Return(error(nil))

	cmd := newCMD(streamName, shardIDs, nil, efo, output, processor)

	// Act
	err := cmd.Start(ctx)

	// Assert
	assert.NoError(t, err)
}

func TestStartProcessingEmptyListOfShardIDs(t *testing.T) {
	// Arrange
	streamName := "test"
	streamArn := uuid.NewString()
	consumerArn := uuid.NewString()

	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	efo := mocks.NewMockefo(ctrl)
	efo.EXPECT().EnsureEFOConsumer(ctx).Return(&streamArn, &consumerArn, error(nil))
	efo.EXPECT().DeregisterConsumer(&streamArn, &consumerArn).Return(error(nil))

	shardIDs := []string{"a", "b"}
	discover := mocks.NewMockdiscover(ctrl)
	discover.EXPECT().ParentShards(ctx).Return(shardIDs, len(shardIDs), error(nil))

	po := []*service.ProcessOutput{{ShardID: "a"}, {ShardID: "b"}}
	processor := mocks.NewMockshardProcessor(ctrl)
	processor.EXPECT().Process(ctx, consumerArn, shardIDs, true, gomock.Any()).Return(po, error(nil))

	output := mocks.NewMockoutput(ctrl)
	output.EXPECT().Write(po).Return(error(nil))

	cmd := newCMD(streamName, make([]string, 0), discover, efo, output, processor)

	// Act
	err := cmd.Start(ctx)

	// Assert
	assert.NoError(t, err)
}

func TestStartForFailureToWriteOutput(t *testing.T) {
	// Arrange
	streamName := "test"
	streamArn := uuid.NewString()
	consumerArn := uuid.NewString()

	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	efo := mocks.NewMockefo(ctrl)
	efo.EXPECT().EnsureEFOConsumer(ctx).Return(&streamArn, &consumerArn, error(nil))
	efo.EXPECT().DeregisterConsumer(&streamArn, &consumerArn).Return(error(nil))

	shardIDs := []string{"a", "b"}
	po := []*service.ProcessOutput{{ShardID: "a"}, {ShardID: "b"}}
	processor := mocks.NewMockshardProcessor(ctrl)
	processor.EXPECT().Process(ctx, consumerArn, shardIDs, false, gomock.Any()).Return(po, error(nil))

	e := errors.New("failed")
	output := mocks.NewMockoutput(ctrl)
	output.EXPECT().Write(po).Return(e)

	cmd := newCMD(streamName, shardIDs, nil, efo, output, processor)

	// Act
	err := cmd.Start(ctx)

	// Assert
	assert.Equal(t, e, err)
}
