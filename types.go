// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type kds interface {
	DescribeStreamSummary(ctx context.Context, params *kinesis.DescribeStreamSummaryInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamSummaryOutput, error)
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
	SubscribeToShard(ctx context.Context, params *kinesis.SubscribeToShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SubscribeToShardOutput, error)
	DescribeStreamConsumer(ctx context.Context, params *kinesis.DescribeStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error)
	RegisterStreamConsumer(ctx context.Context, params *kinesis.RegisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error)
	DeregisterStreamConsumer(ctx context.Context, params *kinesis.DeregisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DeregisterStreamConsumerOutput, error)
}

// partitionKeyCount is used by counting aggregators
// to report the number of times a given partition key
// appears in a shard.
type partitionKeyCount struct {
	PartitionKey string `json:"partitionKey"`
	Count        int    `json:"count"`
}
