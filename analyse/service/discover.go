// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package service

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// Discover provides a set of methods to analyse
// the attributes of a given kds stream.
type Discover struct {
	name string
	kds  KDS
}

func NewDiscover(name string, kds KDS) *Discover {
	return &Discover{
		name: name,
		kds:  kds,
	}
}

// ParentShards method returns a list of shard ids that can be considered
// as roots for processing the stream.
// ParentShards are evaluated by performing a ListShards operation and
// excluding any shards without a parent or an expired parent.
// Returns a list of parent shards and the total number of shards.
func (d *Discover) ParentShards(ctx context.Context) ([]string, int, error) {
	var (
		lso *kinesis.ListShardsOutput
		err error
	)
	shards := make([]types.Shard, 0)
	for {
		if lso == nil {
			lso, err = d.kds.ListShards(ctx, &kinesis.ListShardsInput{
				StreamName: &d.name,
			})
		} else {
			lso, err = d.kds.ListShards(ctx, &kinesis.ListShardsInput{
				NextToken: lso.NextToken,
			})
		}
		if err != nil {
			return nil, 0, err
		}
		shards = append(shards, lso.Shards...)
		if lso.NextToken == nil {
			break
		}
	}

	// Store all shard ids in a map so that we can
	// quickly check if a given shard id exists in the
	// list or not.
	shardsSet := make(map[string]bool)
	for _, shard := range shards {
		shardsSet[*shard.ShardId] = true
	}
	r := make([]string, 0)
	for _, shard := range shards {
		// We consider a shard as a parent shard if
		// it does not have a ParentShardId or
		// ParentShardId is expired
		// (therefore not returned via ListShards).
		if shard.ParentShardId == nil {
			r = append(r, *shard.ShardId)
		} else if _, ok := shardsSet[*shard.ParentShardId]; !ok {
			r = append(r, *shard.ShardId)
		}
	}

	return r, len(shards), nil
}
