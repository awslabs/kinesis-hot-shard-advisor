// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aggregator

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

var c *CMSByKey

func TestCMSByKey(t *testing.T) {
	type input struct {
		key   string
		count int
	}
	testCases := []struct {
		given []input
		want  []string
	}{
		{[]input{{"a", 5}}, []string{"a", ""}},
		{[]input{{"a", 5}, {"b", 6}}, []string{"b", "a"}},
		{[]input{{"a", 5}, {"b", 6}, {"a", 1}}, []string{"a", "b"}},
		{[]input{{"a", 5}, {"b", 6}, {"c", 1}}, []string{"a", "b"}},
	}

	for _, tc := range testCases {
		cmsAggregator, err := NewCMSByKey(5, 5, 2)
		if err != nil {
			t.FailNow()
		}
		for _, g := range tc.given {
			for i := 0; i < g.count; i++ {
				cmsAggregator.Aggregate(&types.Record{PartitionKey: &g.key})
			}
		}
		r := cmsAggregator.Result().([]partitionKeyCount)
		keys := make([]string, 0)
		for _, k := range r {
			keys = append(keys, k.PartitionKey)
		}
		assert.ElementsMatch(t, tc.want, keys)
		assert.Greater(t, r[0].Count, 1)
	}
}

func BenchmarkNewCMS10x10x10(b *testing.B) {
	benchmarkNewCMS(b, 10, 10, 10)
}

func BenchmarkNewCMS10x1000x10(b *testing.B) {
	benchmarkNewCMS(b, 10, 1000, 10)
}

func BenchmarkNewCMS10x10000x10(b *testing.B) {
	benchmarkNewCMS(b, 10, 10000, 10)
}

func BenchmarkNewCMS10x1000000x10(b *testing.B) {
	benchmarkNewCMS(b, 10, 1000000, 10)
}

func BenchmarkUniqueKeys1M(b *testing.B) {
	benchmarkUniqueKeys(b, 1000000)
}

func benchmarkUniqueKeys(b *testing.B, count int) {
	a, err := NewCMSByKey(5, 10000, 100)
	if err != nil {
		b.FailNow()
	}
	for i := 0; i < b.N; i++ {
		for j := 0; j < count; j++ {
			a.Aggregate(&types.Record{
				PartitionKey: aws.String(fmt.Sprintf("%v", uuid.New())),
			})
		}
	}
}

func benchmarkNewCMS(b *testing.B, hashes, space, limit int) {
	var err error
	for i := 0; i < b.N; i++ {
		c, err = NewCMSByKey(hashes, space, limit)
		if err != nil {
			b.FailNow()
		}
	}
}
