package main

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
)

func TestCount(t *testing.T) {
	type w struct {
		key   string
		count int
	}
	testCases := []struct {
		name   string
		writes []w
		expect []partitionKeyCount
	}{
		{"should count writes by key", []w{{"a", 2}, {"b", 10}}, []partitionKeyCount{{"a", 2}, {"b", 10}}},
	}
	for _, testCase := range testCases {
		c := newCount()
		for _, write := range testCase.writes {
			for i := 0; i < write.count; i++ {
				c.Aggregate(&types.Record{
					PartitionKey:                aws.String(write.key),
					ApproximateArrivalTimestamp: aws.Time(time.Now()),
				})
			}
		}
		r := c.Result().([]partitionKeyCount)
		assert.Equal(t, len(testCase.expect), len(r))
		m := make(map[string]int)
		for _, i := range r {
			m[i.PartitionKey] = i.Count
		}
		for _, expect := range testCase.expect {
			assert.Equal(t, expect.Count, m[expect.PartitionKey])
		}
	}
}
