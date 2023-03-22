// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package aggregator

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
)

func TestRecordsPerSecond(t *testing.T) {
	end := time.Now()
	start := time.Now().Add(time.Second * -3)
	type w struct {
		arrivalSecond int
		data          []byte
	}
	cases := []struct {
		name       string
		writes     []w
		sum        int
		max        int
		timeSeries []int
	}{
		{"should include first and last second", []w{{0, []byte{1}}, {3, []byte{1}}}, 2, 1, []int{1, 0, 0, 1}},
		{"should aggregate writes per second", []w{{0, []byte{1}}, {0, []byte{1}}, {1, []byte{1}}}, 3, 2, []int{2, 1, 0, 0}},
		{"should ignore writes outside the bounds", []w{{-1, []byte{1}}, {0, []byte{1}}, {4, []byte{1}}}, 1, 1, []int{1, 0, 0, 0}},
	}
	for _, testCase := range cases {
		c := NewCountPerSecond(start, end)
		for _, write := range testCase.writes {
			c.Aggregate(&types.Record{
				PartitionKey:                aws.String("a"),
				Data:                        write.data,
				ApproximateArrivalTimestamp: aws.Time(start.Add(time.Second * time.Duration(write.arrivalSecond))),
			})
		}
		r := c.Result().(IngressCountStats)
		assert.Equal(t, testCase.max, r.Max, testCase.name)
		assert.Equal(t, testCase.sum, r.Sum, testCase.name)
		assert.Equal(t, testCase.timeSeries, r.TimeSeries, testCase.name)
	}
}
