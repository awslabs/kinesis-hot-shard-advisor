package main

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

var c *cms

func TestCMS(t *testing.T) {
	c, err := newCMS(5, 5, 5)
	if err != nil {
		t.FailNow()
	}
	keys := []string{"a", "b", "c", "d", "e"}
	c.Aggregate(&types.Record{PartitionKey: aws.String("a")})
	c.Aggregate(&types.Record{PartitionKey: aws.String("b")})
	c.Aggregate(&types.Record{PartitionKey: aws.String("c")})
	c.Aggregate(&types.Record{PartitionKey: aws.String("d")})
	c.Aggregate(&types.Record{PartitionKey: aws.String("e")})
	c.Aggregate(&types.Record{PartitionKey: aws.String("a")})
	r := c.Result().([]record)
	assert.Equal(t, "a", r[0].PartitionKey)
	for _, r := range r {
		assert.True(t, slices.Contains(keys, r.PartitionKey))
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
	a, err := newCMS(5, 10000, 100)
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
		c, err = newCMS(hashes, space, limit)
		if err != nil {
			b.FailNow()
		}
	}
}
