package main

import (
	"testing"
	"time"
)

var c *cms

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

func benchmarkNewCMS(b *testing.B, hashes, space, limit int) {
	var err error
	for i := 0; i < b.N; i++ {
		c, err = newCMS("foo", nil, time.Duration(0), hashes, space, limit)
		if err != nil {
			b.FailNow()
		}
	}
}
