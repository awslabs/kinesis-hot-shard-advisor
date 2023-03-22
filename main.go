// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/awslabs/kinesis-hot-shard-advisor/analyse"
	"github.com/awslabs/kinesis-hot-shard-advisor/analyse/aggregator"
)

var opts = &options{}

func init() {
	flag.StringVar(&opts.Stream, "stream", "", "Stream name")
	flag.IntVar(&opts.Limit, "limit", 10, "Number of keys to output in key distribution graph (Optional). Default is 10.")
	flag.BoolVar(&opts.CMS, "cms", false, "Use count-min-sketch (Optional) algorithm for counting key distribution (Optional). Default is false. Use this method to avoid OOM condition when analysing busy streams with high cardinality.")
	flag.StringVar(&opts.Start, "from", "", "Start time in yyyy-mm-dd hh:mm format (Optional). Default value is current time - 5 minutes.")
	flag.StringVar(&opts.End, "to", "", "End time in yyyy-mm-dd hh:mm format (Optional). Default value is current time.")
	flag.StringVar(&opts.Out, "out", "out.html", "Path to output file (Optional). Default is out.html.")
	flag.StringVar(&opts.SIDs, "shard-ids", "", "Comma separated list of shard ids to analyse.")
	flag.IntVar(&opts.Top, "top", 10, "Number of shards to emit to the report(Optional). Default is 10. Use 0 to emit all shards. Emitting all shards can result in a large file that may take a lot of system resources to view in the browser.")
}

func aggregatorBuilder(start, end time.Time) func() []analyse.Aggregator {
	return func() []analyse.Aggregator {
		aggregators := make([]analyse.Aggregator, 0)
		if opts.CMS {
			c, err := aggregator.NewCMSByKey(5, 10000, opts.Limit)
			if err != nil {
				panic(err)
			}
			aggregators = append(aggregators, c)
		} else {
			aggregators = append(aggregators, aggregator.NewCountByKey())
		}
		aggregators = append(aggregators, aggregator.NewBytesPerSecond(start, end), aggregator.NewCountPerSecond(start, end))
		return aggregators
	}
}

func main() {
	var (
		err error
		ctx context.Context
	)
	flag.Parse()
	if !opts.Validate() {
		os.Exit(1)
	}
	start, end, err := opts.Period()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	// Create a context that can be used
	// to handle SIGINT.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	go func() {
		<-signals
		cancel()
	}()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(so *retry.StandardOptions) {
			so.RateLimiter = ratelimit.NewTokenRateLimit(1000000)
		})
	}))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = analyse.NewCMD(opts.Stream, kinesis.NewFromConfig(cfg), analyse.NewHTMLReporter(opts.Out), aggregatorBuilder(start, end), opts.Limit, opts.Top, start, end, opts.ShardIDs()).Start(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	os.Exit(0)
}
