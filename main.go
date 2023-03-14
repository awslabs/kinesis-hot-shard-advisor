// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

var opts = &options{}

func init() {
	flag.StringVar(&opts.stream, "stream", "", "Stream name")
	flag.IntVar(&opts.limit, "limit", 10, "Number of keys to output in key distribution graph (Optional). Default is 10.")
	flag.BoolVar(&opts.cms, "cms", false, "Use count-min-sketch (Optional) algorithm for counting key distribution (Optional). Default is false. Use this method to avoid OOM condition when analysing busy streams with high cardinality.")
	flag.StringVar(&opts.start, "from", "", "Start time in yyyy-mm-dd hh:mm format (Optional). Default value is current time - 5 minutes.")
	flag.StringVar(&opts.end, "to", "", "End time in yyyy-mm-dd hh:mm format (Optional). Default value is current time.")
	flag.StringVar(&opts.out, "out", "out.html", "Path to output file (Optional). Default is out.html.")
}

func aggregatorBuilder(p *period) func() []Aggregator {
	return func() []Aggregator {
		aggregators := make([]Aggregator, 0)
		if opts.cms {
			c, err := newCMS(5, 10000, opts.limit)
			if err != nil {
				panic(err)
			}
			aggregators = append(aggregators, c)
		} else {
			aggregators = append(aggregators, newCount())
		}
		aggregators = append(aggregators, newIngressBytes(p.start, p.end), newIngressCount(p.start, p.end))
		return aggregators
	}
}

func main() {
	var (
		err error
		ctx context.Context
	)
	flag.Parse()
	if !opts.validate() {
		os.Exit(1)
	}
	p, err := opts.period()
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
	err = newCMD(opts.stream, kinesis.NewFromConfig(cfg), aggregatorBuilder(p), opts.limit, p, opts.out).Start(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	os.Exit(0)
}
