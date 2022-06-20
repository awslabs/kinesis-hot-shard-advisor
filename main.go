// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

var opts = &options{}

func init() {
	flag.StringVar(&opts.stream, "stream", "", "stream name")
	flag.IntVar(&opts.limit, "limit", 10, "max number of keys to display")
	flag.BoolVar(&opts.cms, "cms", false, "use count-min-sketch (experimental)")
	flag.StringVar(&opts.start, "from", "", "start time for analysis")
	flag.StringVar(&opts.end, "to", "", "end time for analysis")
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
		aggregators = append(aggregators, newIngress(p.start, p.end))
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
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = newCMD(opts.stream, kinesis.NewFromConfig(cfg), aggregatorBuilder(p), opts.limit, p).Start(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	os.Exit(0)
}
