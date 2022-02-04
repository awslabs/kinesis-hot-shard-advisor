package main

//go run . -stream "lab3"

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type options struct {
	stream string
	since  string
	limit  int
	cms    bool
}

func (o *options) validate() bool {
	if o.stream == "" {
		fmt.Println("stream name is required")
		return false
	}
	return true
}

var opts = &options{}

func init() {
	flag.StringVar(&opts.stream, "stream", "", "stream name")
	flag.StringVar(&opts.since, "since", "", "analyse the stream since a specific point in time")
	flag.IntVar(&opts.limit, "limit", 10, "max number of keys to display")
	flag.BoolVar(&opts.cms, "cms", false, "use count-min-sketch (experimental)")
}

func main() {
	var (
		since time.Duration
		err   error
		ctx   context.Context
	)
	flag.Parse()
	if !opts.validate() {
		os.Exit(1)
	}
	if opts.since != "" {
		since, err = time.ParseDuration(opts.since)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	ctx = context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	aggregators := make([]Aggregator, 0)
	if opts.cms {
		cms, err := newCMS(10, 1000000, opts.limit)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		aggregators = append(aggregators, cms)
	} else {
		aggregators = append(aggregators, newCount())
	}
	err = newCMD(opts.stream, kinesis.NewFromConfig(cfg), aggregators, opts.limit, since).Start(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	os.Exit(0)
}
