package main

//go run . -stream=lab3 -from="2022-02-24 10:07" -to="2022-02-24 10:09"

import (
	"context"
	"flag"
	"fmt"
	"os"

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
	aggregators = append(aggregators, newIngress(p.start, p.end))
	err = newCMD(opts.stream, kinesis.NewFromConfig(cfg), aggregators, opts.limit, p).Start(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	os.Exit(0)
}
