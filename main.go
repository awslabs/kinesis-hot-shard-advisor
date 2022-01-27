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
}

func main() {
	var (
		since time.Duration
		err   error
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
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = newCmd(kinesis.NewFromConfig(cfg), opts.stream, since, opts.limit).Run()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	os.Exit(0)
}
