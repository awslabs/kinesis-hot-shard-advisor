package main

//go run . -stream=lab3 -from="2022-02-24 10:07" -to="2022-02-24 10:09"

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
	limit  int
	cms    bool
	start  string
	end    string
}

type period struct {
	start time.Time
	end   time.Time
}

func (o *options) validate() bool {
	if o.stream == "" {
		fmt.Println("stream name is required")
		return false
	}
	return true
}

func (o *options) parseStartAndEndTime() (*period, error) {
	var (
		period period
		err    error
	)
	if o.start != "" {
		period.start, err = o.parseTime(o.start)
		if err != nil {
			return nil, err
		}

	}

	if o.end != "" {
		period.end, err = o.parseTime(o.end)
		if err != nil {
			return nil, err
		}
	} else {
		period.end = time.Now()
	}
	period.start = period.start.Round(time.Second).UTC() //converting it to UTC as service understand utc time format
	period.end = period.end.Round(time.Second).UTC()
	//fmt.Println("end time:", period.end)
	return &period, nil
}

func (o *options) parseTime(s string) (time.Time, error) {
	d, err := time.ParseDuration(o.start)
	if err != nil {

		t, err := time.ParseInLocation("2006-01-02 15:04", s, time.Local)
		if err != nil {
			return time.Time{}, err
		}
		return t, nil
	}
	return time.Now().Add(d * -1), nil
}

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
	p, err := opts.parseStartAndEndTime()
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
