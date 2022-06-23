// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"fmt"
	"time"
)

type options struct {
	stream string
	limit  int
	cms    bool
	start  string
	end    string
	out    string
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

func (o *options) period() (*period, error) {
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
	if o.start == "" {
		period.start = period.end.Add(time.Minute * -5)
	}
	// Now that we have worked out our time range in local
	// time, convert it to UTC because Kinesis record timestamps
	// are in UTC.
	period.end = period.end.UTC()
	period.start = period.start.UTC()
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
