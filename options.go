// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type options struct {
	Stream string
	Limit  int
	CMS    bool
	Start  string
	End    string
	Out    string
	SIDs   string
}

func (o *options) Validate() bool {
	if o.Stream == "" {
		fmt.Println("stream name is required")
		return false
	}
	return true
}

func (o *options) Period() (time.Time, time.Time, error) {
	var (
		start, end time.Time
		err        error
	)
	if o.Start != "" {
		start, err = o.parseTime(o.Start)
		if err != nil {
			return start, end, err
		}
	}
	if o.End != "" {
		end, err = o.parseTime(o.End)
		if err != nil {
			return start, end, err
		}
	} else {
		end = time.Now()
	}
	if o.Start == "" {
		start = end.Add(time.Minute * -5)
	}
	// Now that we have worked out our time range in local
	// time, convert it to UTC because Kinesis record timestamps
	// are in UTC.
	end = end.UTC()
	start = start.UTC()
	if end.Sub(start) <= 0 {
		return start, end, errors.New("end time must be greater than start time")
	}
	return start, end, nil
}

func (o *options) parseTime(s string) (time.Time, error) {
	d, err := time.ParseDuration(o.Start)
	if err != nil {

		t, err := time.ParseInLocation("2006-01-02 15:04", s, time.Local)
		if err != nil {
			return time.Time{}, err
		}
		return t, nil
	}
	return time.Now().Add(d * -1), nil
}

func (o *options) ShardIDs() []string {
	if o.SIDs == "" {
		return make([]string, 0)
	}
	r := strings.Split(o.SIDs, ",")
	for i, s := range r {
		r[i] = strings.TrimSpace(s)
	}
	return r
}
