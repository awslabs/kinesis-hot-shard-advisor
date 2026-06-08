// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// tolerance absorbs the wall-clock drift between when Period() captures
// time.Now() and when the test computes its own reference time.
const tolerance = 5 * time.Second

// assertClose asserts that want and got are within tolerance of each other.
func assertClose(t *testing.T, want, got time.Time, msg string) {
	t.Helper()
	diff := got.Sub(want)
	if diff < 0 {
		diff = -diff
	}
	assert.LessOrEqualf(t, diff, tolerance,
		"%s: want %s, got %s (diff %s exceeds tolerance %s)", msg, want, got, diff, tolerance)
}

func TestPeriod(t *testing.T) {
	const layout = "2006-01-02 15:04:05"

	testCases := []struct {
		name      string
		start     string
		end       string
		expectErr bool
		// check is invoked only when expectErr is false. now is captured
		// just before Period() runs so relative-duration expectations can
		// be computed against the same wall clock (within tolerance).
		check func(t *testing.T, now, start, end time.Time)
	}{
		{
			name: "absolute from and absolute to produce that exact UTC window",
			// Local-time inputs; Period() converts to UTC.
			start: "2024-01-01 00:00:00",
			end:   "2024-01-02 00:00:00",
			check: func(t *testing.T, now, start, end time.Time) {
				wantStart, _ := time.ParseInLocation(layout, "2024-01-01 00:00:00", time.Local)
				wantEnd, _ := time.ParseInLocation(layout, "2024-01-02 00:00:00", time.Local)
				assert.True(t, start.Equal(wantStart.UTC()), "start should equal the parsed -from in UTC")
				assert.True(t, end.Equal(wantEnd.UTC()), "end should equal the parsed -to in UTC")
			},
		},
		{
			name:  "relative from alone yields start=now-24h and end=now",
			start: "24h",
			end:   "",
			check: func(t *testing.T, now, start, end time.Time) {
				assertClose(t, now.Add(-24*time.Hour).UTC(), start, "start should be now-24h")
				assertClose(t, now.UTC(), end, "end should default to now")
			},
		},
		{
			// Bug-exposing case: parseTime must honour its own argument.
			// -from=24h and -to=1h must yield end=now-1h (a 23h window),
			// NOT end derived from -from's 24h duration.
			name:  "relative from and relative to honour their own durations",
			start: "24h",
			end:   "1h",
			check: func(t *testing.T, now, start, end time.Time) {
				assertClose(t, now.Add(-24*time.Hour).UTC(), start, "start should be now-24h (from -from)")
				assertClose(t, now.Add(-1*time.Hour).UTC(), end, "end should be now-1h (from -to), not now-24h")
			},
		},
		{
			name:      "end at or before start is an error",
			start:     "2024-01-02 00:00:00",
			end:       "2024-01-01 00:00:00",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			o := &options{Start: tc.start, End: tc.end}
			now := time.Now()
			start, end, err := o.Period()
			if tc.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			tc.check(t, now, start, end)
		})
	}
}
