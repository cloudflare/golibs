// Copyright (c) 2014 CloudFlare, Inc.

package ewma

import (
	"testing"
	"time"
)

type testTupleRate struct {
	packet bool
	delay  float64
	cur    float64
}

var testVectorRate = [][]testTupleRate{
	// Sanity check (half life is 1 second)
	{
		// Feeding packets every second gets to 1 pps eventually
		{false, 1, 0},
		{true, 1, 0},
		{true, 1, 0.5},
		{true, 1, 0.75},
		{true, 1, 0.875},
		{true, 1, 0.9375},
		{true, 1, 0.96875},
		{true, 1, 0.984375},
		{true, 1, 0.9921875},
		{true, 1, 0.99609375},
		{true, 1, 0.998046875},

		// Stop over 30 seconds
		{false, 10, 0.1008769989013672},
		{false, 10, 0.05000090412795544},
		{false, 10, 0.033333334231792834},

		// A small number in few minutes
		{false, 1000, 0.000970873786407767},
	},

	// Burst of 10, 1ms apart, gets us to ~7 pps
	{
		{true, 1, 0},
		{true, 0.001, -1}, {true, 0.001, -1}, {true, 0.001, -1}, {true, 0.001, -1}, {true, 0.001, -1},
		{true, 0.001, -1}, {true, 0.001, -1}, {true, 0.001, -1}, {true, 0.001, -1}, {true, 0.001, -1},
		{false, 0, 6.9075045629642595},
		{false, 1, 3.9537522814821298},
		{false, 1, 2.101876140741065},
	},

	// 10 packets 100ms apart, get 5 pps
	{
		{true, 1, 0},
		{true, 0.1, -1}, {true, 0.1, -1}, {true, 0.1, -1}, {true, 0.1, -1}, {true, 0.1, -1},
		{true, 0.1, -1}, {true, 0.1, -1}, {true, 0.1, -1}, {true, 0.1, -1}, {true, 0.1, -1},
		{false, 0, 5.000000000000002},
		{false, 1, 3.000000000000001},
		{false, 1, 1.6250000000000004},
	},
}

func TestRate(t *testing.T) {
	for testNo, test := range testVectorRate {
		ts := time.Now()
		e := NewEwmaRate(time.Duration(1 * time.Second))

		for lineNo, l := range test {
			ts = ts.Add(time.Duration(l.delay * float64(time.Second.Nanoseconds())))
			if l.packet {
				e.Update(ts)
			}
			if l.cur != -1 && e.Current(ts) != l.cur {
				t.Errorf("Test %d, line %d: %v != %v",
					testNo, lineNo, e.Current(ts), l.cur)
			}
		}
	}
}

func TestRateCoverErrors(t *testing.T) {
	e := NewEwmaRate(time.Duration(1 * time.Second))

	if e.CurrentNow() != 0 {
		t.Error("expecting 0")
	}

	e.UpdateNow()
	rate := e.CurrentNow()
	if !(rate > 0.1 && rate < 0.8) {
		// depending on the speed of the CPU
		t.Error("expecting 0", e.CurrentNow())
	}

}
