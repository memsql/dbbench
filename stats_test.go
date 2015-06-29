package main

import (
	"fmt"
	"testing"
)

func assertNear(t *testing.T, expected float64, actual float64, msg string) {
	if actual+.001 < expected || actual-.001 > expected {
		t.Error(msg, "expected", expected, "but got", actual)
	}
}

func TestNormInverseCDF(t *testing.T) {
	type testcase struct {
		p float64
		z float64
	}

	for _, testCase := range []testcase{
		{0.95, 1.645},
		{0.99, 2.326},
	} {
		assertNear(t, testCase.z, NormInverseCDF(testCase.p),
			fmt.Sprint("For", testCase.p))
	}
}

func TestStreamingStats(t *testing.T) {
	type testcase struct {
		vals   []float64
		mean   float64
		stddev float64
	}

	for _, testCase := range []testcase{
		{[]float64{1, 2}, 1.5, .707},
		{[]float64{1, 2, 3, 4, 5}, 3, 1.581},
		{[]float64{1, 1, 1}, 1, 0},
	} {
		var ss StreamingStats
		for _, v := range testCase.vals {
			ss.Add(v)
		}

		if ss.Count() != len(testCase.vals) {
			t.Error("For count of", testCase.vals,
				"expected", len(testCase.vals),
				"got", ss.Count())
		}
		assertNear(t, testCase.mean, ss.Mean(),
			fmt.Sprint("For mean of", testCase.vals))
		assertNear(t, testCase.stddev, ss.SampleStdDev(),
			fmt.Sprint("For stddev of", testCase.vals))
	}
}
