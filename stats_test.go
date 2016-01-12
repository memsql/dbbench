/*
 * Copyright (c) 2015-2016 by MemSQL. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
