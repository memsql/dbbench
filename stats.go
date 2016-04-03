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
	"flag"
	"math"
	"math/rand"
)

var maxSampleCount = flag.Int64("max-sample-count", 10000, "Samples to keep when streaming.")

type StreamingSample struct {
	count   int
	samples []float64
}

func (ss *StreamingSample) Add(x float64) {
	if ss.count == 0 {
		ss.samples = make([]float64, 0, *maxSampleCount)
	}

	if ss.count < cap(ss.samples) {
		ss.samples = append(ss.samples, x)
	} else {
		index := int(rand.Int31n(int32(ss.count + 1)))
		if index < cap(ss.samples) {
			ss.samples[index] = x
		}
	}
	ss.count += 1
}

func (ss *StreamingSample) Count() int {
	return ss.count
}

func (ss *StreamingSample) Samples() []float64 {
	return ss.samples
}

func (ss *StreamingSample) Histogram(nBucketsMax int) (buckets []int, minV float64, maxV float64, extra int) {
	if ss.count == 0 {
		panic("Cannot compute histogram of empty sample.")
	}
	if nBucketsMax <= 0 {
		panic("Cannot compute histogram with <=0 buckets.")
	}

	minV = minf(ss.samples)
	maxV = maxf(ss.samples)
	diff := maxV - minV

	if diff > 0.0 {
		buckets = make([]int, nBucketsMax)
		for _, v := range ss.samples {
			buckets[int((v-minV)/diff*float64(nBucketsMax-1))]++
		}
	} else {
		buckets = make([]int, 1)
		buckets[0] = len(ss.samples)
	}

	return buckets, minV, maxV, ss.count - len(ss.samples)
}

/*
 * Use Welfords Method to compute variance in a stream.
 */
type StreamingStats struct {
	count              int
	mean               float64
	sumSquareDeviation float64
}

func (ss *StreamingStats) Add(x float64) {
	if ss.count == 0 {
		ss.mean = x
	} else {
		/*
		 * According to Welfords method,
		 *
		 *    M_k = M_{k-1} + (x_k - M_{k-1}) / k
		 *    S_k = S_{k-1} + (x_k - M_{k-1})*(x_k - M_k)
		 */
		oldMean := ss.mean
		ss.mean += (x - ss.mean) / float64(ss.count+1)
		ss.sumSquareDeviation += (x - oldMean) * (x - ss.mean)
	}
	ss.count++
}

func (ss *StreamingStats) Count() int {
	return ss.count
}

func (ss *StreamingStats) Mean() float64 {
	return ss.mean
}

func (ss *StreamingStats) Confidence(alpha float64) float64 {
	if ss.count < 30 {
		// XXX Use students t-distribution for small samples.
		return 0
	}

	z_alpha := NormInverseCDF(1 - ((1 - alpha) / 2))

	return z_alpha * ss.SampleStdDev() / math.Sqrt(float64(ss.count))

}

func (ss *StreamingStats) SampleStdDev() float64 {
	return math.Sqrt(ss.SampleVariance())
}

func (ss *StreamingStats) SampleVariance() float64 {
	if ss.count > 1 {
		// ss.count - 1 for sample variance.
		return ss.sumSquareDeviation / float64(ss.count-1)
	} else {
		return 0
	}
}

/*
 * Modified from the author's original bc code by Alex Reece
 * (awreece@gmail.com) on Jul 2, 2015. For information about
 * the algorithm, see http://home.online.no/~pjacklam/notes/invnorm/
 * Original comment reproduced below.
 *
 * Lower tail quantile for standard normal distribution function.
 * This function returns an approximation of the inverse cumulative
 * standard normal distribution function.  I.e., given P, it returns
 * an approximation to the X satisfying P = Pr{Z <= X} where Z is a
 * random variable from the standard normal distribution.
 *
 * The algorithm uses a minimax approximation by rational functions
 * and the result has a relative error whose absolute value is less
 * than 1.15e-9.
 *
 * Author:      Peter John Acklam
 * Time-stamp:  2005-03-10 14:13:52 +01:00
 * E-mail:      pjacklam@online.no
 * WWW URL:     http://home.online.no/~pjacklam
 */
func NormInverseCDF(p float64) float64 {
	q := p - 0.5

	a := q
	if a < 0 {
		a = -a
	}

	if a <= .47575 {
		/* Rational approximation for central region. */

		r := q * q
		z := (((((-39.69683028665376*r+220.9460984245205)*r-
			275.9285104469687)*r+138.3577518672690)*r-
			30.66479806614716)*r + 2.506628277459239) * q /
			(((((-54.47609879822406*r+161.5858368580409)*r-
				155.6989798598866)*r+66.80131188771972)*r-
				13.28068155288572)*r + 1)

		return (z)
	} else {
		/* Rational approximation for tails. */

		/* If in upper tail, map to lower tail. */
		if q > 0 {
			p = 1 - p
		}

		r := math.Sqrt(-2 * math.Log(p))
		z := (((((-0.007784894002430293*r-0.3223964580411365)*r-
			2.400758277161838)*r-2.549732539343734)*r+
			4.374664141464968)*r + 2.938163982698783) /
			((((0.007784695709041462*r+0.3224671290700398)*r+
				2.445134137142996)*r+3.754408661907416)*r +
				1)

		/* If in upper tail, swap sign. */
		if q > 0 {
			z = -z
		}

		return (z)
	}
}
