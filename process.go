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
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

var queryStatsFile = flag.String("query-stats-file", "",
	"Log query specific stats to CSV file. <job name, start micros, elapsed micros, rows affected>")
var confidence = flag.Float64("confidence", 0.99, "Confidence interval.")
var updateInterval = flag.Duration("intermediate-stats-interval", 1*time.Second,
	"Show intermediate stats at this interval.")
var intermediateUpdates = flag.Bool("intermediate-stats", true, "Show intermediate stats every update-interval.")

type jobStats struct {
	StreamingStats
	RowsAffected int64
	Start        time.Duration
	Stop         time.Duration
}

type JobStats struct {
	jobStats
	StreamingHistogram
}

func (js *jobStats) Update(jr *JobResult) {
	js.Add(float64(jr.Elapsed))
	if js.Start == 0 || jr.Start < js.Start {
		js.Start = jr.Start
	}
	if js.Stop == 0 || jr.Start+jr.Elapsed > js.Stop {
		js.Stop = jr.Start + jr.Elapsed
	}
	js.RowsAffected += jr.RowsAffected
}

func (js *jobStats) String() string {
	jsTime := js.Stop.Seconds() - js.Start.Seconds()
	return fmt.Sprintf("latency %v±%v; %d transactions (%.3f TPS); %d rows (%.3f RPS)",
		time.Duration(js.Mean()), time.Duration(js.Confidence(*confidence)),
		js.Count(), float64(js.Count())/jsTime,
		js.RowsAffected, float64(js.RowsAffected)/jsTime)
}

func (js *JobStats) Update(jr *JobResult) {
	js.jobStats.Update(jr)
	js.StreamingHistogram.Add(uint64(jr.Elapsed))
}

func histogramBar(buf *bytes.Buffer, count, maxCount uint64) {
	width := int(50 * 8 * float64(count) / float64(maxCount))

	// Deliberately highlight outliers
	if width == 0 && count > 0 {
		width = 1
	}
	for i := 0; i < width/8; i++ {
		buf.WriteString("█")
	}
	buf.WriteString([]string{"", "▏", "▎", "▍", "▌", "▋", "▊", "▉"}[width%8])
}

func (js *JobStats) Histogram() string {
	var buf bytes.Buffer
	buckets := js.StreamingHistogram.Buckets[:]

	var minBucket = -1
	var maxBucket = -1
	for i, b := range buckets {
		if b > 0 {
			maxBucket = i
			if minBucket < 0 {
				minBucket = i
			}
		}
	}
	maxCount := maxUint64(buckets)

	for bi, count := range buckets {
		if bi < minBucket || bi > maxBucket {
			continue
		}

		var bucketBottom, bucketTop uint64
		if bi == 0 {
			bucketBottom = 0
		} else {
			bucketBottom = 1 << uint64(bi-1)
		}
		bucketTop = 1 << uint64(bi)

		buf.WriteString(fmt.Sprintf(
			"%12v - %12v [%5d]: ",
			time.Duration(bucketBottom), time.Duration(bucketTop), count))
		histogramBar(&buf, count, maxCount)
		buf.WriteString("\n")
	}
	return buf.String()
}

func (js *JobStats) String() string {
	return js.jobStats.String() + "\n" + js.Histogram()
}

func processResults(config *Config, resultChan <-chan *JobResult) map[string]*JobStats {
	var resultFile *csv.Writer
	var allTestStats = make(map[string]*JobStats)
	var recentTestStats = make(map[string]*jobStats)

	if len(*queryStatsFile) > 0 {
		if file, err := os.Create(*queryStatsFile); err != nil {
			log.Fatalf("Could not open result file %s: %v",
				*queryStatsFile, err)
		} else {
			defer file.Close()

			resultFile = csv.NewWriter(file)
			defer resultFile.Flush()
		}
	}

	ticker := time.NewTicker(*updateInterval)
	if !*intermediateUpdates {
		ticker.Stop()
	}
	defer ticker.Stop()

	for {
		select {
		case jr, ok := <-resultChan:
			if !ok {
				return allTestStats
			}
			if resultFile != nil {
				resultFile.Write([]string{
					jr.Name,
					strconv.FormatInt(jr.Start.Nanoseconds()/1000, 10),
					strconv.FormatInt(jr.Elapsed.Nanoseconds()/1000, 10),
					strconv.FormatInt(jr.RowsAffected, 10),
				})
			}
			if _, ok := allTestStats[jr.Name]; !ok {
				allTestStats[jr.Name] = new(JobStats)
			}
			if _, ok := recentTestStats[jr.Name]; !ok {
				recentTestStats[jr.Name] = new(jobStats)
			}

			allTestStats[jr.Name].Update(jr)
			recentTestStats[jr.Name].Update(jr)

		case <-ticker.C:
			for name, stats := range recentTestStats {
				log.Printf("%s: %v", name, stats)
			}
			recentTestStats = make(map[string]*jobStats)
		}
	}
}
