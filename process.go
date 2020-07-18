/*
 * Copyright (c) 2015-2020 by MemSQL. All rights reserved.
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
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

var confidence = flag.Float64("confidence", 0.99, "Confidence interval.")
var updateInterval = flag.Duration("intermediate-stats-interval", 1*time.Second,
	"Show intermediate stats at this interval.")
var intermediateUpdates = flag.Bool("intermediate-stats", true, "Show intermediate stats every update-interval.")

/*
 * We use a FileFlagValue so that the query-stats-file is opened when we
 * first parse the flags (i.e. before we change our base directory).
 */
var queryStatsFile WriteFileFlagValue

func init() {
	flag.Var(&queryStatsFile, "query-stats-file",
		"Log query specific stats to CSV file. <job name, start micros, elapsed micros, rows affected>")
}

type jobStats struct {
	Transactions   StreamingStats
	Errors         StreamingStats
	Queries        uint64
	RowsAffected   int64
	TotalErrors    uint64
	AcceptedErrors uint64
	Start          time.Duration
	Stop           time.Duration
}

type JobStats struct {
	jobStats
	Transactions StreamingHistogram
	Errors       StreamingHistogram
}

func (js *jobStats) Update(config *Config, jr *JobResult) {
	js.AcceptedErrors += jr.Errors.TotalAccepted(config.Flavor, config.AcceptedErrors)
	if totalErrors := jr.Errors.TotalErrors(); totalErrors > 0 {
		// TODO(msilver): why do we have both? it appears the concept of "transaction" within dbbench maps to one end to
		// end execution of a job, even if that job contains multiple queries (this is only possible with the
		// multi-query-mode option, which is used rarely). This is incredibly misleading, because row count sums
		// across all queries in a job, yet we report "transactions per second (TPS)" which is really more like
		// "jobs per second".
		js.TotalErrors += totalErrors      // actual number of errors
		js.Errors.Add(float64(jr.Elapsed)) // number of jobs that caused errors
	} else {
		// Only count transactions that succeed
		js.RowsAffected += jr.RowsAffected
		js.Transactions.Add(float64(jr.Elapsed))
	}
	js.Queries += uint64(jr.Queries)
	if js.Start == 0 || jr.Start < js.Start {
		js.Start = jr.Start
	}
	if js.Stop == 0 || jr.Start+jr.Elapsed > js.Stop {
		js.Stop = jr.Start + jr.Elapsed
	}
}

func (js *jobStats) String() string {
	jsTime := js.Stop.Seconds() - js.Start.Seconds()
	return fmt.Sprintf("%d transactions (%.3f TPS), latency %v±%v; %d rows (%.3f RPS), %d queries (%.3f QPS); %d aborts (%.3f%%), latency %v±%v",
		js.Transactions.Count(), float64(js.Transactions.Count())/jsTime,
		time.Duration(js.Transactions.Mean()), time.Duration(js.Transactions.Confidence(*confidence)),
		js.RowsAffected, float64(js.RowsAffected)/jsTime,
		js.Queries, float64(js.Queries)/jsTime,
		// TODO(msilver) see above re inconsistent counting methods. Should we divide by js.Transactions.Count() instead?
		js.TotalErrors, 100*float64(js.TotalErrors)/float64(js.Queries),
		time.Duration(js.Errors.Mean()), time.Duration(js.Errors.Confidence(*confidence)))
}

func (js *JobStats) Update(config *Config, jr *JobResult) {
	unhandledErrors := jr.Errors.UnhandledErrors(config.Flavor, config.AcceptedErrors)
	if len(unhandledErrors) > 0 {
		log.Fatalf("Unexpected errors while running %v:\n%v", jr.Name, unhandledErrors)
	}
	js.jobStats.Update(config, jr)
	if jr.Errors.TotalErrors() == 0 {
		js.Transactions.Add(uint64(jr.Elapsed))
	} else {
		js.Errors.Add(uint64(jr.Elapsed))
	}
}

func (js *JobStats) String() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("%v\nTransactions:\n%v", js.jobStats.String(), js.Transactions.Histogram()))
	if abortHistogram := js.Errors.Histogram(); len(abortHistogram) > 0 {
		str.WriteString(fmt.Sprintf("Aborts:\n%v", abortHistogram))
	}
	return str.String()
}

func processResults(config *Config, resultChan <-chan *JobResult) map[string]*JobStats {
	var resultFile *csv.Writer
	var allTestStats = make(map[string]*JobStats)
	var recentTestStats = make(map[string]*jobStats)

	if queryStatsFile.GetFile() != nil {
		defer queryStatsFile.GetFile().Close()
		resultFile = csv.NewWriter(queryStatsFile.GetFile())
		defer resultFile.Flush()
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
					strconv.FormatUint(jr.Errors.TotalErrors(), 10),
				})
			}
			if _, ok := allTestStats[jr.Name]; !ok {
				allTestStats[jr.Name] = new(JobStats)
			}
			if _, ok := recentTestStats[jr.Name]; !ok {
				recentTestStats[jr.Name] = new(jobStats)
			}

			allTestStats[jr.Name].Update(config, jr)
			recentTestStats[jr.Name].Update(config, jr)

		case <-ticker.C:
			for name, stats := range recentTestStats {
				log.Printf("%s: %v", name, stats)
			}
			recentTestStats = make(map[string]*jobStats)
		}
	}
}
