package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

func mergeJobResultChans(chans ...<-chan *JobResult) <-chan *JobResult {
	var wg sync.WaitGroup

	outchan := make(chan *JobResult, 2*len(chans))

	wg.Add(len(chans))
	for _, ch := range chans {
		go func(c <-chan *JobResult) {
			for jr := range c {
				outchan <- jr
			}
			wg.Done()
		}(ch)
	}

	go func() {
		wg.Wait()
		close(outchan)
	}()

	return outchan
}

var queryStatsFile = flag.String("query-stats-file", "",
	"Log query specific stats to CSV file. <job name, rows affected, start micros, end micros>")
var confidence = flag.Float64("confidence", 0.99, "Confidence interval.")
var updateInterval = flag.Duration("intermediate-stats-interval", 1*time.Second,
	"Show intermediate stats at this interval.")
var intermediateUpdates = flag.Bool("intermediate-stats", true, "Show intermediate stats every update-interval.")

type JobStats struct {
	StreamingStats
	RowsAffected int64
	Start        time.Duration
	Stop         time.Duration
}

func (js *JobStats) Update(jr *JobResult) {
	js.Add(float64(jr.Stop - jr.Start))
	if js.Start == 0 || jr.Start < js.Start {
		js.Start = jr.Start
	}
	if js.Stop == 0 || jr.Stop > js.Stop {
		js.Stop = jr.Stop
	}
	js.RowsAffected += jr.RowsAffected
}

func (js *JobStats) String() string {
	jsTime := js.Stop.Seconds() - js.Start.Seconds()
	return fmt.Sprintf("latency %v±%v; %d transactions (%.3f TPS); %d rows (%.3f RPS)",
		time.Duration(js.Mean()), time.Duration(js.Confidence(*confidence)),
		js.Count(), float64(js.Count())/jsTime,
		js.RowsAffected, float64(js.RowsAffected)/jsTime)
}

func processResults(config *Config, resultChan <-chan *JobResult) map[string]*JobStats {
	var resultFile *csv.Writer
	var allTestStats = make(map[string]*JobStats)
	var recentTestStats = make(map[string]*JobStats)

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
					strconv.FormatInt(jr.RowsAffected, 10),
					strconv.FormatInt(jr.Start.Nanoseconds()/1000, 10),
					strconv.FormatInt(jr.Stop.Nanoseconds()/1000, 10),
				})
			}
			if _, ok := allTestStats[jr.Name]; !ok {
				allTestStats[jr.Name] = new(JobStats)
			}
			if _, ok := recentTestStats[jr.Name]; !ok {
				recentTestStats[jr.Name] = new(JobStats)
			}

			allTestStats[jr.Name].Update(jr)
			recentTestStats[jr.Name].Update(jr)

		case <-ticker.C:
			for name, stats := range recentTestStats {
				log.Printf("%s: %v", name, stats)
			}
			recentTestStats = make(map[string]*JobStats)
		}
	}
}
