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
	"bufio"
	"context"
	"encoding/csv"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type queryInvocation struct {
	query string
	args  []interface{}
}

type jobInvocation struct {
	name    string
	queries []queryInvocation
}

type Job struct {
	Name    string
	Queries []string

	QueueDepth uint64
	Rate       float64
	Count      uint64
	BatchSize  uint64

	QueryLog     io.ReadCloser
	QueryArgs    *csv.Reader
	QueryResults *SafeCSVWriter

	Start time.Duration
	Stop  time.Duration
}

type JobResult struct {
	Name         string
	Start        time.Duration
	Elapsed      time.Duration
	Queries      int
	RowsAffected int64
	Errors       ErrorCounts
}

func (ji *jobInvocation) Invoke(db Database, df DatabaseFlavor, results *SafeCSVWriter, start time.Duration) *JobResult {
	var elapsed time.Duration
	var rowsAffected int64
	errorCounts := make(ErrorCounts)

	for _, qi := range ji.queries {
		runQueryStart := time.Now()
		rows, err := db.RunQuery(results, qi.query, qi.args)
		elapsed += time.Since(runQueryStart)

		if err != nil {
			// Attempt to handle the error
			e := errorCounts.Add(err, qi.query, df)
			if e != nil {
				// Error handling not available for this DB flavor
				log.Fatalf("%v. Error occurred while running %v:\n%v", e, ji.name, err)
			}
		} else {
			rowsAffected += rows
		}
	}

	return &JobResult{ji.name, start, elapsed, len(ji.queries), rowsAffected, errorCounts}
}

func (ji *jobInvocation) String() string {
	return quotedStruct(ji)
}

func (job *Job) String() string {
	return quotedStruct(job)
}

func (job *Job) getNextQueryArgs() ([]interface{}, error) {
	if job.QueryArgs == nil {
		return nil, nil
	}

	textArgs, err := job.QueryArgs.Read()
	if err != nil {
		if err != io.EOF {
			// TODO(awreece) Avoid log.Fatal.
			log.Fatalf("error parsing arg file for job %s: %v", job.Name, err)
		}
		return nil, err
	}

	iargs := make([]interface{}, 0, len(textArgs))
	for _, arg := range textArgs {
		iargs = append(iargs, arg)
	}
	return iargs, nil
}

func (job *Job) getNextJobInvocation() (*jobInvocation, error) {
	queryInvocations := make([]queryInvocation, 0, len(job.Queries))
	for _, query := range job.Queries {
		args, err := job.getNextQueryArgs()
		if err != nil {
			return nil, err
		}
		queryInvocations = append(queryInvocations, queryInvocation{query, args})
	}
	return &jobInvocation{job.Name, queryInvocations}, nil
}

func (job *Job) startTickQueryChannel(ctx context.Context) <-chan *jobInvocation {
	ch := make(chan *jobInvocation)
	go func() {
		defer close(ch)

		ticker := time.NewTicker(time.Duration(float64(time.Second) / job.Rate))
		defer ticker.Stop()

		for ticks := uint64(0); job.Count == 0 || ticks < job.Count; ticks++ {
			ji, err := job.getNextJobInvocation()
			if err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for bi := uint64(0); bi < job.BatchSize; bi++ {
					ch <- ji
				}
			}
		}
	}()
	return ch
}

func (job *Job) startLogQueryChannel(ctx context.Context) <-chan *jobInvocation {
	ch := make(chan *jobInvocation)
	go func() {
		defer close(ch)

		scanner := bufio.NewScanner(job.QueryLog)
		var lastTime int64

		for linesScanned := uint64(0); scanner.Scan() &&
			(job.Count == 0 || linesScanned < job.Count); linesScanned++ {
			line := scanner.Text()
			parts := strings.SplitN(line, ",", 2)
			if len(parts) != 2 {
				log.Fatalf("%s: invalid query log on line %d",
					job.Name, linesScanned+1)
			}
			if timeMicros, err := strconv.ParseInt(parts[0], 10, 64); err != nil {
				log.Fatalf("%s: error parsing query log time on line %d: %v",
					job.Name, linesScanned+1, err)
			} else {
				var timeToSleep = time.Duration(0)
				if linesScanned > 0 {
					timeToSleep = time.Duration(timeMicros-lastTime) * time.Microsecond
				}
				lastTime = timeMicros

				select {
				case <-ctx.Done():
					return
				case <-time.NewTimer(timeToSleep).C:
					// TODO(awreece) Support multi statement log files.
					ch <- &jobInvocation{job.Name, []queryInvocation{{parts[1], nil}}}
				}
			}
		}
	}()
	return ch
}

func (job *Job) startQueryChannel(ctx context.Context) <-chan *jobInvocation {
	if job.Rate > 0 {
		return job.startTickQueryChannel(ctx)
	} else if job.QueryLog != nil {
		return job.startLogQueryChannel(ctx)
	} else {
		ch := make(chan *jobInvocation)
		go func() {
			defer close(ch)
			for i := uint64(0); job.Count == 0 || i < job.Count; i++ {
				ji, err := job.getNextJobInvocation()
				if err != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case ch <- ji:
				}
			}
		}()
		return ch
	}
}

func (job *Job) runLoop(ctx context.Context, db Database, df DatabaseFlavor, startTime time.Time, results chan<- *JobResult) {
	log.Printf("starting %v", job.Name)
	defer log.Printf("stopping %v", job.Name)

	queueSem := make(chan interface{}, job.QueueDepth)
	for i := uint64(0); i < job.QueueDepth; i++ {
		queueSem <- nil
	}

	var wg sync.WaitGroup
	for ji := range job.startQueryChannel(ctx) {
		wg.Add(1)
		if job.QueueDepth > 0 {
			<-queueSem
		}
		go func(_ji *jobInvocation) {
			defer wg.Done()
			r := _ji.Invoke(db, df, job.QueryResults, time.Since(startTime))
			if job.QueueDepth > 0 {
				queueSem <- nil
			}
			results <- r
		}(ji)
	}

	// Do not return until all spawned goroutines have completed. This ensures
	// that we will not close the results chan before all spawned goroutines
	// have completed their sends on it.
	wg.Wait()
	close(queueSem)
}

func (job *Job) Run(ctx context.Context, db Database, df DatabaseFlavor, results chan<- *JobResult) {
	startTime := time.Now()

	if job.Stop > 0 {
		ctx, _ = context.WithTimeout(ctx, job.Stop)
	}

	defer job.cleanup()

	select {
	case <-ctx.Done():
		return
	case <-time.NewTimer(job.Start).C:
		job.runLoop(ctx, db, df, startTime, results)
	}
}

func (job *Job) cleanup() {
	if job.QueryResults != nil {
		job.QueryResults.Close()
	}
	if job.QueryLog != nil {
		job.QueryLog.Close()
	}
}

func makeJobResultChan(ctx context.Context, db Database, df DatabaseFlavor, jobs map[string]*Job) <-chan *JobResult {
	outChan := make(chan *JobResult)

	go func() {
		var wg sync.WaitGroup
		for _, job := range jobs {
			wg.Add(1)
			go func(j *Job) {
				j.Run(ctx, db, df, outChan)
				wg.Done()
			}(job)
		}

		wg.Wait()
		close(outChan)
	}()

	return outChan
}
