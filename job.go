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
	"bufio"
	"encoding/csv"
	"golang.org/x/net/context"
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

	QueryLog  io.Reader
	QueryArgs *csv.Reader

	Start time.Duration
	Stop  time.Duration
}

type JobResult struct {
	Name         string
	Start        time.Duration
	Elapsed      time.Duration
	RowsAffected int64
}

func (ji *jobInvocation) Invoke(db Database, start time.Duration) *JobResult {
	invokeStart := time.Now()
	var rowsAffected int64

	for _, qi := range ji.queries {
		rows, err := db.RunQuery(qi.query, qi.args)
		if err != nil {
			// TODO(awreece) Avoid log.Fatal.
			log.Fatalf("error for query %s in %s: %v", qi.query, ji.name, err)
		}
		rowsAffected += rows
	}

	stop := time.Now()
	elapsed := stop.Sub(invokeStart)

	return &JobResult{ji.name, start, elapsed, rowsAffected}
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

func (job *Job) runLoop(ctx context.Context, db Database, startTime time.Time, results chan<- *JobResult) {
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
			r := _ji.Invoke(db, time.Since(startTime))
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

func (job *Job) Run(ctx context.Context, db Database, results chan<- *JobResult) {
	startTime := time.Now()

	if job.Stop > 0 {
		ctx, _ = context.WithTimeout(ctx, job.Stop)
	}

	select {
	case <-ctx.Done():
		return
	case <-time.NewTimer(job.Start).C:
		job.runLoop(ctx, db, startTime, results)
	}
}

func makeJobResultChan(ctx context.Context, db Database, jobs map[string]*Job) <-chan *JobResult {
	outChan := make(chan *JobResult)

	go func() {
		var wg sync.WaitGroup
		for _, job := range jobs {
			wg.Add(1)
			go func(j *Job) {
				j.Run(ctx, db, outChan)
				wg.Done()
			}(job)
		}

		wg.Wait()
		close(outChan)
	}()

	return outChan
}
