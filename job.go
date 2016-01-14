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
	"database/sql"
	"golang.org/x/net/context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (ji *JobInvocation) runQuery(db *sql.DB, query string) int64 {
	var rowsAffected int64
	switch strings.ToLower(strings.Fields(query)[0]) {
	case "select", "show", "explain", "describe", "desc":
		rows, err := db.Query(query)
		if err != nil {
			log.Fatalf("error for query %s in %s: %v", query, ji.Name, err)
		}
		defer rows.Close()
		for rows.Next() {
			rowsAffected++
		}
		if err = rows.Err(); err != nil {
			log.Fatalf("error for query %s in %s: %v", query, ji.Name, err)
		}
	default:
		res, err := db.Exec(query)
		if err != nil {
			log.Fatalf("error for query %s in %s: %v", query, ji.Name, err)
		}
		rowsAffected, _ = res.RowsAffected()
	}
	return rowsAffected
}

func (ji *JobInvocation) Invoke(db *sql.DB, start time.Duration) *JobResult {
	invokeStart := time.Now()
	var rowsAffected int64

	for _, query := range ji.Queries {
		rowsAffected += ji.runQuery(db, query)
	}

	stop := time.Now()
	elapsed := stop.Sub(invokeStart)

	return &JobResult{ji.Name, start, elapsed, rowsAffected}
}

func (job *Job) startTickQueryChannel(ctx context.Context) <-chan *JobInvocation {
	ji := &JobInvocation{job.Name, job.Queries}
	ch := make(chan *JobInvocation)
	go func() {
		defer close(ch)

		ticker := time.NewTicker(time.Duration(float64(time.Second) / job.Rate))
		defer ticker.Stop()

		for ticks := uint64(0); job.Count == 0 || ticks < job.Count; ticks++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ch <- ji
			}
		}
	}()
	return ch
}

func (job *Job) startLogQueryChannel(ctx context.Context) <-chan *JobInvocation {
	ch := make(chan *JobInvocation)
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
					ch <- &JobInvocation{job.Name, []string{parts[1]}}
				}
			}
		}
	}()
	return ch
}

func (job *Job) startQueryChannel(ctx context.Context) <-chan *JobInvocation {
	if job.Rate > 0 {
		return job.startTickQueryChannel(ctx)
	} else if job.QueryLog != nil {
		return job.startLogQueryChannel(ctx)
	} else {
		ji := &JobInvocation{job.Name, job.Queries}
		ch := make(chan *JobInvocation)
		go func() {
			defer close(ch)
			for i := uint64(0); job.Count == 0 || i < job.Count; i++ {
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

func (job *Job) runLoop(ctx context.Context, db *sql.DB, startTime time.Time, results chan<- *JobResult) {
	log.Printf("starting %v", job.Name)
	defer log.Printf("stopping %v", job.Name)

	queueSem := make(chan interface{}, job.QueueDepth)
	for i := uint64(0); i < job.QueueDepth; i++ {
		queueSem <- nil
	}

	var wg sync.WaitGroup
	for jobInvocation := range job.startQueryChannel(ctx) {
		wg.Add(1)
		if job.QueueDepth > 0 {
			<-queueSem
		}
		go func(ji *JobInvocation) {
			defer wg.Done()
			r := ji.Invoke(db, time.Since(startTime))
			if job.QueueDepth > 0 {
				queueSem <- nil
			}
			results <- r
		}(jobInvocation)
	}

	// Do not return until all spawned goroutines have completed. This ensures
	// that we will not close the results chan before all spawned goroutines
	// have completed their sends on it.
	wg.Wait()
}

func (job *Job) Run(ctx context.Context, db *sql.DB, results chan<- *JobResult) {
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

func makeJobResultChan(ctx context.Context, db *sql.DB, jobs map[string]*Job) <-chan *JobResult {
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
