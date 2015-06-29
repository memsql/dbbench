package main

import (
	"bufio"
	"code.google.com/p/go.net/context"
	"database/sql"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (job *Job) startTickQueryChannel(ctx context.Context) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)

		ticker := time.NewTicker(time.Duration(float64(time.Second) / job.Rate))
		defer ticker.Stop()

		for ticks := 0; job.Count == 0 || ticks < job.Count; ticks++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ch <- job.Query
			}
		}
	}()
	return ch
}

func (job *Job) startLogQueryChannel(ctx context.Context) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)

		scanner := bufio.NewScanner(job.QueryLog)
		var lastTime int64

		for linesScanned := 0; scanner.Scan() &&
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
					ch <- parts[1]
				}
			}
		}
	}()
	return ch
}

func (job *Job) startQueryChannel(ctx context.Context) <-chan string {
	if job.Rate > 0 {
		return job.startTickQueryChannel(ctx)
	} else if job.QueryLog != nil {
		return job.startLogQueryChannel(ctx)
	} else {
		ch := make(chan string)
		go func() {
			defer close(ch)
			for i := 0; job.Count == 0 || i < job.Count; i++ {
				select {
				case <-ctx.Done():
					return
				case ch <- job.Query:
				}
			}
		}()
		return ch
	}
}

func (job *Job) StartResultChan(ctx context.Context, db *sql.DB) <-chan JobResult {
	jobStart := time.Now()
	results := make(chan JobResult)

	if job.Stop > 0 {
		ctx, _ = context.WithTimeout(ctx, job.Stop)
	}

	time.AfterFunc(job.Start, func() {
		log.Printf("starting %v", job.Name)
		defer log.Printf("stopping %v", job.Name)

		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(results)
		}()

		queueSem := make(chan interface{}, job.QueueDepth)
		for i := 0; i < job.QueueDepth; i++ {
			queueSem <- nil
		}
		defer func() {
			wg.Wait()
			close(queueSem)
		}()

		for query := range job.startQueryChannel(ctx) {
			wg.Add(1)
			if job.QueueDepth > 0 {
				<-queueSem
			}
			go func(q string) {
				results <- TimeQuery(db, time.Since(jobStart), job.Name, q)
				if job.QueueDepth > 0 {
					queueSem <- nil
				}
				wg.Done()
			}(query)
		}
	})
	return results
}
