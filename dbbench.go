package main

import (
	"code.google.com/p/go.net/context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type Job struct {
	Name string

	Query      string
	QueueDepth int
	Rate       float64
	Count      int

	QueryLog io.Reader

	Start time.Duration
	Stop  time.Duration
}

type QueryLogRecord struct {
	Start time.Time
	Query string
}

type Config struct {
	Duration time.Duration
	Setup    []string
	Teardown []string
	Jobs     map[string]*Job
}

type JobResult struct {
	Name         string
	Start        time.Duration
	Stop         time.Duration
	RowsAffected int64
	Duration     time.Duration
}

func TimeQuery(db *sql.DB, jobStart time.Duration, name string, query string) JobResult {
	start := time.Now()
	var rowsAffected int64

	switch strings.Fields(query)[0] {
	case "select", "show":
		rows, err := db.Query(query)
		if err != nil {
			log.Fatalf("error for query %s in %s: %v", query, name, err)
		}
		defer rows.Close()
		for rows.Next() {
			rowsAffected++
		}
		if err = rows.Err(); err != nil {
			log.Fatalf("error for query %s in %s: %v", query, name, err)
		}
	default:
		res, err := db.Exec(query)
		if err != nil {
			log.Fatalf("error for query %s in %s: %v", query, name, err)
		}
		rowsAffected, _ = res.RowsAffected()
	}

	stop := time.Now()
	elapsed := stop.Sub(start)

	return JobResult{name, jobStart, jobStart + elapsed, rowsAffected, elapsed}
}

func runTest(db *sql.DB, config *Config) {
	testStart := time.Now()
	if len(config.Setup) > 0 && *runSetup {
		log.Printf("Performing setup")
		for _, query := range config.Setup {
			TimeQuery(db, 0, "setup", query)
		}
	}

	if *runWorkload {
		ctx, _ := context.WithTimeout(context.Background(), config.Duration)
		var resultChans = make([]<-chan JobResult, 0, len(config.Jobs))

		for _, job := range config.Jobs {
			resultChans = append(resultChans, job.StartResultChan(ctx, db))
		}

		testStats := processResults(config, mergeJobResultChans(resultChans...))

		for name, stats := range testStats {
			log.Printf("%s: %v", name, stats)
		}
	}

	if len(config.Teardown) > 0 && *runTeardown {
		log.Printf("Performing teardown")
		for _, query := range config.Teardown {
			TimeQuery(db, time.Since(testStart), "teardown", query)
		}
	}
}

var driver = flag.String("driver", "mysql", "Database driver to use.")
var username = flag.String("username", "root", "Database connection username")
var password = flag.String("password", "", "Database connection password")
var host = flag.String("host", "localhost", "Database connection host")
var port = flag.Int("port", 3306, "Database connection port")
var database = flag.String("database", "", "Database to use.")
var maxIdleConns = flag.Int("max-idle-conns", 100, "Maximum idle database connections")
var maxActiveConns = flag.Int("max-active-conns", 0, "Maximum active database connections")
var runSetup = flag.Bool("run-setup", true, "Run the setup phase")
var runWorkload = flag.Bool("run-workload", true, "Run the workload phase")
var runTeardown = flag.Bool("run-teardown", true, "Run the teardown phase")

func main() {
	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s [options] <runfile.ini>\n", os.Args[0])
		flag.PrintDefaults()
	}

	if len(flag.Args()) == 0 {
		flag.Usage()
		log.Fatal("No config file to parse")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", *username, *password, *host,
		*port, *database)
	log.Println("Connecting to", dsn)

	db, err := sql.Open(*driver, dsn)
	if err != nil {
		log.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	log.Println("Connected")

	/*
	 * Go very aggressively recycles connections; inform the runtime
	 * to hold onto some idle connections.
	 */
	db.SetMaxIdleConns(*maxIdleConns)

	/*
	 * This can lead to deadlocks in go version <= 1.2:
	 * https://code.google.com/p/go/source/detail?r=8a7ac002f840
	 */
	db.SetMaxOpenConns(*maxIdleConns)

	if config, err := parseConfig(flag.Arg(0)); err != nil {
		log.Fatalf("parsing config file %v", err)
	} else {
		runTest(db, config)
	}
}
