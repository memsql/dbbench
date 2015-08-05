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
	"time"
)

type JobInvocation struct {
	Name    string
	Queries []string
}

type Job struct {
	Name string

	Queries    []string
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
	Setup    JobInvocation
	Teardown JobInvocation
	Jobs     map[string]*Job
}

type JobResult struct {
	Name         string
	Start        time.Duration
	Stop         time.Duration
	RowsAffected int64
}

func runTest(db *sql.DB, config *Config) {
	if len(config.Setup.Queries) > 0 && *runSetup {
		log.Printf("Performing setup")
		config.Setup.Invoke(db, 0)
	}

	if *runWorkload {
		ctx := context.Background()
		if config.Duration > 0 {
			ctx, _ = context.WithTimeout(context.Background(), config.Duration)
		}
		var resultChans = make([]<-chan *JobResult, 0, len(config.Jobs))

		for _, job := range config.Jobs {
			resultChans = append(resultChans, job.StartResultChan(ctx, db))
		}

		testStats := processResults(config, mergeJobResultChans(resultChans...))

		for name, stats := range testStats {
			log.Printf("%s: %v", name, stats)
		}
	}

	if len(config.Teardown.Queries) > 0 && *runTeardown {
		log.Printf("Performing teardown")
		config.Teardown.Invoke(db, 0)
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
