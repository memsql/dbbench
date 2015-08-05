package main

import (
	"code.google.com/p/gcfg"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

type queryConfig struct {
	Query            []string
	Query_File       []string
	Multi_query_mode string
}

type iniJob struct {
	Query_Log string

	queryConfig
	Rate        float64
	Queue_Depth int

	Count int
	Start string
	Stop  string
}

type iniConfig struct {
	Global struct {
		Duration string
	}
	Setup    queryConfig
	Teardown queryConfig
	Job      map[string]*iniJob
}

var NoQueryProvidedError = errors.New("no query provided")

var QS = flag.String("query-separator", ";", "Separator between queries in a file.")

var EmptyQueryError = errors.New("cannot use empty query")

func canonicalizeQuery(query string) (string, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	if len(query) == 0 {
		return "", EmptyQueryError
	}
	if strings.Contains(query, *QS) {
		return "", errors.New("cannot have a semicolon")
	}
	switch strings.Fields(query)[0] {
	case "begin":
		return "", errors.New("cannot use transactions")
	case "use":
		return "", errors.New("cannot change database")
	}
	return query, nil
}

func (qc *queryConfig) GetQueries() ([]string, error) {
	if len(qc.Query) == 0 && len(qc.Query_File) == 0 {
		return nil, NoQueryProvidedError
	}

	queries := make([]string, 0, 1)

	for _, queryFile := range qc.Query_File {
		if contents, err := ioutil.ReadFile(queryFile); err != nil {
			return nil, err
		} else {
			for _, query := range strings.Split(string(contents), *QS) {
				if query, err := canonicalizeQuery(query); err != nil && err != EmptyQueryError {
					return nil, fmt.Errorf("invalid query in %s: %v",
						queryFile, err)
				} else if err == nil {
					queries = append(queries, query)
				}
			}
		}
	}
	for _, query := range qc.Query {
		if query, err := canonicalizeQuery(query); err != nil {
			return nil, err
		} else {
			queries = append(queries, query)
		}
	}
	if len(queries) == 0 {
		return nil, errors.New("no queries provided")
	}

	return queries, nil
}

func (ij *iniJob) ToJob() (*Job, error) {
	var job = new(Job)
	var err error

	if len(ij.Query_Log) > 0 {
		job.QueryLog, err = os.Open(ij.Query_Log)
		if err != nil {
			return nil, fmt.Errorf("error opening %s: %v", ij.Query_Log, err)
		}
	} else {
		job.Queries, err = ij.queryConfig.GetQueries()
		if err != nil {
			return nil, err
		}
		if len(job.Queries) > 1 && ij.queryConfig.Multi_query_mode != "multi-connection" {
			return nil, errors.New("more than one query provided without multi-query-mode=multi-connection")
		}

		if ij.Queue_Depth > 0 {
			job.QueueDepth = ij.Queue_Depth
		} else if ij.Rate == 0 {
			job.QueueDepth = 1
		}

		job.Rate = ij.Rate
		job.Count = ij.Count
	}

	if job.Start, err = time.ParseDuration(ij.Start); err != nil && len(ij.Start) > 0 {
		return nil, fmt.Errorf("parsing start: %v", err)
	}
	if job.Stop, err = time.ParseDuration(ij.Stop); err != nil && len(ij.Stop) > 0 {
		return nil, fmt.Errorf("parsing start: %v", err)
	}
	if job.Stop > 0 && job.Stop < job.Start {
		return nil, errors.New("job start must be before end")
	}
	return job, nil
}

func parseConfig(configFile string) (*Config, error) {
	var iniConfig iniConfig
	var config = new(Config)
	var err error

	if err = gcfg.ReadFileInto(&iniConfig, configFile); err != nil {
		return nil, err
	}

	if len(iniConfig.Global.Duration) > 0 {
		if config.Duration, err = time.ParseDuration(iniConfig.Global.Duration); err != nil {
			return nil, fmt.Errorf("error parsing duration: %v", err)
		}
	}
	config.Setup.Queries, err = iniConfig.Setup.GetQueries()
	if err != nil && err != NoQueryProvidedError {
		return nil, fmt.Errorf("error parsing setup: %v", err)
	}
	config.Setup.Name = "setup"
	config.Teardown.Queries, err = iniConfig.Teardown.GetQueries()
	if err != nil && err != NoQueryProvidedError {
		return nil, fmt.Errorf("error parsing teardown: %v", err)
	}
	config.Teardown.Name = "teardown"

	config.Jobs = make(map[string]*Job)
	for name, iniJob := range iniConfig.Job {
		if job, err := iniJob.ToJob(); err != nil {
			return nil, fmt.Errorf("error parsing job %s: %v", err, name)
		} else {
			job.Name = name
			if job.Start > config.Duration {
				return nil, fmt.Errorf("job %s starts after test finishes", name)
			} else if job.Stop > 0 && job.Stop > config.Duration {
				return nil, fmt.Errorf("job %s finishes after test finishes", name)
			}
			config.Jobs[name] = job
		}
	}
	return config, nil
}
