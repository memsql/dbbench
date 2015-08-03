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
	Query     []string
	QueryFile []string
}

type iniJob struct {
	QueryLog string

	queryConfig
	Rate       float64
	QueueDepth int

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

func canonicalizeQuery(query string) (string, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	if len(query) == 0 {
		return "", errors.New("cannot use empty query")
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
	if len(qc.Query) == 0 && len(qc.QueryFile) == 0 {
		return nil, NoQueryProvidedError
	}

	queries := make([]string, 0, 1)

	for _, queryFile := range qc.QueryFile {
		if contents, err := ioutil.ReadFile(queryFile); err != nil {
			return nil, err
		} else {
			for _, query := range strings.Split(string(contents), *QS) {
				if query, err := canonicalizeQuery(query); err != nil {
					return nil, fmt.Errorf("invalid query in %s: %v",
						queryFile, err)
				} else {
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

	return queries, nil
}

func (ij *iniJob) ToJob() (*Job, error) {
	var job = new(Job)
	var err error
	var queries []string

	if len(ij.QueryLog) > 0 {
		job.QueryLog, err = os.Open(ij.QueryLog)
		if err != nil {
			return nil, fmt.Errorf("error opening %s: %v", ij.QueryLog, err)
		}
	} else {
		queries, err = ij.queryConfig.GetQueries()
		if err != nil {
			return nil, err
		}
		if len(queries) > 1 {
			return nil, errors.New("more than one query provided")
		}
		job.Query = queries[0]

		if ij.QueueDepth > 0 {
			job.QueueDepth = ij.QueueDepth
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

	if config.Duration, err = time.ParseDuration(iniConfig.Global.Duration); err != nil {
		return nil, fmt.Errorf("error parsing duration: %v", err)
	}
	config.Setup, err = iniConfig.Setup.GetQueries()
	if err != nil && err != NoQueryProvidedError {
		return nil, fmt.Errorf("error parsing setup: %v", err)
	}
	config.Teardown, err = iniConfig.Teardown.GetQueries()
	if err != nil && err != NoQueryProvidedError {
		return nil, fmt.Errorf("error parsing teardown: %v", err)
	}

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
