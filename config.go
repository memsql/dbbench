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
	"errors"
	"flag"
	"fmt"
	"github.com/awreece/goini"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

var NoQueryProvidedError = errors.New("no query provided")

var QS = flag.String("query-separator", ";", "Separator between queries in a file.")

var EmptyQueryError = errors.New("cannot use empty query")

func canonicalizeQuery(query string) (string, error) {
	query = strings.TrimSpace(query)
	if len(query) == 0 {
		return "", EmptyQueryError
	}
	if strings.Contains(query, *QS) {
		return "", errors.New("cannot have a semicolon")
	}
	switch strings.ToLower(strings.Fields(query)[0]) {
	case "begin":
		return "", errors.New("cannot use transactions")
	case "use":
		return "", errors.New("cannot change database")
	}
	return query, nil
}

func readQueriesFromFile(queryFile string) ([]string, error) {
	var queries []string
	if contents, err := ioutil.ReadFile(queryFile); err != nil {
		return nil, err
	} else {
		for _, query := range strings.Split(string(contents), *QS) {
			query, err := canonicalizeQuery(query)
			if err != nil && err != EmptyQueryError {
				return nil, fmt.Errorf("invalid query in %s: %v", queryFile,
					err)
			} else if err == nil {
				queries = append(queries, query)
			}
		}
	}
	return queries, nil
}

var globalOptions = goini.DecodeOptionSet{
	"duration": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "When the test will stop launching new jobs, as a duration " +
			" elapsed since setup ",
		Parse: func(v string, c interface{}) (e error) {
			c.(*Config).Duration, e = time.ParseDuration(v)
			return e
		},
	},
}

var setupOptions = goini.DecodeOptionSet{
	"query": &goini.DecodeOption{Kind: goini.MultiOption,
		Usage: "Setup query to be executed before any jobs are started. " +
			"Must be a single query and cannot have any effect on the " +
			"connection (e.g USE or BEGIN).",
		Parse: func(v string, jii interface{}) (e error) {
			ji := jii.(*JobInvocation)
			ji.Queries = append(ji.Queries, v)
			return nil
		},
	},
	"query-file": &goini.DecodeOption{Kind: goini.MultiOption,
		Usage: "Setup query to be executed before any jobs are started. " +
			"Must be a single query and cannot have any effect on the " +
			"connection (e.g USE or BEGIN).",
		Parse: func(v string, jii interface{}) (e error) {
			ji := jii.(*JobInvocation)
			if qs, err := readQueriesFromFile(v); err != nil {
				return err
			} else {
				ji.Queries = append(ji.Queries, qs...)
				return nil
			}
		},
	},
}

var jobOptions = goini.DecodeOptionSet{
	"start": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "When this job should start, as a duration elapsed since setup.",
		Parse: func(v string, j interface{}) (e error) {
			j.(*Job).Start, e = time.ParseDuration(v)
			return e
		},
	},
	"stop": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "When this job should stop, as a duration elapsed since setup.",
		Parse: func(v string, j interface{}) (e error) {
			j.(*Job).Stop, e = time.ParseDuration(v)
			return e
		},
	},
	"query": &goini.DecodeOption{Kind: goini.MultiOption,
		Usage: "Query to execute for the job. " +
			"Must be a single query and cannot have any effect on the " +
			"connection (e.g USE or BEGIN).",
		Parse: func(v string, j interface{}) error {
			if q, e := canonicalizeQuery(v); e != nil {
				return e
			} else {
				j.(*Job).Queries = append(j.(*Job).Queries, q)
				return nil
			}
		},
	},
	"query-file": &goini.DecodeOption{Kind: goini.MultiOption,
		Usage: "File containing queries to execute for the job. " +
			"Queries are separated by the query-separator and cannot have any " +
			"effect on the connection (e.g USE or BEGIN).",
		Parse: func(v string, j interface{}) error {
			if qs, err := readQueriesFromFile(v); err != nil {
				return err
			} else {
				j.(*Job).Queries = append(j.(*Job).Queries, qs...)
				return nil
			}
		},
	},
	"rate": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "Rate to execute the job, a floating point executions per seconds.",
		Parse: func(v string, ji interface{}) (e error) {
			j := ji.(*Job)
			j.Rate, e = strconv.ParseFloat(v, 64)
			if e == nil && j.Rate < 0 {
				return errors.New("invalid negative value for rate")
			}
			return e
		},
	},
	"queue-depth": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "Number of simultaneous executions of the job allowed.",
		Parse: func(v string, j interface{}) (e error) {
			// Is there a way to make go respect numeric prefixes (e.g. 0x0)?
			j.(*Job).QueueDepth, e = strconv.ParseUint(v, 10, 0)
			return e
		},
	},
	"count": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "Number of time job is executed before stopping.",
		Parse: func(v string, j interface{}) (e error) {
			j.(*Job).Count, e = strconv.ParseUint(v, 10, 0)
			return e
		},
	},
	"multi-query-mode": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "Set to 'multi-connection' to signal that the job will execute " +
			"multiple queries, but it is safe for them to be on different " +
			"connections.",
		Parse: func(v string, j interface{}) error {
			if v == "multi-connection" {
				j.(*Job).MultiQueryAllowed = true
				return nil
			} else {
				return fmt.Errorf("invalid value for multi-query-mode: %s",
					strconv.Quote(v))
			}
		},
	},
	"query-log": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "A flat text file containing a log file to replay instead of a " +
			"normal job. The query log format is a series of newline " +
			"delimited records containing a time in microseconds and a query " +
			"separated by a comma. For example, '8644882534,select 1'.",
		Parse: func(v string, j interface{}) (e error) {
			j.(*Job).QueryLog, e = os.Open(v)
			return e
		},
	},
}

func parseConfigJobs(config *Config, iniConfig *goini.RawConfig) error {
	config.Jobs = make(map[string]*Job)
	for name, section := range iniConfig.Sections() {
		// Don't try to parse a reserved section as a job.
		if name == "setup" || name == "teardown" || name == "global" {
			continue
		}

		job := new(Job)

		if err := jobOptions.Decode(section, job); err != nil {
			return fmt.Errorf("error parsing job %s: %v",
				strconv.Quote(name), err)
		} else {
			job.Name = name
			if config.Duration > 0 && job.Start > config.Duration {
				return fmt.Errorf("job %s starts after test finishes.",
					strconv.Quote(name))
			} else if job.Stop > 0 && config.Duration > 0 && job.Stop > config.Duration {
				return fmt.Errorf("job %s finishes after test finishes.",
					strconv.Quote(name))
			} else if len(job.Queries) == 0 && job.QueryLog == nil {
				return fmt.Errorf(
					"job %s does not specify either a query or a query log.",
					strconv.Quote(name))
			} else if len(job.Queries) > 0 && job.QueryLog != nil {
				return fmt.Errorf(
					"job %s cannot have both queries and a query log.",
					strconv.Quote(name))
			} else if len(job.Queries) > 1 && !job.MultiQueryAllowed {
				return fmt.Errorf("job %s must have only one query.",
					strconv.Quote(name))
			}

			// If neither the queue depth nor the rate has been set,
			// allow one query at a time.
			//
			if job.QueueDepth == 0 && job.Rate == 0 {
				job.QueueDepth = 1
			}

			config.Jobs[name] = job
		}
	}
	return nil
}

func parseConfig(configFile string) (*Config, error) {
	cp := goini.NewRawConfigParser()
	cp.ParseFile(configFile)
	iniConfig, err := cp.Finish()
	if err != nil {
		return nil, err
	}

	var config = new(Config)

	if err := globalOptions.Decode(iniConfig.GlobalSection, config); err != nil {
		return nil, fmt.Errorf("Error parsing global section: %v", err)
	}
	if err := setupOptions.Decode(iniConfig.Sections()["setup"], &config.Setup); err != nil {
		return nil, fmt.Errorf("Error parsing setup section: %v", err)
	}
	if err := setupOptions.Decode(iniConfig.Sections()["teardown"], &config.Teardown); err != nil {
		return nil, fmt.Errorf("Error parsing teardown section: %v", err)
	}

	if err := parseConfigJobs(config, iniConfig); err != nil {
		return nil, err
	}

	return config, nil
}
