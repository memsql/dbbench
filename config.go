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
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/awreece/goini"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Duration time.Duration
	Setup    []string
	Teardown []string
	Jobs     map[string]*Job
}

func (c *Config) String() string {
	return quotedStruct(c)
}

func readQueriesFromReader(df DatabaseFlavor, r io.Reader) ([]string, error) {
	queries := make([]string, 0, 1)
	if contents, err := ioutil.ReadAll(r); err != nil {
		return nil, err
	} else {
		for _, query := range strings.Split(string(contents), df.QuerySeparator()) {
			err := df.CheckQuery(query)
			if err != nil && err != EmptyQueryError {
				return nil, fmt.Errorf("invalid query %v", err)
			} else if err == nil {
				queries = append(queries, query)
			}
		}
	}
	return queries, nil
}

func readQueriesFromFile(df DatabaseFlavor, queryFile string) ([]string, error) {
	file, err := os.Open(queryFile)
	if err != nil {
		return nil, err
	}
	return readQueriesFromReader(df, file)
}

type globalSectionParser struct {
	config *Config
	flavor DatabaseFlavor
}

var globalOptions = goini.DecodeOptionSet{
	"duration": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "When the test will stop launching new jobs, as a duration " +
			" elapsed since setup ",
		Parse: func(v string, gsp interface{}) (e error) {
			gsp.(*globalSectionParser).config.Duration, e = time.ParseDuration(v)
			return e
		},
	},
}

func decodeGlobalSection(df DatabaseFlavor, s goini.RawSection, c *Config) error {
	return globalOptions.Decode(s, &globalSectionParser{c, df})
}

type setupSectionParser struct {
	queries []string
	df      DatabaseFlavor
}

var setupOptions = goini.DecodeOptionSet{
	"query": &goini.DecodeOption{Kind: goini.MultiOption,
		Usage: "Setup query to be executed before any jobs are started. " +
			"Must be a single query and cannot have any effect on the " +
			"connection (e.g USE or BEGIN).",
		Parse: func(v string, sspi interface{}) error {
			ssp := sspi.(*setupSectionParser)
			if e := ssp.df.CheckQuery(v); e != nil {
				return e
			}
			ssp.queries = append(ssp.queries, v)
			return nil
		},
	},
	"query-file": &goini.DecodeOption{Kind: goini.MultiOption,
		Usage: "Setup query to be executed before any jobs are started. " +
			"Must be a single query and cannot have any effect on the " +
			"connection (e.g USE or BEGIN).",
		Parse: func(v string, sspi interface{}) error {
			ssp := sspi.(*setupSectionParser)
			if qs, err := readQueriesFromFile(ssp.df, v); err != nil {
				return err
			} else {
				ssp.queries = append(ssp.queries, qs...)
				return nil
			}
		},
	},
}

func decodeSetupSection(df DatabaseFlavor, s goini.RawSection, ss *[]string) error {
	parser := setupSectionParser{*ss, df}
	err := setupOptions.Decode(s, &parser)
	if err == nil {
		*ss = parser.queries
	}
	return err
}

type jobParser struct {
	j                 *Job
	df                DatabaseFlavor
	multiQueryAllowed bool
}

var jobOptions = goini.DecodeOptionSet{
	"start": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "When this job should start, as a duration elapsed since setup.",
		Parse: func(v string, jp interface{}) (e error) {
			jp.(*jobParser).j.Start, e = time.ParseDuration(v)
			return e
		},
	},
	"stop": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "When this job should stop, as a duration elapsed since setup.",
		Parse: func(v string, jp interface{}) (e error) {
			jp.(*jobParser).j.Stop, e = time.ParseDuration(v)
			return e
		},
	},
	"query": &goini.DecodeOption{Kind: goini.MultiOption,
		Usage: "Query to execute for the job. " +
			"Must be a single query and cannot have any effect on the " +
			"connection (e.g USE or BEGIN).",
		Parse: func(v string, jpi interface{}) error {
			jp := jpi.(*jobParser)
			if e := jp.df.CheckQuery(v); e != nil {
				return e
			} else {
				jp.j.Queries = append(jp.j.Queries, v)
				return nil
			}
		},
	},
	"query-file": &goini.DecodeOption{Kind: goini.MultiOption,
		Usage: "File containing queries to execute for the job. " +
			"Queries are separated by the query-separator and cannot have any " +
			"effect on the connection (e.g USE or BEGIN).",
		Parse: func(v string, jpi interface{}) error {
			jp := jpi.(*jobParser)
			if qs, err := readQueriesFromFile(jp.df, v); err != nil {
				return err
			} else {
				jp.j.Queries = append(jp.j.Queries, qs...)
				return nil
			}
		},
	},
	"query-args-file": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "File(s) containing csv delimited query args, one line per " +
			"query.",
		Parse: func(v string, jpi interface{}) error {
			jp := jpi.(*jobParser)
			if file, err := os.Open(v); err != nil {
				return err
			} else {
				jp.j.QueryArgs = csv.NewReader(file)
				return nil
			}
		},
	},
	"rate": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "The number of batches executed per second (default 0.0).",
		Parse: func(v string, jpi interface{}) (e error) {
			jp := jpi.(*jobParser)
			jp.j.Rate, e = strconv.ParseFloat(v, 64)
			if e == nil && jp.j.Rate < 0 {
				return errors.New("invalid negative value for rate")
			}
			return e
		},
	},
	"batch-size": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "Number of jobs started during one batch (default 1).",
		Parse: func(v string, jp interface{}) (e error) {
			jp.(*jobParser).j.BatchSize, e = strconv.ParseUint(v, 10, 0)
			return e
		},
	},
	"queue-depth": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "Number of simultaneous executions of the job allowed.",
		Parse: func(v string, jp interface{}) (e error) {
			// Is there a way to make go respect numeric prefixes (e.g. 0x0)?
			jp.(*jobParser).j.QueueDepth, e = strconv.ParseUint(v, 10, 0)
			return e
		},
	},
	"concurrency": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "Number of simultaneous executions of the job allowed.",
		Parse: func(v string, jp interface{}) (e error) {
			// Is there a way to make go respect numeric prefixes (e.g. 0x0)?
			jp.(*jobParser).j.QueueDepth, e = strconv.ParseUint(v, 10, 0)
			return e
		},
	},
	"count": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "Number of time job is executed before stopping.",
		Parse: func(v string, jp interface{}) (e error) {
			jp.(*jobParser).j.Count, e = strconv.ParseUint(v, 10, 0)
			return e
		},
	},
	"multi-query-mode": &goini.DecodeOption{Kind: goini.UniqueOption,
		Usage: "Set to 'multi-connection' to signal that the job will execute " +
			"multiple queries, but it is safe for them to be on different " +
			"connections.",
		Parse: func(v string, jp interface{}) error {
			if v == "multi-connection" {
				jp.(*jobParser).multiQueryAllowed = true
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
		Parse: func(v string, jp interface{}) (e error) {
			jp.(*jobParser).j.QueryLog, e = os.Open(v)
			return e
		},
	},
}

func decodeJobSection(df DatabaseFlavor, section goini.RawSection, job *Job) error {
	jp := jobParser{job, df, false}

	if err := jobOptions.Decode(section, &jp); err != nil {
		return err
	} else if len(job.Queries) == 0 && job.QueryLog == nil {
		return errors.New("no query provided")
	} else if len(job.Queries) > 0 && job.QueryLog != nil {
		return errors.New("cannot have both queries and a query log")
	} else if len(job.Queries) > 1 && !jp.multiQueryAllowed {
		return fmt.Errorf("must have only one query")
	} else if job.Rate == 0 && job.BatchSize > 0 {
		return errors.New("can only specify batch-size with rate")
	}

	// If neither the queue depth nor the rate has been set,
	// allow one query at a time.
	//
	if job.QueueDepth == 0 && job.Rate == 0 {
		job.QueueDepth = 1
	}

	if job.Rate > 0 && job.BatchSize == 0 {
		job.BatchSize = 1
	}

	return nil
}

func decodeConfigJobs(df DatabaseFlavor, config *Config, iniConfig *goini.RawConfig) error {
	config.Jobs = make(map[string]*Job)
	for name, section := range iniConfig.Sections() {
		// Don't try to parse a reserved section as a job.
		if name == "setup" || name == "teardown" || name == "global" {
			continue
		}

		job := new(Job)
		job.Name = name
		if err := decodeJobSection(df, section, job); err != nil {
			return fmt.Errorf("Error parsing job %s: %v",
				strconv.Quote(name), err)
		}
		config.Jobs[name] = job
	}
	return nil
}

func parseIniConfig(df DatabaseFlavor, iniConfig *goini.RawConfig) (*Config, error) {
	var config = new(Config)

	if err := decodeGlobalSection(df, iniConfig.GlobalSection, config); err != nil {
		return nil, fmt.Errorf("Error parsing global section: %v", err)
	}
	if err := decodeSetupSection(df, iniConfig.Sections()["setup"], &config.Setup); err != nil {
		return nil, fmt.Errorf("Error parsing setup section: %v", err)
	}
	if err := decodeSetupSection(df, iniConfig.Sections()["teardown"], &config.Teardown); err != nil {
		return nil, fmt.Errorf("Error parsing teardown section: %v", err)
	}
	if err := decodeConfigJobs(df, config, iniConfig); err != nil {
		return nil, err
	}

	for name, job := range config.Jobs {
		if config.Duration > 0 && job.Start > config.Duration {
			return nil, fmt.Errorf("job %s starts after test finishes.",
				strconv.Quote(name))
		} else if job.Stop > 0 && config.Duration > 0 && job.Stop > config.Duration {
			return nil, fmt.Errorf("job %s stops after test finishes.",
				strconv.Quote(name))
		}
	}

	return config, nil
}

func parseConfig(df DatabaseFlavor, configFile string) (*Config, error) {
	cp := goini.NewRawConfigParser()
	cp.ParseFile(configFile)
	iniConfig, err := cp.Finish()
	if err != nil {
		return nil, err
	}

	return parseIniConfig(df, iniConfig)
}
