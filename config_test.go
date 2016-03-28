/*
 * Copyright (c) 2016 by MemSQL. All rights reserved.
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
	"github.com/awreece/goini"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReadQueries(t *testing.T) {
	var cases = []struct {
		in  string
		out []string
	}{
		{"select * from t; select * from t",
			[]string{"select * from t", " select * from t"},
		},
		{"   select * \n from t; select * \nfrom t",
			[]string{"   select * \n from t", " select * \nfrom t"},
		},
		{";;;;", []string{}},
	}

	df := supportedDatabaseFlavors["mysql"]
	for _, c := range cases {
		qs, err := readQueriesFromReader(df, strings.NewReader(c.in))
		if err != nil {
			t.Errorf("Error reading queries from %s: %v", strconv.Quote(c.in), err)
		} else if !reflect.DeepEqual(qs, c.out) {
			t.Errorf("Failure reading queries from %s:\ngot\t\t%v\nbut expected\t%v",
				strconv.Quote(c.in), quotedValue(qs), quotedValue(c.out))
		}
	}
}

func TestParseIniConfig(t *testing.T) {
	var goodCases = []struct {
		in  string
		out *Config
	}{
		{"[test]\nquery=select 1",
			&Config{
				Jobs: map[string]*Job{
					"test": &Job{
						Name: "test", QueueDepth: 1,
						Queries: []string{"select 1"},
					},
				},
			},
		},
		{"[test1]\nquery=select 1\n[test2]\nquery=select 2",
			&Config{
				Jobs: map[string]*Job{
					"test1": &Job{
						Name: "test1", QueueDepth: 1,
						Queries: []string{"select 1"},
					},
					"test2": &Job{
						Name: "test2", QueueDepth: 1,
						Queries: []string{"select 2"},
					},
				},
			},
		},
		{`
			[test1]
			query=select 1
			rate=1
			`,
			&Config{
				Jobs: map[string]*Job{
					"test1": &Job{
						Name: "test1", Rate: 1.0,
						Queries:   []string{"select 1"},
						BatchSize: 1,
					},
				},
			},
		},
		{`
			[test job]
			query=show databases
			count=1
			`,
			&Config{
				Jobs: map[string]*Job{
					"test job": &Job{
						Name: "test job", QueueDepth: 1, Count: 1,
						Queries: []string{"show databases"},
					},
				},
			},
		},
		{`
			[setup]
			query=insert into t select RAND(), RAND()
			query=insert into t select RAND(), RAND() from t
			query=insert into t select RAND(), RAND() from t

			[teardown]
			query=drop table t

			[count]
			query=count(*) from t where a < b
			count=30
			`,
			&Config{
				Setup: JobInvocation{
					Queries: []string{
						"insert into t select RAND(), RAND()",
						"insert into t select RAND(), RAND() from t",
						"insert into t select RAND(), RAND() from t",
					},
				},
				Teardown: JobInvocation{
					Queries: []string{
						"drop table t",
					},
				},
				Jobs: map[string]*Job{
					"count": &Job{
						Name: "count", QueueDepth: 1, Count: 30,
						Queries: []string{"count(*) from t where a < b"},
					},
				},
			},
		},
		{
			`
			[run 2 queries at a time for 10 seconds, starting at 5s]
			query=select count(*) from mytable
			queue-depth=2
			start=5s
			stop=15s
			`,
			&Config{
				Jobs: map[string]*Job{
					"run 2 queries at a time for 10 seconds, starting at 5s": &Job{
						Name:       "run 2 queries at a time for 10 seconds, starting at 5s",
						QueueDepth: 2, Start: 5 * time.Second, Stop: 15 * time.Second,
						Queries: []string{"select count(*) from mytable"},
					},
				},
			},
		},
		{
			`
			duration=10s

			[test job]
			query=select 1+1
			`,
			&Config{
				Duration: 10 * time.Second,
				Jobs: map[string]*Job{
					"test job": &Job{
						Name: "test job", QueueDepth: 1,
						Queries: []string{"select 1+1"},
					},
				},
			},
		},
	}

	var badCases = []string{
		"[test]\nrate=1",
	}

	df := supportedDatabaseFlavors["mysql"]
	for _, c := range goodCases {
		cp := goini.NewRawConfigParser()
		cp.Parse(strings.NewReader(c.in))
		iniConfig, err := cp.Finish()
		if err != nil {
			t.Errorf("Error parsing config %s: %v", strconv.Quote(c.in), err)
			continue
		}

		config, err := parseIniConfig(df, iniConfig)
		if err != nil {
			t.Errorf("Error parsing ini config %s: %v", strconv.Quote(c.in), err)
			continue
		}

		if !reflect.DeepEqual(config, c.out) {
			t.Errorf("Failure parsing config %s:\ngot\t\t%v\nbut expected\t%v",
				strconv.Quote(c.in), config, c.out)
		}
	}

	for _, c := range badCases {
		cp := goini.NewRawConfigParser()
		cp.Parse(strings.NewReader(c))
		iniConfig, err := cp.Finish()
		if err != nil {
			t.Errorf("Error parsing config %s: %v", strconv.Quote(c), err)
			continue
		}

		_, err = parseIniConfig(df, iniConfig)
		if err == nil {
			t.Errorf("Unexpected succesful parse of iniConfig for %s", strconv.Quote(c))
		}
	}
}
