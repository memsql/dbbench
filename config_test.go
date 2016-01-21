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
)

func TestCanonicalize(t *testing.T) {
	var successCases = []struct {
		in  string
		out string
	}{
		{"select * from t", "select * from t"},
		{"   select * from t\n", "select * from t"},
		{"/*!90620 set interpreter_mode=llvm*/", "/*!90620 set interpreter_mode=llvm*/"},
	}

	for _, c := range successCases {
		actual, err := canonicalizeQuery(c.in)
		if err != nil {
			t.Errorf("Error canonicalizing query %s: %v", strconv.Quote(c.in), err)
		} else if actual != c.out {
			t.Errorf("Failure canonicalizing query %s:\ngot\t\t%s\nbut expected\t%s",
				strconv.Quote(c.in), strconv.Quote(actual), strconv.Quote(c.out))
		}
	}

	var failCases = []string{
		"select * from t; select 1",
		"use db",
		"begin",
	}

	for _, c := range failCases {
		_, err := canonicalizeQuery(c)
		if err == nil {
			t.Errorf("Unexpected successful canonicalization of query %s", strconv.Quote(c))
		}
	}
}

func quotedList(l []string) []string {
	ret := make([]string, len(l))
	for i, s := range l {
		ret[i] = strconv.Quote(s)
	}
	return ret
}

func TestReadQueries(t *testing.T) {
	var cases = []struct {
		in  string
		out []string
	}{
		{"select * from t; select * from t",
			[]string{"select * from t", "select * from t"},
		},
		{"   select * \n from t; select * \nfrom t",
			[]string{"select * \n from t", "select * \nfrom t"},
		},
		{";;;;", []string{}},
	}

	for _, c := range cases {
		qs, err := readQueriesFromReader(strings.NewReader(c.in))
		if err != nil {
			t.Errorf("Error reading queries from %s: %v", strconv.Quote(c.in), err)
		} else if !reflect.DeepEqual(qs, c.out) {
			t.Errorf("Failure reading queries from %s:\ngot\t\t%v\nbut expected\t%v",
				strconv.Quote(c.in), quotedList(qs), quotedList(c.out))
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
		{"[test1]\nquery=select 1\nrate=1",
			&Config{
				Jobs: map[string]*Job{
					"test1": &Job{
						Name: "test1", Rate: 1.0,
						Queries: []string{"select 1"},
					},
				},
			},
		},
	}

	var badCases = []string{
		"[test]\nrate=1",
	}

	for _, c := range goodCases {
		cp := goini.NewRawConfigParser()
		cp.Parse(strings.NewReader(c.in))
		iniConfig, err := cp.Finish()
		if err != nil {
			t.Errorf("Error parsing config %s: %v", strconv.Quote(c.in), err)
			continue
		}

		config, err := parseIniConfig(iniConfig)
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

		_, err = parseIniConfig(iniConfig)
		if err == nil {
			t.Errorf("Unexpected succesful parse of iniConfig for %s", strconv.Quote(c))
		}
	}
}
