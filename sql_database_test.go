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
	"strconv"
	"testing"
)

func TestSQLCheck(t *testing.T) {
	var successCases = []struct {
		in string
	}{
		{"select * from t"},
		{"   select * from t\n"},
		{"/*!90620 set interpreter_mode=llvm*/"},
	}

	for _, c := range successCases {
		if err := checkSQLQuery(c.in); err != nil {
			t.Errorf("Unexpected error checking query %s: %v",
				strconv.Quote(c.in), err)
		}
	}

	var failCases = []string{
		"select * from t; select 1",
		"use db",
		"begin",
	}

	for _, c := range failCases {
		if err := checkSQLQuery(c); err == nil {
			t.Errorf("Unexpected success checking query %s",
				strconv.Quote(c))
		}
	}
}
