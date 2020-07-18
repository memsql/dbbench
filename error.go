/*
 * Copyright (c) 2015-2020 by MemSQL. All rights reserved.
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
	"fmt"
	"sort"
	"strings"
)

// Go's map can only handle comparable types as a key. We can't be sure that an error thrown by any possible database
// driver is comparable. So, we instead key by error code.
type ErrorCounts map[string]errorCounts // error code (string) -> errorCounts

type errorCounts struct {
	errorsPerQuery

	Error error
}

type errorsPerQuery map[string]uint64 // query -> count

func (ec ErrorCounts) String() string {
	var str strings.Builder
	str.WriteString("Errors (with frequency count)\n")
	for _, ec := range ec {
		str.WriteString(fmt.Sprintf("  (%dx) %v\n    Error occurred while running:\n%v", ec.Total(), ec.Error, ec))
	}
	return str.String()
}

func (ec ErrorCounts) Add(err error, query string, df DatabaseFlavor) error {
	code, e := df.ErrorCode(err)
	if e != nil {
		return e
	}
	if _, ok := ec[code]; !ok {
		ec[code] = errorCounts{make(errorsPerQuery), err}
	}
	ec[code].Add(query)
	return nil
}

func (ec ErrorCounts) TotalErrors() (total uint64) {
	for _, ecc := range ec {
		total += ecc.Total()
	}
	return
}

func (ec ErrorCounts) TotalAccepted(df DatabaseFlavor, errors Set) (total uint64) {
	for errCode, ecc := range ec {
		if errors.Contains(errCode) {
			total += ecc.Total()
		}
	}
	return
}

// Return a new ErrorCounts that contains just the subset of unhandled errors
func (ec ErrorCounts) UnhandledErrors(df DatabaseFlavor, errors Set) (newEc ErrorCounts) {
	newEc = make(ErrorCounts)
	for errCode, ecc := range ec {
		if !errors.Contains(errCode) {
			newEc[errCode] = ecc
		}
	}
	return
}

func (epq errorsPerQuery) String() string {
	var str strings.Builder

	// Now we sort the map by value (count), kudos: https://stackoverflow.com/a/44380276
	type kv struct {
		Query string
		Count uint64
	}

	var ss []kv
	for query, count := range epq {
		ss = append(ss, kv{query, count})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Count > ss[j].Count
	})

	for _, kv := range ss {
		str.WriteString(fmt.Sprintf("    (%dx) %v\n", kv.Count, kv.Query))
	}

	return str.String()
}

func (epq errorsPerQuery) Add(query string) {
	epq[query]++
}

func (epq errorsPerQuery) Total() (total uint64) {
	for _, count := range epq {
		total += count
	}
	return
}
