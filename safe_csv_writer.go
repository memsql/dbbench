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
	"encoding/csv"
	"io"
	"os"
	"sync"
)

type SafeCSVWriter struct {
	m         sync.Mutex
	csvWriter *csv.Writer
	ioCloser  io.Closer
}

func (scw *SafeCSVWriter) Close() {
	scw.ioCloser.Close()
}

func (scw *SafeCSVWriter) Write(record []string) error {
	scw.m.Lock()
	defer scw.m.Unlock()

	return scw.csvWriter.Write(record)
}

func (scw *SafeCSVWriter) Flush() {
	scw.m.Lock()
	defer scw.m.Unlock()

	scw.csvWriter.Flush()
}

func (scw *SafeCSVWriter) Error() error {
	scw.m.Lock()
	defer scw.m.Unlock()

	return scw.csvWriter.Error()
}

func NewSafeCSVWriter(path string) (*SafeCSVWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &SafeCSVWriter{csvWriter: csv.NewWriter(f), ioCloser: f}, nil
}
