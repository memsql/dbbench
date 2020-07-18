/*
 * Copyright (c) 2016-2020 by MemSQL. All rights reserved.
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
	"os"
	"reflect"
	"strconv"
	"strings"
)

type WriteFileFlagValue struct {
	f *os.File
}

func (wffv *WriteFileFlagValue) Set(v string) (err error) {
	if wffv.f != nil {
		wffv.f.Close()
	}

	if v == "" {
		wffv.f = nil
	} else {
		wffv.f, err = os.Create(v)
	}
	return err
}

func (wffv *WriteFileFlagValue) String() string {
	ret := "&fileFlagValue{"
	if wffv.f != nil {
		ret += wffv.f.Name()
	}
	ret += "}"
	return ret
}

func (wffv *WriteFileFlagValue) Get() interface{} {
	return wffv.f
}

func (wffv *WriteFileFlagValue) GetFile() *os.File {
	return wffv.f
}

type Set map[interface{}]struct{}

func (s Set) Add(i interface{}) {
	s[i] = struct{}{}
}

func (s Set) Contains(i interface{}) bool {
	_, ok := s[i]
	return ok
}

func firstString(c, d string) string {
	if c != "" {
		return c
	} else {
		return d
	}
}

func firstInt(c, d int) int {
	if c != 0 {
		return c
	} else {
		return d
	}
}

func quotedValue(i interface{}) string {
	switch v := i.(type) {
	default:
		return fmt.Sprintf("%v", i)
	case string:
		return strconv.Quote(v)
	case []string:
		var str strings.Builder
		str.WriteString("[")
		var printed bool
		for _, s := range v {
			if printed {
				str.WriteString(", ")
			}
			str.WriteString(strconv.Quote(s))
			printed = true
		}
		str.WriteString("]")
		return str.String()
	}
}

func quotedStruct(s interface{}) string {
	var str strings.Builder

	structType := reflect.TypeOf(s)
	structValue := reflect.ValueOf(s)

	if structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
		structValue = structValue.Elem()

		str.WriteString("&")
	}

	str.WriteString(fmt.Sprintf("%s{", structType.Name()))

	var printedField bool
	for fi := 0; fi < structType.NumField(); fi++ {
		fieldValue := structValue.Field(fi).Interface()
		structField := structType.Field(fi)

		if !reflect.DeepEqual(reflect.Zero(structField.Type).Interface(), fieldValue) {
			if printedField {
				str.WriteString(", ")
			}
			str.WriteString(fmt.Sprintf("%s: %s", structField.Name,
				quotedValue(fieldValue)))
			printedField = true
		}
	}
	str.WriteString("}")

	return str.String()
}

func minFloat64(vals []float64) float64 {
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func maxFloat64(vals []float64) float64 {
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func maxInt(vals []int) int {
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func maxUint64(vals []uint64) uint64 {
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}
