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
	"bytes"
	"fmt"
	"reflect"
	"strconv"
)

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
		var buf bytes.Buffer
		buf.WriteString("[")
		var printed bool
		for _, s := range v {
			if printed {
				buf.WriteString(", ")
			}
			buf.WriteString(strconv.Quote(s))
			printed = true
		}
		buf.WriteString("]")
		return buf.String()
	}
}

func quotedStruct(s interface{}) string {
	var buf bytes.Buffer

	structType := reflect.TypeOf(s)
	structValue := reflect.ValueOf(s)

	if structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
		structValue = structValue.Elem()

		buf.WriteString("&")
	}

	buf.WriteString(fmt.Sprintf("%s{", structType.Name()))

	var printedField bool
	for fi := 0; fi < structType.NumField(); fi++ {
		fieldValue := structValue.Field(fi).Interface()
		structField := structType.Field(fi)

		if !reflect.DeepEqual(reflect.Zero(structField.Type).Interface(), fieldValue) {
			if printedField {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("%s: %s", structField.Name,
				quotedValue(fieldValue)))
			printedField = true
		}
	}
	buf.WriteString("}")

	return buf.String()
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
