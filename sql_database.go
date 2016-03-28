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
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
)

type sqlDb struct {
	db *sql.DB
}

func (s *sqlDb) RunQuery(q string) (int64, error) {
	switch action := strings.ToLower(strings.Fields(q)[0]); action {
	case "select", "show", "explain", "describe", "desc":
		return s.countQueryRows(q)
	case "use", "begin":
		return 0, fmt.Errorf("invalid query action: ", action)
	default:
		return s.countExecRows(q)
	}
}

func (s *sqlDb) countQueryRows(q string) (int64, error) {
	rows, err := s.db.Query(q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var rowsAffected int64
	for rows.Next() {
		rowsAffected++
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

func (s *sqlDb) countExecRows(q string) (int64, error) {
	res, err := s.db.Exec(q)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *sqlDb) Close() {
	s.db.Close()
}

type sqlDatabaseFlavor struct {
	name      string
	dsnFunc   func(cc *ConnectionConfig) string
	checkFunc func(q string) error
}

var maxIdleConns = flag.Int("max-idle-conns", 100, "Maximum idle database connections")
var maxActiveConns = flag.Int("max-active-conns", 0, "Maximum active database connections")

func (sq *sqlDatabaseFlavor) QuerySeparator() string {
	return ";"
}

func (sq *sqlDatabaseFlavor) Connect(cc *ConnectionConfig) (Database, error) {
	dsn := sq.dsnFunc(cc)
	log.Println("Connecting to", dsn)

	db, err := sql.Open(sq.name, dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	log.Println("Connected")

	/*
	 * Go very aggressively recycles connections; inform the runtime
	 * to hold onto some idle connections.
	 */
	db.SetMaxIdleConns(*maxIdleConns)

	/*
	 * This can lead to deadlocks in go version <= 1.2:
	 *
	 *     commit 0d12e24ebb037202c3324c230e075f1e448f6f34
	 *     Author: Marko Tiikkaja <marko@joh.to>
	 *     Date:   Thu Dec 26 11:27:18 2013 -0800
	 *
	 *         database/sql: Use all connections in pool
	 */
	db.SetMaxOpenConns(*maxIdleConns)

	return &sqlDb{db}, nil
}

func (sq *sqlDatabaseFlavor) CheckQuery(q string) error {
	return sq.checkFunc(q)
}

func checkSQLQuery(q string) error {
	query := strings.TrimSpace(q)
	if len(query) == 0 {
		return EmptyQueryError
	}
	if strings.Contains(query, ";") {
		return errors.New("cannot have a semicolon")
	}

	switch strings.ToLower(strings.Fields(query)[0]) {
	case "begin":
		return errors.New("cannot use transactions")
	case "use":
		return errors.New("cannot change database")
	}
	return nil
}

func mySQLDataSourceName(cc *ConnectionConfig) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?allowAllFiles=true",
		firstString(cc.Username, "root"),
		firstString(cc.Password, ""),
		firstString(cc.Host, "localhost"),
		firstInt(cc.Port, 3306),
		firstString(cc.Database, ""))
}

func postgresDataSourceName(cc *ConnectionConfig) string {
	return fmt.Sprintf("postggresql://%s:%s@%s:%d/%s",
		firstString(cc.Username, "root"),
		firstString(cc.Password, ""),
		firstString(cc.Host, "localhost"),
		firstInt(cc.Port, 5432),
		firstString(cc.Database, ""))
}

func sqlServerDataSourceName(cc *ConnectionConfig) string {
	return fmt.Sprintf("user id=%s;password=%s;server=%s;port=%d;database=%s",
		firstString(cc.Username, "root"),
		firstString(cc.Password, ""),
		firstString(cc.Host, "localhost"),
		firstInt(cc.Port, 1433),
		firstString(cc.Database, ""))
}
