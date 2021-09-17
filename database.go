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
	"errors"
	"net/url"
	"strconv"
	"strings"
)

/*
 * An abstract flavor of a database; for example, "postgres" or "mysql".
 */
type DatabaseFlavor interface {
	/*
	 * Connects to the database given by the connection config.
	 *
	 * If any connection property (e.g. host, port) is unspecified
	 * (i.e. zero), uses a database specific default for an instance
	 * of this database running on the local host.
	 */
	Connect(cc *ConnectionConfig) (Database, error)

	/*
	 * Validate that the query is supported by the underlying database
	 * driver. For example. the golang "database/sql".DB uses a connection
	 * pool so queries that affect the connection (e.g. "use", "begin") are
	 * disallowed.
	 *
	 * If the query is empty, returns EmptyQueryError.
	 */
	CheckQuery(string) error

	/*
	 * The separator used in query files for this flavor of database
	 * (e.g. ";") for most SQL databases.
	 */
	QuerySeparator() string

	/*
	 * The extracted error code (string) from the error (error) thrown by the database driver. This is needed to let
	 * dbbench handle arbitrary errors from any given database flavor.
	 */
	ErrorCode(error) (string, error)
}

var EmptyQueryError = errors.New("empty query found")

/*
 * The user specified parameters for connecting to a database. If any
 * field is zero, no user preference was provided.
 */
type ConnectionConfig struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	Params   string
}

/*
 * Override the connection configuration with parameters from the URL.
 *
 * If a given parameter is not inside the URL, then the one from
 * the connection configuration is kept untouched.
 */
func (cc *ConnectionConfig) OverrideFromURL(u url.URL) {
	if u.Host != "" {
		cc.Host = u.Host
	}
	if u.User.Username() != "" {
		cc.Username = u.User.Username()
	}
	pass, isPassSet := u.User.Password()
	if isPassSet {
		cc.Password = pass
	}
	if u.Hostname() != "" {
		cc.Host = u.Hostname()
	}
	if u.Port() != "" {
		cc.Port, _ = strconv.Atoi(u.Port())
	}
	if u.Path != "" {
		cc.Database = strings.Trim(u.Path, "/")
	}
	if u.Query() != nil {
		cc.Params = u.Query().Encode()
	}
}

/*
 * An instance of a query-able database; for example, a sql.DB.
 */
type Database interface {
	/*
	 * Runs the query, returning the number of records affected.
	 * If results is not nil, write the results of the query to
	 * it.
	 *
	 * It is assumed that Database will have it's own connection pooling
	 * so that it is safe to call RunQuery from arbitrarily many
	 * goroutines without blocking.
	 */
	RunQuery(results *SafeCSVWriter, query string, args []interface{}) (int64, error)

	/*
	 * Close the database, reclaiming any resources.
	 *
	 * It is illegal to call RunQuery after a database has been closed.
	 */
	Close()
}

// TODO: implement error parsing for mssql and vertica
var supportedDatabaseFlavors = map[string]DatabaseFlavor{
	"mysql":    &sqlDatabaseFlavor{"mysql", mySQLDataSourceName, checkSQLQuery, mySQLErrorCodeParser},
	"mssql":    &sqlDatabaseFlavor{"mssql", sqlServerDataSourceName, checkSQLQuery, unimplementedErrorCodeParser},
	"postgres": &sqlDatabaseFlavor{"postgres", postgresDataSourceName, checkSQLQuery, postgresErrorCodeParser},
	"vertica":  &sqlDatabaseFlavor{"vertica", verticaDataSourceName, checkSQLQuery, unimplementedErrorCodeParser},
}
