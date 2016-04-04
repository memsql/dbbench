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
	"errors"
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
}

/*
 * An instance of a query-able database; for example, a sql.DB.
 */
type Database interface {
	/*
	 * Runs the query, returning the number of records affected.
	 *
	 * It is assumed that Database will have it's own connection pooling
	 * so that it is safe to call RunQuery from arbitrarily many
	 * goroutines without blocking.
	 */
	RunQuery(query string, args []interface{}) (int64, error)

	/*
	 * Close the database, reclaiming any resources.
	 *
	 * It is illegal to call RunQuery after a database has been closed.
	 */
	Close()
}

var supportedDatabaseFlavors = map[string]DatabaseFlavor{
	"mysql":    &sqlDatabaseFlavor{"mysql", mySQLDataSourceName, checkSQLQuery},
	"mssql":    &sqlDatabaseFlavor{"mssql", sqlServerDataSourceName, checkSQLQuery},
	"postgres": &sqlDatabaseFlavor{"postgres", postgresDataSourceName, checkSQLQuery},
}
