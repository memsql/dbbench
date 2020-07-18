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
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/vertica/vertica-sql-go"
)

func cancelOnInterrupt(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		signal.Stop(c)
		cancel()
		close(c)
	}()
}

func runTest(db Database, df DatabaseFlavor, config *Config) {
	if len(config.Setup) > 0 {
		log.Printf("Performing setup")
		for _, query := range config.Setup {
			if _, err := db.RunQuery(nil, query, nil); err != nil {
				log.Fatalf("error in setup query %q: %v", query, err)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelOnInterrupt(cancel)
	if config.Duration > 0 {
		ctx, _ = context.WithTimeout(ctx, config.Duration)
	}

	testStats := processResults(config, makeJobResultChan(ctx, db, df, config.Jobs))

	for name, stats := range testStats {
		log.Printf("%s: %v", name, stats)
	}

	if len(config.Teardown) > 0 {
		log.Printf("Performing teardown")
		for _, query := range config.Teardown {
			if _, err := db.RunQuery(nil, query, nil); err != nil {
				log.Fatalf("error in teardown query %q: %v", query, err)
			}
		}
	}
}

var driverName = flag.String("driver", "mysql", "Database driver to use.")
var baseDir = flag.String("base-dir", "",
	"Directory to use as base for files (default directory containing runfile).")

var printVersion = flag.Bool("version", false, "Print the version and quit")

var GlobalConfig ConnectionConfig

func init() {
	flag.StringVar(&GlobalConfig.Username, "username", "",
		"Database connection username")
	flag.StringVar(&GlobalConfig.Password, "password", "",
		"Database connection password")
	flag.StringVar(&GlobalConfig.Host, "host", "",
		"Database connection host")
	flag.IntVar(&GlobalConfig.Port, "port", 0,
		"Database connection port")
	flag.StringVar(&GlobalConfig.Database, "database", "",
		"Database connection database")
	flag.StringVar(&GlobalConfig.Params, "params", "",
		"Override default connection parameters")
}

func main() {
	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s [options] <runfile.ini>\n", os.Args[0])
		flag.PrintDefaults()
	}

	if *printVersion {
		fmt.Println("0.3.1")
		return
	}

	if len(flag.Args()) == 0 {
		flag.Usage()
		log.Fatal("No config file to parse")
	}
	if len(flag.Args()) > 1 {
		flag.Usage()
		log.Fatal("Cannot have more than one config file (do you have flags after the config file??)")
	}
	configFile := flag.Arg(0)
	if *baseDir == "" {
		*baseDir = filepath.Dir(configFile)
	}

	flavor, ok := supportedDatabaseFlavors[*driverName]
	if !ok {
		log.Fatalf("Database flavor %s not supportd", *driverName)
	}

	config, err := parseConfig(flavor, configFile, *baseDir)
	if err != nil {
		log.Fatalf("parsing config file %v", err)
	}

	if db, err := flavor.Connect(&GlobalConfig); err != nil {
		log.Fatal("Error connecting to the database: ", err)
	} else {
		defer db.Close()

		os.Chdir(*baseDir)
		runTest(db, flavor, config)
	}
}
