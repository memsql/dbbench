# dbbench

A simple tool for benchmarking a SQL database.

Using this tool, simple 'jobs' can be defined to describe a workload run
against a server. The jobs are executed against the server and timed to produce
aggregated benchmarking information that is emitted periodically and when the
test completes. Exact job run data can also be logged for additional offline
analysis.

## Getting started

To install, first install the dependencies (`golang-go`, `git`, and
`mercurial`). If you are installing `go` for the first time, you will also need
to [set your `GOPATH` environment
variable](https://golang.org/doc/code.html#GOPATH) and add `$GOPATH/bin` to
your `PATH`.

```
go get github.com/memsql/dbbench
```

To run, you first need to create a run file. At minimum, a run file contains
two sections: the global section and a job section. A trivial config file
might be:

```ini
[global]
duration=1s

[job "test job"]
query=show databases
count=1
```

To run, execute:
```
$ dbbench --host=127.0.0.1 run.ini
```

## Job definitions

A job is a single workload run from a specified start time
until a specified end time or a specified count of queries has occured.
By default, a job starts when the test begins
and continues until the test ends with no limit on the number of queries.

There are two major types jobs: repeatedly executing a single query or
replaying a query log.

When repeatedly executing a single query, it is possible to specify either
a maximum queue depth (the number of in-flight queries) or the target
rate of queries (the number of queries executed every second). The single
query can either by provided by a `query` or a `queryfile` line in the job
config file. For example:

```ini
[job "run once"]
query=select * from mytable
count=1

[job "run for one second"]
queryfile=select.sql
stop=1s
```

The query log format is a flat text file with a time in microseconds and
a query separated by a comma. For example:

```
8644882534,insert into mytable values (1)
8644885780,insert into mytable values (2)
8644888687,insert into mytable values (3)
```

The first query is when the job starts and every other query is executed after
the correct delay from the initial query. For example, the job config below
with the query log above would execute an insert at 1.000s, 1.003s, and 1.006s:

```ini
[job "run after one second"]
querylog=insert.log
start=1s
```
