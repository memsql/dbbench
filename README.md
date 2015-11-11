# dbbench

`dbbench` is a tool designed to do one simple task: _to run a workload against a database_ and (optionally) record statistics for post processing.

`dbbench` does *not*:

  - measure the performance of related different queries
  - randomly generate queries, data, etc.
  - do analysis on the data collected
  - etc.

Using this tool, simple 'jobs' can be defined to describe a workload run
against a server. Each job represents a single query; by composing multiple
jobs together, an arbitrary workload can be described.

The jobs are executed against the server and timed to produce
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
a job section. A trivial config file might be:

```ini
[job "test job"]
query=show databases
count=1
```

To run, execute:
```
$ dbbench --host=127.0.0.1 run.ini
```

## Job definitions

There are two major types jobs: repeatedly executing a single query or
replaying a query log.

### Single query jobs

A single query run repeatedly against the database is the most common
type of job. The query can be specified in one of two ways:

  - `query`: The query to be used for this job.
  - `query-file`: The path to a sql file containing exactly one query
     to be used for this job.

By default, a job is run serially (only one query for a given job will be
executing at a time) forever. However, there are many different options for
controlling how a job is run:

  - `count`: Stop running the query after it has executed this many times.
  - `start`: Do not execute this query until the specified time has
      elapsed specified the test has stared.
  - `stop`: Stop executing the query after the specified time has elapsed since
      the test started.
  - `queue-depth`: Allow this many queries from the job to execute simultaneously.
  - `rate`: Execute queries at this rate (in queries per second), allowing
      multiple queries to run simultaneously if necessary.


For example:

```ini
[job "run once"]
query=select sum(a) from mytable
count=1

[job "run one at a time as fast as possible for one minute"]
query-file=select.sql
stop=1m

[job "create a new query every 2 seconds"]
query=select * from mytable where a=b
rate=0.5

[job "run 2 queries at a time for 10 seconds, starting at 5s"]
query=select count(*) from mytable
queue-depth=2
start=5s
stop=15s
```

### Query log jobs

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
query-log=insert.log
start=1s
```
