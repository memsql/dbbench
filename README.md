# dbbench

`dbbench` is a tool designed to do one simple task: _to run a workload against
a database_ and (optionally) record statistics for post processing.

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

To install, first install the dependencies (`golang-go` and `git`). If you are installing `go` for the first time, you will also need
to [set your `GOPATH` environment
variable](https://golang.org/doc/code.html#GOPATH) and add `$GOPATH/bin` to
your `PATH`.

```
go get github.com/memsql/dbbench
```

To run, you first need to create a run file. At minimum, a run file contains
a job section. A trivial config file might be:

```ini
[test job]
query=show databases
count=1
```

To run, execute:
```
$ dbbench --host=127.0.0.1 run.ini
```

## Job definitions

Each job is represented by a section in the config file:

```ini
[my job name]
```

_Note: the names `[setup]`, `[teardown]`, and `[global]` are reserved. See below for more details._

There are two different types jobs: repeatedly executing a single query or
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
[run once]
query=select sum(a) from mytable
count=1

[run one at a time as fast as possible for one minute]
query-file=select.sql
stop=1m

[create a new query every 2 seconds]
query=select * from mytable where a=b
rate=0.5

[run 2 queries at a time for 10 seconds, starting at 5s]
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
[run after one second]
query-log=insert.log
start=1s
```

## Setup / teardown

It is possible to describe (optional) setup and teardown phases
for the workload. These phases constitute queries that are executed
serially before the workload starts. Any number of queries can
be in a setup or a teardown section.

```ini
[setup]
query-file=create_table.sql
query=insert into t select RAND(), RAND()
query=insert into t select RAND(), RAND() from t
query=insert into t select RAND(), RAND() from t
query=insert into t select RAND(), RAND() from t
query=insert into t select RAND(), RAND() from t
query=insert into t select RAND(), RAND() from t

[teardown]
query=drop table t

[count]
query=count(*) from t where a < b
count=30
```

NOTE: since go uses connection pooling, it is unsafe to set
session variables in the setup script if it is possible for
more than one query to execute simultaneously during the workload
phase (e.g. if multiple jobs are specified, or if `rate` or 
`queue-depth` parameters are used) since it is impossible to
guarantee that the same job will re-use the same connection.

## Global configuration

There is an optional global section with one property:

  - `duration`: Stop the test after this amount of time has elapsed.

For example, the following config describes a workload that will stop after 10 seconds:

```ini
duration=10s

[test job]
query=select 1+1
```

# Author
`dbbench` is heavily inspired by [`fio`](https://github.com/axboe/fio). It
was written by Alex Reece <awreece@gmail.com> (Performance Engineer at MemSQL)
to enable flexible testing of a database. He got tired of writing specific test
applications to simulate a given workload, and found that the existing database
benchmark/test tools out there weren't flexible enough to do what he wanted.
