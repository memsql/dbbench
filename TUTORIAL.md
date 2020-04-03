# `dbbench` overview and tutorial

`dbbench` is a tool for running a database workload against a database.
The workload is described in a simple configuration file. Each section
of the configuration file defines a job and how it is executed.

## Hello, world.

The simplest configuration is a single job with a single query:

```ini
[hello world]
query=select "hello world"
```

This configuration describes a workload where a single connection repeatedly
executes the query `select "hello world"` forever. `dbbench` will run
this workload until the user interrupts the process (usually with Ctrl-C):

```console
$ dbbench --host=127.0.0.1 --port=3306 examples/hello_world.ini
2016/04/15 12:57:29 Connecting to root:@tcp(127.0.0.1:3306)/?allowAllFiles=true&interpolateParams=true
2016/04/15 12:57:29 Connected
2016/04/15 12:57:29 starting hello world
2016/04/15 12:57:30 hello world: latency 796.907µs±63.671µs; 1230 transactions (1230.410 TPS); 1230 rows (1230.410 RPS)
2016/04/15 12:57:31 hello world: latency 827.866µs±67.469µs; 1185 transactions (1184.799 TPS); 1185 rows (1184.799 RPS)
^C
2016/04/15 12:57:32 stopping hello world
2016/04/15 12:57:32 hello world: latency 811.522µs±40.603µs; 3113 transactions (1208.336 TPS); 3113 rows (1208.336 RPS)
   131.072µs -    262.144µs [  601]: ██████████████████▋
   262.144µs -    524.288µs [ 1605]: ██████████████████████████████████████████████████
   524.288µs -   1.048576ms [   21]: ▋
  1.048576ms -   2.097152ms [  180]: █████▌
  2.097152ms -   4.194304ms [  705]: █████████████████████▉
  4.194304ms -   8.388608ms [    1]: ▏
```

When run, `dbbench` will output statistics about the workload every second
(controlled by `--intermediate-stats-interval`). For each job, `dbbench` will
report the average latency (and a 99% confidence interval around the
average if there were >30 queries that completed that second), the number of
transactions and records affected, and an estimated transactions per second
and records per second.

When the workload is stopped, statistics accross the entire duration of the
workload are reported for each job. In addition, a histogram of individual
job latency is displayed.

## Setup and teardown

A job can be named any thing other than one of the 3 reserved names:
`setup`, `teardown`, and `global`. The `setup` section runs before
the workload is started and the `teardown` section is run after the
workload has finished (the `global` seciton is currently unused).

For example, the following workload creates a table (named `test_table`),
adds data for testing, and destroys the table after it has finished:

```ini
[setup]
query=create table test_table(a int)
query=insert into test_table values (1), (2), (3)

[teardown]
query=drop table test_table

[select count start]
query=select count(*) from test_table
```

```console
$ dbbench --host=127.0.0.1 --port=3306 --database=testdb examples/select_count_star.ini
2016/04/15 13:27:03 Connecting to root:@tcp(vm:3306)/testdb?allowAllFiles=true&interpolateParams=true
2016/04/15 13:27:03 Connected
2016/04/15 13:27:03 Performing setup
2016/04/15 13:27:03 starting select count start
2016/04/15 13:27:04 select count start: latency 819.207µs±144.644µs; 1197 transactions (1197.676 TPS); 1197 rows (1197.676 RPS)
2016/04/15 13:27:05 select count start: latency 825.986µs±67.612µs; 1192 transactions (1187.266 TPS); 1192 rows (1187.266 RPS)
^C2016/04/15 13:27:06 stopping select count start
2016/04/15 13:27:06 select count start: latency 814.006µs±60.146µs; 3387 transactions (1204.776 TPS); 3387 rows (1204.776 RPS)
   131.072µs -    262.144µs [  952]: ████████████████████████████████▌
   262.144µs -    524.288µs [ 1460]: ██████████████████████████████████████████████████
   524.288µs -   1.048576ms [   24]: ▊
  1.048576ms -   2.097152ms [  218]: ███████▍
  2.097152ms -   4.194304ms [  730]: █████████████████████████
  4.194304ms -   8.388608ms [    1]: ▏
  8.388608ms -  16.777216ms [    1]: ▏
 16.777216ms -  33.554432ms [    0]: 
 33.554432ms -  67.108864ms [    1]: ▏
2016/04/15 13:27:06 Performing teardown
```

> **Tutorial Question: Write a workload that loads data into a table in the setup section. [Check](examples/simple_load_data.ini) your answer when you are done.**

## Using multiple connections
By default, a job runs in a repeatedly in a single connection. There are
2 different ways to control how a job is executed:

  - Add a `concurrency` parameter to the job, which defines the number of
    simultaneous connections that will used by the job. There will be at
    most this many simultaneous instances of the job active at any given
    time. For example, the job in this workload will run the same query
    repeatedly in 10 simulataneous connections.

      ```ini
      [uses 10 connections]
      query=select sleep(1)
      concurrency=10
      ```

  - Add a `rate` parameter to the job, which defines how frequently a batch of
    job instances will be started. `dbbench` will use as many connections
    as are necessary to sustain starting this many job instances per second.
    For example, the job in this workload will run the given query once
    every 10 seconds:

      ```ini
      [once every 10 seconds]
      query=select sleep(100)
      rate=0.1
      ```

    Note that since the query takes 100 seconds to complete but is started
    every 10 seconds, this workload will need to use at least 10 connections.

    If the `batch-size` parameter is provided, that many jobs instances will
    be launched in the batch. For example, the job in this workload will run
    10 simultaneous queries every second:

      ```ini
      [once every second]
      query=select sleep(0.1)
      rate=1
      batch-size=10
      ```

> **Tutorial Question: Write a workload that does 1000 load data queries a minute that all start executing in the first second of the minute. [Check](examples/burst_load_data.ini) your answer when you are done.**

## Parameterizing queries

It is possible to parametrize the queries and fill in values so that each job
instance is unique. Add the `query-args-file` parameter to the job section,
which names a CSV file to be used for query parameters. When all the lines
of the `query-args-file` are consumed, the job will stop. No new instances of
the job will be started, although all running instances of the job will be
quiesced. For this CSV file,

```csv
hello,world
hola,tierra
```

the following workload will execute twice and then stop.

```ini
[concat]
query=select concat(?, ?)
query-args-file=hello_worlds.csv
```

_Note that the `?` syntax for parameters is an artifact of the
[`mysql` driver](https://godoc.org/github.com/go-sql-driver/mysql) -- 
if you are using another driver, you will have to use the parameter
syntax for that driver. For example, the 
[`postgres` driver](https://godoc.org/github.com/lib/pq) uses the
Postgres-native ordinal markers (`$1`, `$2`, etc)._

There is preliminary but untested support to change the delimiter of the
`query-args-file` from comma to any other single character via the
`query-args-delim` parameter. For example,

```ini
[untested parameter]
query=select concat(?, ?)
query-args-file=hello_worlds.tsv
query-args-delim="\t"
```

Note that you can make a 'infinitely' long file with a named pipe:

```console
$ mkfifo /tmp/pipe
$ while true; do echo hello; echo world; done >/tmp/pipe
```

> **Tutorial Question: Write a workload that does a load data of a different file every second. [Check](examples/load_data.ini) your answer when you are done.**

## Stopping a job
There are 3 different ways to stop a job:

  - Add a `duration` parameter to the top level workload configuration, which
    defines when the entire workload will stop. After this time has elapsed,
    no new instances of any job will be started (althogh any active jobs will
    be quiesced). For example,

      ```ini
      duration=10s
   
      [run forever]
      query=select sleep(1)
      ```

  - Add a `stop` parameter to the job configuration, which defines when this
    particular job will stop. After this time has elapsed, no new instances
    of this job will be started (althogh any active jobs will be quiesced).
    For example,

      ```ini
      [run for 10 seconds]
      query=select sleep(1)
      stop=10s
      ```

    Note that there is an `start` parameter for jobs that works in an analogous
    manner.

  - Add a `count` parameter to the job configuraiton, which defines the number
    of times this job will be executed. After this many instances of this job
    have been started, no new instances of this job will be started. For
    example,

      ```ini
      [run 5 times]
      query=select sleep(1)
      count=5
      ```

## Running queries from a file
It is possible to replay queries in parallel from a file in a job. One would want 
to do this if they have a general log or a series of queries that they just want 
to replay without much thought or job control other than an offset from start time. 
To use `query-log-file` in a job:

```ini
[query log file]
query-log-file=/path/to/file
```

The file consists of an offest in microseconds (NOT milliseconds) and a query per 
line separated by a comma:

```ini
0,select count(*) from test_table
500,insert into test_table values (1)
2000,select count(*) from test_table
```

Those queries will essentially be run one after another, however, one can construct 
the offsets to run jobs in parallel. One can also use timestamps for the offset, 
using the first job as the starting point. Example of timestamps (remember to 
convert timestamps in ms to μs):

```ini
1461197566000,select count(*) from test_table
1461198066000,insert into test_table values (1)
1461198566000,select count(*) from test_table
```

Caveats:
  - A job may not use `query-log-file` and `query` at the same time, nor can one use 
    the `query-args-file` with the `query-log-file`.
  - `count` may be used, this will limit the number of queries run from the file to 
    the count value.
  - `rate`, `queue-depth`, and `concurrency` are not allowed.
  - Session variables, transactions and any other stateful operations are unsupported.
  

> **Tutoral Question: Write a query-log that run 4 concurrent sleep(1) queries. When you are done, check the example [`dbbench` config  file](examples/query-log.ini) and [query log file](examples/query.log).**

> **Tutoral Question: Use `tcpdump` to generate a `dbbench` compatible log file. One example is [here](http://codearcana.com/posts/2016/07/21/fast-query-log-with-tcpdump-and-tshark.html).**

## Running repeated queries from a file
Sourcing a query to run repeatedly from a file can be done using `query-file`.
To use `query-file` in a job:

```ini
[query-file]
query-file=/path/to/query-file.sql
```

The file consists of a single query:

```query-file.sql
select "hello world";
```
