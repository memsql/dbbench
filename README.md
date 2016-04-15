# dbbench

`dbbench` is a fast, lightweight database workload generator that executes a
workload defined a flexible configuration file. Using this `dbbench`, simple
'jobs' can be defined to describe a workload run
against a server. Each job represents a single query; by composing multiple
jobs together, an arbitrary workload can be described. The jobs are executed
against the server and timed to produce
aggregated benchmarking information that is emitted periodically and when the
test completes. Exact job run data can also be logged for additional offline
analysis.

Note that since `dbbench` is a workload driver, it does *not* randomly generate
queries, tables, data, etc. Any random data generation or setup can be easily
done with SQL queries.

## Getting started

To install, first install the dependencies (`golang-go` and `git`). If you are installing `go` for the first time, you will also need
to [set your `GOPATH` environment
variable](https://golang.org/doc/code.html#GOPATH) and add `$GOPATH/bin` to
your `PATH`.

```console
$ mkdir $HOME/go
$ export GOPATH=$HOME/go
$ export PATH=$PATH:$GOPATH/bin
```

Once `go` has been set up, use the `go` tool to get `dbbench`.

``` console
$ go get github.com/memsql/dbbench
```

You can also use the `go` tool to update `dbbench`:

```console
$ go get -u github.com/memsql/dbbench
```

## Running `dbbench`

To learn how to run `dbbench`, follow the [tutorial](TUTORIAL.md).

## Author
`dbbench` is heavily inspired by [`fio`](https://github.com/axboe/fio). It
was written by Alex Reece <awreece@gmail.com> (Performance Engineer at MemSQL)
to enable flexible testing of a database. He got tired of writing specific test
applications to simulate a given workload, and found that the existing database
benchmark/test tools out there weren't flexible enough to do what he wanted. For more
context about the ethos of `dbbench`, see the
[blog post](http://blog.memsql.com/dbbench-active-benchmarking/) that introduced it.
