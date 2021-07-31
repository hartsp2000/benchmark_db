# benchmark_db

Benchmark DB

A Golang app to test various databases

#Requiements

Golang 1.16.x
bash and make support to run scripts
Cassandra, Hbase, Redis and/or Postgres installed and running based
on the type of test(s) that will be run


#Building

To clean already built artifacts

$ make clean

To create new build artifacts

$ make

#Usage

$ bin/benchmark_db

or

$ bin/benchmark_db --help
