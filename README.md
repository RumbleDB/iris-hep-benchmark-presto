# Presto SQL Queries

## Introduction

This repository hosts the queries described [here](https://github.com/iris-hep/adl-benchmarks-index). The queries are written in SQL for [Presto](https://prestosql.io/). As its name suggests, the Presto version is meant to be run with Presto and any back end storage system it supports (although it was currently only tested for the *memory* back end). As part of this project we are trying to study the limitations of various flavors of the SQL query language.

Presto is a framework which provides a unified front-end to a set of data store technologies such as Cassandra or HDFS. It allows querying using a proprietary version of SQL.

## Installation

To install the Presto server, follow these [instructions](https://prestodb.io/docs/current/installation/deployment.html). You then need to follow these [instructions](https://prestodb.io/docs/current/installation/cli.html) in order to set up a Presto client.

Since deploying the Presto server can involve quite a bit of configuration file management, this repository contains an instance of the `etc` folder required for deployment. This can be found in `presto/server_config/etc.zip`. Make sure to change the `node.data-dir` property in `etc/node.properties`. Also, you might want to change the IP and port in `etc/function-namespace/memory.properties` such that it fits to your `mysql` configuration. This configuration features only one catalog: `memory`. Naturally, more can be added, however, the next sections in the tutorial will make the assumption that `memory` is used.

## The Data

The `presto/scripts` folder contains a number of scripts which are useful towards building a database. It is generally also a good idea to add this folder to the `PATH` variable, as it makes executing queries more straight forward. In order to set up the CERN database, one should follow the next instructions:

1. Start the Presto server
1. Execute `run_presto.sh memory make_db.sql`. This will create the schema, and the table structure.
1. Execute `python csv_to_sql_insert.py`. This will insert the contents of the database to Presto. Make sure to look into this script, and change the paths, such that `CSV_PATH` points to your initial file location, and the `out_name` parameters point to valid output file locations.
1. Execute `run_presto.sh memory create_view.sql`. This will create a view of the database, such that particles are encapsulated in `row` type entities.

By following these queries, your Presto distribution should have the CERN database inserted in the memory catalog.

## Running Queries

Make sure the `presto/scripts` folder is added to the `PATH` variable. This will make running queries straight forward. Assuming this step has been done, all one needs to do is to navigate to a query folder (e.g. `presto/queries/q1`) and execute a query with the following structure `run_presto.sh memory <query_file_name>`.

## Known Execution Issues

* When starting the Presto server, one might see an error of the following sort: `ERROR	main com.facebook.presto.server.PrestoServer	No factory for function namespace manager mysql`, with the top of the stack trace indicating `java.lang.IllegalStateException: No factory for function namespace manager mysql`. Even if `mysql` is selected in the `etc/function-namespace/memory.properties` configuration file, this error can unexpectedly occur. To fix this, it is indicated to use the latest version of Presto server. This was tested on `presto-server-334` and indeed no error was reported.

* Note that Presto has no dedicated means of importing a `.csv` file (see [here](https://github.com/prestodb/presto/issues/11055)). This is the reason why the addition of `.csv` data is done manually using SQL `INSERT` operations. 
