# Presto SQL Queries

## Introduction

This repository hosts a set of queries described at a high-level [here](https://github.com/iris-hep/adl-benchmarks-index). The queries are written in SQL for [Presto](https://prestosql.io/). Presto offers a unified front-end to a set of data store technologies such as Cassandra or HDFS. As part of its query interface, it uses a proprietary version of SQL. 

## Installation

To install the Presto server, follow these [instructions](https://prestodb.io/docs/current/installation/deployment.html). You then need to follow these [instructions](https://prestodb.io/docs/current/installation/cli.html) in order to set up a Presto client.

Since deploying the Presto server can involve quite a bit of configuration, we provide a pre-configured `etc` folder which can be downloaded from [here](https://polybox.ethz.ch/index.php/s/TuCtNXTH7XQg0t5/download). Make sure to change the `node.data-dir` property in `etc/node.properties`. This configuration features only one catalog: `memory`. Naturally, more can be added. It should be mentioned, however, that for testing these queries, we have only used the `memory` catalog.

## Setting up the environment

The `scripts` folder contains a number of scripts which are useful towards setting up a database. In order to set up the CERN database, one should follow the next instructions:

1. Start the Presto server
1. Run the `set_up_cern_environment.sh` script

The `set_up_cern_environment.sh` script is pre-configured to use the data in `data/Run2012B_SingleMu-1000.parquet`, however feel free to pass a different dataset (from the same CERN database) as a first parameter to this script.

The `set_up_cern_environment.sh` script makes use of the following scripts (which can also be found in the `scripts` folder):

* `run_presto.sh`: this script is used as a shorthand to submitting SQL queries to the Presto client. Make sure to set the values of the script variables such that it can work on your machine. The following are the variables which might need to be changed.
	* `host`: the hostname of your Presto server deployment
	* `port`: the port of your Presto server deployment
	* `catalog`: by default this is `memory`
	* `output_format`: by default this is `CSV_HEADER`, and should be left as such, unless you also change the references and the `pytest` script
	* `presto_jar`: the path to your Presto client jar 
* `run_presto.sh memory make_db.sql`: this will create the schema, and the table structure.
* `memory create_view.sql`: this will create a view of the database, such that particles are encapsulated in `ROW` type entities.
* `csv_to_sql_insert.py`: this will insert the contents of the database to Presto. The script offers the following options:

```
$python csv_to_sql_insert.py -h
usage: csv_to_sql_insert.py [-h] [--csv CSV] [--table TABLE]
                            [--col_name COL_NAME] [--out_dir OUT_DIR]
                            [--use_cached USE_CACHED]
                            [--cached_path CACHED_PATH] [--catalog CATALOG]
                            [--dump_count DUMP_COUNT]
                            [--presto_script PRESTO_SCRIPT]

optional arguments:
  -h, --help            show this help message and exit
  --csv CSV             Path to the CSV containing the entries
  --table TABLE         The name of the table into which the entries are
                        inserted
  --col_name COL_NAME   The name of the column where the stringified row is
                        temporarily stored.
  --out_dir OUT_DIR     The path to the directory where the intermediary
                        outputs of the script are stored.
  --use_cached USE_CACHED
                        If false, the script executes the entire conversion
                        process from parquet to CSVbefore inserting. Otherwise
                        it uses the results of a previous run.
  --cached_path CACHED_PATH
                        Specifies the location of the cached data. Only used
                        if --cached_path=True.
  --catalog CATALOG     Specifies the catalog to which the table is added.
  --dump_count DUMP_COUNT
                        The number of instances to be inserted into the table
                        at each SQL INSERT.
  --presto_script PRESTO_SCRIPT
                        Path to the script which is used to execute commands
                        over presto.
```

In case you need to change the default values, then make sure to make these changes in `set_up_cern_environment.sh`, in the line calling `csv_to_sql_insert.py`.

## Running Queries

To run the queries, you need to run the `run_presto.sh <path-to-query>` command.

## Testing the Correctness of the Queries

The repository offers a test suite which checks the output of the queries against a set of reference results. These reference results can be found in each query folder, and have the name `ref-1000.csv`. The reference results have been extracted from the `data/Run2012B_SingleMu-1000.parquet` dataset.

The tests can be executed using the `test_queries.py` script located in the root directory. In addition to the default `pytest` options, it offers the following arguments:

```
$python test_queries.py -h  
[...]
  -Q QUERY_ID, --query-id=QUERY_ID
                        Folder name of query to run.
  -F FREEZE_RESULT, --freeze-result=FREEZE_RESULT
                        Whether the results of the query should be persisted to disk.
  -N NUM_EVENTS, --num-events=NUM_EVENTS
                        Number of events taken from the input file. This influences which reference file
                        should be taken.
  -I INPUT_PATH, --input-path=INPUT_PATH
                        Path to input ROOT file.
  -S SCRIPT_PATH, --script-path=SCRIPT_PATH
                        Path to the script which sets up the DB.
  -P PRESTO_CMD, --presto-cmd=PRESTO_CMD
                        Path to the script which sets up the DB.
  --plot-histogram      Plot resulting histogram as PNG file.

```

To run all queries one should use the command `python test_queries.py -v`. To run a specific query, one can use `python test_queries.py -Q <path-to-query-folder>`. 

When running tests, you do not need to run the `set_up_cern_environment.sh` beforehand. This is automatically executed at the beginning of the tests. <font color='red'>Note that the Presto server must be started prior to running the tests!</font>

## Known Execution Issues

* When starting the Presto server, one might see an error of the following sort: `ERROR	main com.facebook.presto.server.PrestoServer	No factory for function namespace manager mysql`, with the top of the stack trace indicating `java.lang.IllegalStateException: No factory for function namespace manager mysql`. Even if `mysql` is selected in the `etc/function-namespace/memory.properties` configuration file, this error can unexpectedly occur. To fix this, it is indicated to use the latest version of Presto server. This was tested on `presto-server-334` and indeed no error was reported.

* Note that Presto has no dedicated means of importing a `.csv` file (see [here](https://github.com/prestodb/presto/issues/11055)). This is the reason why the addition of `.csv` data is done manually using SQL `INSERT` operations. 
