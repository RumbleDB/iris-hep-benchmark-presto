#!/usr/bin/env bash

# Setting up some useful parameters
host=localhost
port=8080
catalog=memory
output_format=CSV_HEADER
presto_jar=/home/dan/data/software/presto-client/presto.jar

if [[ $# != 1 ]]
then
	echo "Usage: run_presto.sh <sql_file>"
	exit 1 	
fi

$presto_jar --server ${host}:${port} --catalog ${catalog} --schema default --output-format ${output_format} --file $1