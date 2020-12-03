#!/usr/bin/env bash

# Setting up some useful parameters
host=localhost
port=8080


if [[ $# != 2 ]]
then
	echo "Usage: run_presto.sh <catalog> <sql_file>"
	exit 1 	
fi

presto_jar=/home/dan/data/software/presto-client/presto.jar
$presto_jar --server ${host}:${port} --catalog $1 --schema default --file $2