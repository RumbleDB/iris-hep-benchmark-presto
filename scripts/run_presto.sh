#!/usr/bin/env bash

if [[ $# != 2 ]]
then
	echo "Usage: run_presto.sh <catalog> <sql_file>"
	exit 1 	
fi

presto_jar=/home/dan/data/software/presto-client/presto.jar
$presto_jar --server localhost:8080 --catalog $1 --schema default --file $2