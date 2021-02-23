#!/usr/bin/env bash

# Setting up some useful parameters
host=localhost
port=8080
catalog=memory
presto_jar=/home/dan/data/software/presto-client/presto.jar

$presto_jar --server ${host}:${port} --catalog ${catalog} --schema default "$@"
