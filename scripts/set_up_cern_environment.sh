#!/usr/bin/env bash
catalog=memory

# Change to the script directory
cd "$(dirname "$0")"

# Crete the schema, insert the data 
./run_presto.sh ${catalog} make_db.sql
python3 csv_to_sql_insert.py

# Restructure the data into a new view
./run_presto.sh ${catalog} create_view.sql