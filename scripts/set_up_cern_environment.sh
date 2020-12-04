#!/usr/bin/env bash
data_path="../data/Run2012B_SingleMu-1000.parquet"

if [[ $# != 1 ]]; then
	data=$1
fi

# Change to the script directory
cd "$(dirname "$0")"

# Crete the schema, insert the data 
./run_presto.sh make_db.sql
python3 csv_to_sql_insert.py --csv=${data_path}

# Restructure the data into a new view
./run_presto.sh create_view.sql