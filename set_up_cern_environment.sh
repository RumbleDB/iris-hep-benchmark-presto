#!/usr/bin/env bash
catalog=memory

./run_presto.sh ${catalog} create_view.sql
./run_presto.sh ${catalog} make_db.sql
python csv_to_sql_insert.py