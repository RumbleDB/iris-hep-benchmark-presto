#!/usr/bin/env bash
catalog=memory

./run_presto.sh ${catalog} make_db.sql
python3 csv_to_sql_insert.py
./run_presto.sh ${catalog} create_view.sql