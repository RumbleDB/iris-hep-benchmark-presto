import argparse
import pandas
import re
import os
from os.path import join
from os import getcwd
from subprocess import run


parser = argparse.ArgumentParser()
parser.add_argument("--csv", type=str, default="../data/Run2012B_SingleMu-1000.parquet", 
    help="Path to the CSV containing the entries")
parser.add_argument("--table", type=str, default="memory.cern.Run2012B_SingleMu_small", 
    help="The name of the table into which the entries are inserted")
parser.add_argument("--col_name", type=str, default="str", 
    help="The name of the column where the stringified row is temporarily stored.")
parser.add_argument("--out_dir", type=str, default="../data", 
    help="The path to the directory where the intermediary outputs of the script are stored.")
parser.add_argument("--use_cached", type=bool, default=False, 
    help="If false, the script executes the entire conversion process from parquet to CSV" \
        "before inserting. Otherwise it uses the results of a previous run.")
parser.add_argument("--cached_path", type=str, default="../data/stringified_new.csv", 
    help="Specifies the location of the cached data. Only used if --cached_path=True.")
parser.add_argument("--catalog", type=str, default="memory", 
    help="Specifies the catalog to which the table is added.")
parser.add_argument("--dump_count", type=int, default=400, 
    help="The number of instances to be inserted into the table at each SQL INSERT.")
parser.add_argument("--presto_script", type=str, default="run_presto.sh", 
    help="Path to the script which is used to execute commands over presto.")


def stringify_data(path, col_name="str", out_dir="../data", out_name="stringified.csv"):
  # Create the folder where the data is saved
  if not os.path.exists(out_dir):
    os.mkdir(out_dir)
  out_path = join(out_dir, out_name)

  data = pandas.read_parquet(path)
  data[col_name] = data[data.columns].astype(str).apply(lambda x: ', '.join(x), axis=1)
  data.to_csv(out_path, index=False)
  return col_name, out_path


def add_commas(path, col_name="str", out_dir="../data", out_name="stringified_new.csv"):
  def _inner_transform(x):
    x = re.sub(' +', ',', x)
    x = re.sub(",+", ",", x)
    x = re.sub("\[,", "[", x)
    x = re.sub(",\]", "]", x)
    x = re.sub("nan", "null", x)
    return re.sub("\[", "ARRAY[", x)

  data = pandas.read_csv(path)
  data[col_name] = data[col_name].apply(_inner_transform)
  out_path = join(out_dir, out_name)
  data.to_csv(out_path, index=False)
  return out_path


def gradually_insert(path, col_name='str', catalog='memory', dump_count=400, 
  table_name="memory.cern.Run2012B_SingleMu_small", presto_script="../run_presto.sh"):
  def _execute_command(q):
    with open("temp.sql", "w") as f:
      f.write(collector)
    run([f"./{presto_script}", catalog, "temp.sql"])  

  data = pandas.read_csv(path)
  collector = cl = "INSERT INTO {} VALUES".format(table_name) 

  for i, row in data.iterrows():
    if i % dump_count == 0 and i != 0:
      print("At row:", i)
      collector += " (" + row[col_name] + ");"
      _execute_command(collector)
      collector = cl 
    else:
      collector += " (" + row[col_name] + "),"

  # if there some extra rows that need to be added
  if collector != cl:
    collector = collector[:-1] + ";"
    _execute_command(collector)


if __name__ == '__main__':
  args = parser.parse_args()

  if args.use_cached:
    gradually_insert(args.cached_path, col_name=args.col_name, catalog=args.catalog, 
        dump_count=args.dump_count, table_name=args.table, presto_script=args.presto_script)
  else:
    col_name, save_path = stringify_data(args.csv, col_name=args.col_name, out_dir=args.out_dir)
    save_path = add_commas(save_path, col_name=col_name, out_dir=args.out_dir)
    gradually_insert(save_path, col_name=col_name, catalog=args.catalog, dump_count=args.dump_count, 
        table_name=args.table, presto_script=args.presto_script)
