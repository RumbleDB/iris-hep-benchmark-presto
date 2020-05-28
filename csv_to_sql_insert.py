import pandas
import re

from os.path import join
from os import getcwd
from subprocess import run

CSV_PATH = '/home/dan/data/garbage/cern_queries/presto/data/Run2012B_SingleMu_small.parquet'
TABLE_NAME = "memory.cern.Run2012B_SingleMu_small"


def make_sql_insert_script(path, col_name='str'):
	data = pandas.read_csv(path)	
	with open("insert_data.sql", "w") as f:
		f.write("-- Insert data into the `companies` table\nINSERT INTO {} VALUES\n".format(TABLE_NAME))

		for i, row in data.iterrows():
			f.write(",\n\t(" + row[col_name] + ")" if i != 0 else "\t(" + row[col_name] + ")")

		f.write(";")

def gradually_insert(path, col_name='str', catalog='memory'):
	"""
	Gradually inserts the rows in the DF located at path. The rows
	should already be concatenated together into a string under 
	the column col_name.

	:param path: the path to the DF
	:param col_name: the column under which the stringified rows are
					 located
	:param catalog: the presto catalog which will be used
	"""
	def _execute_command(q):
		with open("temp.sql", "w") as f:
			f.write(collector)
		run(['run_presto.sh', catalog, "temp.sql"])  

	data = pandas.read_csv(path)
	collector = cl = "INSERT INTO {} VALUES".format(TABLE_NAME) 

	for i, row in data.iterrows():
		if i % 100 == 0 and i != 0:
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


def stringify_data(path, col_name="str", out_name="stringified.csv"):
	"""
	Read a parquet file as a DF, and add a new column whose name is specified  
	in the col_name parameter. This column holds the row's other columns 
	under concatenated string format. The new DF is saved under the name 
	indicated by out_name. 

	:param path: the path to the DF
	:param col_name: the name of the new column
	:param out_name: the name of the output csv
	"""
	data = pandas.read_parquet(path)
	data[col_name] = data[data.columns].astype(str).apply(lambda x: ', '.join(x), axis=1)
	data.to_csv(out_name, index=False)


def add_commas(path, col_name="str", out_name="stringified_new.csv"):
	"""
	Processes the stringified rows, and makes sure the data is in a format 
	that presto accepts when inserting.

	:param path: the path to the DF
	:param col_name: the name of the column with the stringified data 
	:param out_name: the name of the output csv
	"""
	def _inner_transform(x):
		x = re.sub(' +', ',', x)
		x = re.sub(",+", ",", x)
		x = re.sub("\[,", "[", x)
		x = re.sub(",\]", "]", x)
		x = re.sub("nan", "null", x)
		return re.sub("\[", "ARRAY[", x)

	data = pandas.read_csv(path)
	data[col_name] = data[col_name].apply(_inner_transform)
	data.to_csv(out_name, index=False)


if __name__ == '__main__':
	# stringify_data(CSV_PATH)
	# add_commas("stringified.csv")
	gradually_insert("stringified_new.csv")
