#!/usr/bin/env python3

from abc import ABC, abstractmethod
import io
import json
import logging
from matplotlib import pyplot as plt
import numpy as np
from os.path import dirname, join
import subprocess
import sys
import time

import pandas as pd
import pytest
import requests
from urllib.parse import quote_plus, urlencode
import warnings


# This superclass might be useful in case other means
# of querying Presto are added in the future 
class PrestoProxy(ABC):
  @abstractmethod
  def run(self, query_file, other_params):
    pass


class PrestoCliProxy(PrestoProxy):
  def __init__(self, cmd):
    self.cmd = cmd

  def run(self, query_file):
    # Assemble command
    cmd = [self.cmd, query_file]

    # Run query and read result
    output = subprocess.check_output(cmd, encoding='utf-8')
    return io.StringIO(output)


@pytest.fixture(scope="session")
def setup_db(pytestconfig):
  cmd = [
      pytestconfig.getoption('script_path'), 
      pytestconfig.getoption('input_path')
  ]
  logging.info('Running setup command %s', cmd[0])
  logging.info('Using dataset %s', cmd[1])
  subprocess.run(cmd)
  return True


@pytest.fixture(scope="function")
def presto(pytestconfig):
  # By default use the CLI
  presto_cmd = pytestconfig.getoption('presto_cmd')
  logging.info('Using executable %s', presto_cmd)
  return PrestoCliProxy(presto_cmd)


def test_query(query_id, pytestconfig, presto, setup_db):
    num_events = pytestconfig.getoption('num_events')
    num_events = ('-' + str(num_events)) if num_events else ''

    root_dir = join(dirname(__file__))
    query_dir = join(root_dir, 'queries', query_id)
    query_file = join(query_dir, 'query.sql')
    ref_file = join(query_dir, 'ref{}.csv'.format(num_events))
    png_file = join(query_dir, 'plot{}.png'.format(num_events))

    # Run query and read result
    start_timestamp = time.time()
    output = presto.run(query_file)
    end_timestamp = time.time()
    df = pd.read_csv(output, dtype= {'x': np.float64, 'y': np.int32})
    print(df)

    running_time = end_timestamp - start_timestamp
    logging.info('Running time: {:.2f}s'.format(running_time))

    # Freeze reference result
    if pytestconfig.getoption('freeze_result'):
      df.to_csv(ref_file, index=False)

    # Read reference result
    df_ref = pd.read_csv(ref_file, dtype= {'x': np.float64, 'y': np.int32})
    print(df_ref)

    # Plot histogram
    if pytestconfig.getoption('plot_histogram'):
      plt.hist(df.x, bins=len(df.index), weights=df.y)
      plt.savefig(png_file)

    # Normalize reference and query result
    df = df[df.y > 0]
    df = df[['x', 'y']]
    df.reset_index(drop=True, inplace=True)
    df_ref = df_ref[df_ref.y > 0]
    df_ref = df_ref[['x', 'y']]
    df_ref.reset_index(drop=True, inplace=True)

    # Assert correct result
    pd.testing.assert_frame_equal(df_ref, df)


if __name__ == '__main__':
    pytest.main(sys.argv)
