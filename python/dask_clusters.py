"""
   creation of a dask dataframe from GATE parquet files
"""

from dask.dataframe import read_parquet
import numpy as np
import pandas as pd
from pathlib import Path
import glob
from multiprocessing.pool import ThreadPool
import dask
import timeit
import sys
import argparse
import json


def sort_parq(filename):
    """
    sort key returns 241 with filename
         clusters_00000030_241.parq
    """
    the_name=Path(filename).name
    parts=the_name.split('_')
    number=parts[2].split('.')
    out=int(number[0])
    return out


def make_ddf(args):
    pool = ThreadPool(processes=int(args.thread_num))
    file_list=list(glob.glob(f'{args.directory}/*.parq'))
    file_list.sort(key=sort_parq)
    select = []
    for f in file_list:
        parts = f.split('_')
        if int(parts[1]) == int(args.timestep):
            select.append(f)
    select = select[0:int(args.number)]
    with dask.set_options(pool=pool):
        da_frame=read_parquet(select[0],index='index')
        for new_file in select[1:]:
            new_frame=read_parquet(new_file,index='index')
            da_frame=dask.dataframe.multi.concat([da_frame,new_frame],interleave_partitions=False)
    df_core = da_frame[['cloud_id', 'type', 'coord']]
    df_core = df_core[df_core.type == 1]
    df_id = df_core.groupby('cloud_id').count()
    out = df_id.compute()
    out = out.sort_values('type',ascending=False)
    out = out[0:100]
    print(out)


def make_parser():
    '''
    command line arguments for calling program
    '''
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('-d', '--dir', dest='directory', help='path to directory containing parquet files', required=True)
    parser.add_argument('-ts', '--tstep', dest='timestep', help='timestep to pull from parquet files', type=str, required=True)
    parser.add_argument('-n', '--num', dest='number', help='number of parquet files to pull from given timestep', type=str, required=True)
    parser.add_argument('-th' '--threads', dest='thread_num', help='number of threads for thread pool', type=str, required=True)
    return parser


def main(args=None):
    '''
    args are required
    '''
    parser = make_parser()
    args = parser.parse_args(args)
    make_ddf(args)


if __name__ == "__main__":
    sys.exit(main())