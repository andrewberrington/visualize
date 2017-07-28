'''
    convert an LES netcdf or zarr file to a raw binary file for vapor
    and write out a script that will turn that file into
    vapor vdf (uses parquet files to establish the domain)
    example:  python wvdf_timestep_pq.py -json BOMEX_indiv.json -v core -o core_ID
'''
from netCDF4 import Dataset
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import argparse
import sys
import json
import subprocess


def write_error(nc_in):
    namelist = []
    for name, var in nc_in.variables.items():
        if len(var.shape) == 4:
            namelist.append(name)
    return namelist


def dump_bin(filename, varname, outname):
    meters2km = 1.e-3
    print(filename)
    with open(filename, 'r') as f:
        files = json.load(f)
    num_ts = len(files['pq_filenames'])
    pq_filelist = files['pq_filenames']

    # to get z values, which don't change
    # first = pq_filelist[0]
    # table1 = pq.read_table(first).to_pandas()
    # zvals = table1['z'].values * 25. * meters2km

    # to get the x, y extrema, which change depending on the timestep
    x_mins = []
    y_mins = []
    x_maxes = []
    y_maxes = []
    z_maxes = []
    x_mean = []
    y_mean = []
    for f in pq_filelist:
        table = pq.read_table(f).to_pandas()
        x_mins.append(np.amin(table['x'].values))
        y_mins.append(np.amin(table['y'].values))
        x_maxes.append(np.amax(table['x'].values))
        y_maxes.append(np.amax(table['y'].values))
        z_maxes.append(np.amax(table['z'].values))
        x_mean.append(np.mean(table['x'].values))
        y_mean.append(np.mean(table['y'].values))

    x_mins = np.array(x_mins)
    x_maxes = np.array(x_maxes)
    y_maxes = np.array(y_maxes)
    z_maxes = np.array(z_maxes)
    y_mins= np.array(y_mins)

    x_dim = np.amax((x_maxes - x_mins))
    y_dim = np.amax((y_maxes - y_mins))

    # get the start and end indices for the x, y dimensions of the box containing the cloud at a given timestep
    x_indices = np.array([(x_mean - (0.5 * x_dim)), (x_mean + (0.5 * x_dim))]).astype(int)
    y_indices = np.array([(y_mean - (0.5 * y_dim)), (y_mean + (0.5 * y_dim))]).astype(int)

    # in order to define the vdf using vdfcreate
    xvals = np.arange(0, x_dim + 1) * 25. * meters2km
    yvals = np.arange(0, y_dim + 1) * 25. * meters2km
    zvals = np.arange(0, np.amax(z_maxes) + 1) * 25. * meters2km

    filenames = ['xvals.txt', 'yvals.txt', 'zvals.txt']
    arrays = [xvals, yvals, zvals]
    for name, vals in zip(filenames, arrays):
        with open(name, 'w') as outfile:
            [outfile.write('{:6.3f} '.format(item))
            for item in vals[:-1]]
            outfile.write('{:6.3f}\n'.format(vals[-1]))
    lenx, leny, lenz = len(xvals), len(yvals), len(zvals)
    the_shape = (num_ts, lenx, leny, lenz)
    string_shape = f'{lenx}x{leny}x{lenz}'
    vdfcreate = files['vdfcreate']
    thecmd = f'{vdfcreate} -xcoords xvals.txt -ycoords yvals.txt -zcoords zvals.txt \
             -gridtype stretched -dimension {string_shape} -vars3d {varname} -numts {num_ts} {outname}.vdf'
    status1, output1 = subprocess.getstatusoutput(thecmd)
    out_name = '{}.bin'.format(outname)
    print('writing an array of {}(t,x,y,z) shape {}x{}x{}x{}'.format(varname, *the_shape))

    for t_step, ncfile in enumerate(files['var_filenames']):
        with Dataset(ncfile, 'r') as nc_in:
            try:
                var_x = np.arange(x_indices[0][t_step], x_indices[1][t_step]+1)
                var_y = np.arange(y_indices[0][t_step], y_indices[1][t_step]+1)
                var_data = nc_in.variables[varname][..., var_y, var_x]
                print(var_data.shape)
                rev_shape = (var_data.shape[::-1])
                string_shape = "{}x{}x{}".format(*rev_shape)
            except KeyError:
                print('variable names are: ', write_error(nc_in))
                sys.exit(1)
            tmpname = 'temp.bin'
            fp = np.memmap(tmpname, dtype=np.float32, mode='w+',
                           shape=var_data.shape)
            fp[...] = var_data[...]
            del fp
            raw2vdf = files['raw2vdf']
            thecmd = f'{raw2vdf} -varname {varname} -ts {t_step:d} {outname}.vdf {tmpname}'
            status2, output2 = subprocess.getstatusoutput(thecmd)
            print(status2, output2)
    return out_name, string_shape

if __name__ == "__main__":
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('-json', '--cloud_json', dest='cloud_json', help='json file with list of nc files', required=True)
    parser.add_argument('-v', '--varname', dest='varname', help='name of netcdf 3d variable', required=True)
    parser.add_argument('-o', '--outname', dest='outname', help='name of the outputted vdf file', required=True)
    args = parser.parse_args()
binfile, rev_shape = dump_bin(args.cloud_json, args.varname, args.outname)