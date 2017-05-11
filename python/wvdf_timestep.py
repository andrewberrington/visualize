'''
    convert an LES netcdf file to a raw binary file for vapor
    and write out a script that will turn that file into
    vapor vdf

    example:  python wvdf_timestep.py bomex variable_name vdf_name
'''
from netCDF4 import Dataset
import numpy as np
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
        ncfiles = json.load(f)
    num_ts = len(ncfiles['filenames'])
    with Dataset(ncfiles['filenames'][0], 'r') as nc_in:
        xvals = nc_in.variables['x'][:] * meters2km
        yvals = nc_in.variables['y'][:] * meters2km
        zvals = nc_in.variables['z'][:] * meters2km
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
    vdfcreate = ncfiles['vdfcreate']
    thecmd = f'{vdfcreate} -xcoords xvals.txt -ycoords yvals.txt -zcoords zvals.txt \
             -gridtype stretched -dimension {string_shape} -vars3d {varname} -numts {num_ts} {outname}.vdf'
    print('debug', thecmd)
    status1, output1 = subprocess.getstatusoutput(thecmd)
    print(status1, output1)
    out_name = '{}.bin'.format(outname)
    print('writing an array of {}(t,x,y,z) shape {}x{}x{}x{}'.format(varname, *the_shape))
    for t_step, ncfile in enumerate(ncfiles['filenames']):
        with Dataset(ncfile, 'r') as nc_in:
            try:
                var_data = nc_in.variables[varname][...]
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
            raw2vdf = ncfiles['raw2vdf']
            thecmd = f'{raw2vdf} -varname {varname} -ts {t_step:d} {outname}.vdf {tmpname}'
            status2, output2 = subprocess.getstatusoutput(thecmd)
            print(status2, output2)
    return out_name, string_shape

if __name__ == "__main__":
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('-json', '--cloud_json', help='json file with list of nc files', required=True)
    parser.add_argument('-v', '--varname', help='name of netcdf 3d variable', required=True)
    parser.add_argument('-o', '--outname', help='name of the outputted vdf file', required=True)
    args = parser.parse_args()
    binfile, rev_shape = dump_bin(args.cloud_json, args.varname, args.outname)