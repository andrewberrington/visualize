'''
    convert an LES netcdf file to a raw binary file for vapor
    and write out a script that will turn that file into
    vapor vdf

    example:  python3 write_vdf.py TABS output.nc
'''
import glob
from netCDF4 import Dataset
import numpy as np
import pdb
import argparse
import textwrap
import sys


def write_error(nc_in):
    namelist = []
    for name, var in nc_in.variables.items():
        if len(var.shape) == 4:
            namelist.append(name)
    return namelist


def dump_bin(ncfile, varname):
    meters2km = 1.e-3
    with Dataset(ncfile, 'r') as nc_in:
        try:
            var_data = nc_in.variables[args.varname][0, ...]
            print(var_data.shape)
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
            rev_shape = var_data.shape[::-1]
            string_shape = "{}x{}x{}".format(*rev_shape)
        except KeyError:
            print('variable names are: ', write_error(nc_in))
            sys.exit(1)
    out_name = '{}.bin'.format(varname)
    print('writing an array of {}(x,y,z) shape {}x{}x{}'.format(
        varname, *var_data.shape))
    fp = np.memmap(out_name, dtype=np.float32, mode='w+', shape=var_data.shape)
    fp[...] = var_data[...]
    del fp
    return out_name, string_shape


def dump_script(varname, rev_shape):
    command = r"""
        #!/bin/bash -v
        . /Applications/VAPOR/VAPOR.app/Contents/MacOS/vapor-setup.sh
        vdfcreate  -xcoords xvals.txt -ycoords yvals.txt -zcoords zvals.txt \
           -gridtype stretched -dimension {dim:s} -vars3d {var:s} -numts 1 {var:s}.vdf

        raw2vdf -varname {var:s} -ts 0 {var:s}.vdf {var:s}.bin
    """

    vars = dict(var=varname, dim=rev_shape)
    out = textwrap.dedent(command.format_map(vars)).strip()
    with open('doit.sh', 'w') as script:
        script.write(out)
    print(out)


if __name__ == "__main__":
    linebreaks = argparse.RawTextHelpFormatter
    descrip = globals()['__doc__']
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('varname', help='name of netcdf 3d variable')
    parser.add_argument('ncfile', help='netcdf file with les data')
    args = parser.parse_args()
    binfile, rev_shape = dump_bin(args.ncfile, args.varname)
    dump_script(args.varname, rev_shape)
