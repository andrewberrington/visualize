'''
    convert an LES netcdf file to a raw binary file for vapor
    and write out a script that will turn that file into
    vapor vdf

    example:  python3 wvdf_names.py TABS vdf_name output.nc
'''
from netCDF4 import Dataset
import numpy as np
import argparse
# import textwrap
import sys
import json
import subprocess


def write_error(nc_in):
    namelist = []
    for name, var in nc_in.variables.items():
        if len(var.shape) == 4:
            namelist.append(name)
    return namelist

# filenames = glob.glob('cloudtracker/cloudtracker*.nc')

def dump_bin(filename, varname, outname):
    meters2km = 1.e-3
    print(filename)
    with open(filename, 'r') as f:
        ncfiles = json.load(f)
    num_ts = len(ncfiles['filenames'])
    print(num_ts)
    # to get the x, y, z dimensions of the files (only use the first ncdf file)
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
    # create new shape with num_ts at front
    lenx, leny, lenz = len(xvals), len(yvals), len(zvals)
    the_shape = (num_ts, lenz, leny, lenx)
    string_shape = f'{lenx}x{leny}x{lenz}'
    vdfcreate = '/Applications/VAPOR/VAPOR.app/Contents/MacOS/vdfcreate'
    thecmd = f'{vdfcreate} -xcoords xvals.txt -ycoords yvals.txt -zcoords zvals.txt \
             -gridtype stretched -dimension {string_shape} -vars3d {varname} -numts {num_ts} {outname}.vdf'
    print('debug', thecmd)
    status1, output1 = subprocess.getstatusoutput(thecmd)
    print(status1, output1)
    print(the_shape)
    out_name = '{}.bin'.format(outname)
    print('writing an array of {}(t,x,y,z) shape {}x{}x{}x{}'.format(varname, *the_shape))
    sys.exit(0)
    for t_step, ncfile in enumerate(ncfiles['filenames']):
        with Dataset(ncfile, 'r') as nc_in:
            try:
                var_data = nc_in.variables[varname][...]
                # remember to put args.varname back in above
                print(var_data.shape)
                rev_shape = (var_data.shape[::-1])
                string_shape = "{}x{}x{}".format(*rev_shape)
            except KeyError:
                print('variable names are: ', write_error(nc_in))
                sys.exit(1)
            tmpname = 'temp.bin'
            
            fp = np.memmap(tmpname, dtype=np.float32, mode='w+',
                           shape=var_data.shape)
            fp[t_step, ...] = var_data[...]
            print(np.shape(fp))
            del fp
            raw2vdf = '/Applications/VAPOR/VAPOR.app/Contents/MacOS/raw2vdf'
            thecmd = f'{raw2vdf} -varname {varname} -ts {t_step:d} {outname}.vdf {tmpname}'
            thecmd = f'ls -l {outname}.vdf'
            thecmd = 'pwd ; ls *'
            status2, output2 = subprocess.getstatusoutput(thecmd)
            print(status2, output2)
            return out_name, string_shape


# def dump_script(varname, rev_shape, outname, num_ts):
#     # create a loop here to put multiple timesteps into the outputted vdf file
#     command = r"""
#         #!/bin/bash -v
#         . /Applications/VAPOR/VAPOR.app/Contents/MacOS/vapor-setup.sh
        

#         raw2vdf -varname {var:s} -ts {num_ts:s} {outn:s}.vdf {outn:s}.bin
#     """

#     vars = dict(var=varname, dim=rev_shape, outn=outname, num_ts=num_ts)
#     out = textwrap.dedent(command.format_map(vars)).strip()
#     with open('doit.sh', 'w') as script:
#         script.write(out)
#     print(out)


if __name__ == "__main__":
    linebreaks = argparse.RawTextHelpFormatter
    descrip = globals()['__doc__']
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('cloud_json', help='json file with list of nc files')
    parser.add_argument('varname', help='name of netcdf 3d variable')
    # added new argument outname for the outputted vdf file
    # pass this argument into the appropriate locations of the
    # dump_script and dump_bin functions
    parser.add_argument('outname', help='name of the outputted vdf file')
    # parser.add_argument('num_ts', help='number of timesteps')
    args = parser.parse_args()
    binfile, rev_shape = dump_bin(args.cloud_json, args.varname, args.outname)
    # dump_script(args.varname, rev_shape, args.outname, args.num_ts)