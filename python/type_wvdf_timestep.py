'''
    convert an LES zarr file to a raw binary file for vapor and turn that file into vapor vdf (using parquet files to establish coordinates)
    example: python type_wvdf_timestep.py -json 16936.json -v QN -t condensed -o QN_condensed
'''
import zarr
import pyarrow.parquet as pq
import numpy as np
import argparse
import os
import glob
import sys
import json
import subprocess
import itertools
from collections import defaultdict


def write_error(the_in):
    namelist = []
    for name, var in the_in.variables.items():
        if len(var.shape) == 4:
            namelist.append(name)
    return namelist

# the following function will eventually be separated into a different module
# and imported to be used in dump_bin


def process_pq(pq_list, the_type):
    '''
    function to process a list of pq files and return appropriate
    3D coordinates
    '''

    keys = {
        "condensed": 0,
        "condensed_edge": 1,
        "condensed_env": 2,
        "condensed_shell": 3,
        "core": 4,
        "core_edge": 5,
        "core_env": 6,
        "core_shell": 7,
        "plume": 8,
        }

    # to get both the full domain of x and y along with the x and y coordinates
    # for the given type (condensed, core, etc.)

    full = defaultdict(list)
    sub = defaultdict(list)
    extrema = defaultdict(list)
    for f in pq_list:
        table = pq.read_table(f).to_pandas()
        c_id = table['cloud_id'].values[0]
        if the_type == 'full' or the_type == 'base':
            tablerows = table['type'] == keys["condensed"]
        else:
            tablerows = table['type'] == keys[the_type]
        df_thetype = table[tablerows]
        for dimension in ['x', 'y', 'z']:
            for suffix in ['full', 'sub']:
                if suffix == 'full':
                    full[(dimension, suffix)].append(table[dimension].values)
                else:
                    sub[(dimension, suffix)].append(df_thetype[dimension].values)
            for suffix in ['min', 'max']:
                if suffix == 'min':
                    extrema[(dimension, suffix)].append(np.amin(table[dimension].values))
                    sub[(dimension, suffix)].append(np.amin(df_thetype[dimension].values))
                else:
                    extrema[(dimension, suffix)].append(np.amax(table[dimension].values))
                    sub[(dimension, suffix)].append(np.amax(table[dimension].values))
    return full, sub, c_id, extrema


def dump_bin(filename, varname, tracktype, outname):
    '''
       establishes the vdf using vdfcreate and the coordinates 
       from the parquet files, then pulls data from zarr files
       to populate the vdf with data using raw2vdf
       
       Parameters
       ----------

       these are required to be inputted from the command line
       when calling the script

       filename: string
          name of the json file containing the metadata
       
       varname: string
          name of the LES variable to be visualized (e.g. QN, W, TR01)

       tracktype: string
          cloud tracking type (e.g. core, condensed, plume)

       outname: string
          name of the outputted vdf file

       Returns
       -------

       xvals.txt: txt file
          txt file containing the x coordinates of the vdf

       yvals.txt: txt file
          txt file containing the y coordinates of the vdf

       zvals.txt: txt file
          txt file containing the z coordinates of the vdf

       temp.bin: binary file
          binary file used in establishing the memory map for raw2vdf

       outname_cloud_id.vdf: Vapor data format file
          the vdf file to be loaded and visualized in Vapor

       outname_cloud_id_data: directory
          directory containing the data to be read into Vapor using the vdf file
    '''

    meters2km = 1.e-3
    print(filename)
    with open(filename, 'r') as f:
        files = json.load(f)
    num_ts = len(files['pq_filenames'])
    pq_filelist = sorted(files['pq_filenames'])

    fulldict, subdict, cloud_id, extdict = process_pq(pq_filelist, tracktype)

    min_x, max_x = np.amin(extdict[('x', 'min')]), np.amax(extdict[('x', 'max')])
    min_y, max_y = np.amax(extdict[('y', 'min')]), np.amax(extdict[('x', 'max')])

    # to handle the cases where the cloud crosses a boundary
    domain = 256

    x = np.array(list(itertools.chain.from_iterable(fulldict[('x', 'full')])))
    y = np.array(list(itertools.chain.from_iterable(fulldict[('y', 'full')])))
    # hardcoded for bomex currently
    off_x = 0
    off_y = 0
    if (max_x - min_x) > (domain / 2):
        off_x = domain - np.min(x[(x > domain / 2)])
    if (max_y - min_y) > (domain / 2):
        off_y = domain - np.min(y[(y > domain / 2)])

    # define the full domain of the vdf
    # currently disabled
    # x_mean = []
    # y_mean = []
    # width_y_full = []
    # width_x_full = []
    # for x, y in zip(xvals_full, yvals_full):
    #     width_x_full.append(np.amax(x) - np.amin(x))
    #     width_y_full.append(np.amax(y) - np.amin(y))
    #     x_mean.append(np.mean(x))
    #     y_mean.append(np.mean(y))

    # x_dim_full = np.amax(width_x_full)
    # y_dim_full = np.amax(width_y_full)

    # get the start and end indices for the x, y dimensions of the box
    # containing the cloud at a given timestep
    x_indices = np.array([0, 256]).astype(int)
    y_indices = np.array([0, 256]).astype(int)

    # in order to define the vdf using vdfcreate
    # eventually will add the ability to change resolution for other datasets
    # for now is hardcoded to establish the vdf for BOMEX only
    xvals = np.arange(0, x_indices[1]) * 25. * meters2km
    yvals = np.arange(0, y_indices[1]) * 25. * meters2km
    zvals = np.arange(0, np.amax(extdict[('z', 'max')]) + 1) * 25. * meters2km

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
             -gridtype stretched -dimension {string_shape} -vars3d {varname}_{tracktype} -numts {num_ts} {outname}_{cloud_id}.vdf'
    status1, output1 = subprocess.getstatusoutput(thecmd)
    out_name = '{}.bin'.format(outname)
    print('writing an array of {}(t,x,y,z) shape {}x{}x{}x{}'.format(varname, *the_shape))

    for t_step, the_file in enumerate(files['var_filenames']):
        the_in = zarr.open_group(the_file, mode='r')
        try:
            # startx, stopx = x_indices[0], x_indices[1]
            # starty, stopy = y_indices[0], y_indices[1]
            # extra slice 0 is there to remove the time dimension from the zarr data
            if varname == 'rv':
                import calc_vars as cv
                # get vapor mixing ratio in kg/kg
                var_data = cv.calc_rv(the_in)
            elif varname == 'td':
                import calc_vars as cv
                # get dewpoints in K
                var_data = cv.calc_Td(the_in)
            else:
                var_data = the_in[varname][:][0]
            x_r = subdict[('x', 'sub')][t_step]
            y_r = subdict[('y', 'sub')][t_step]
            z = subdict[('z', 'sub')][t_step]
            if off_x > 0:
                var_data = np.roll(var_data, off_x, axis=2)
                x_r = x_r + off_x
                x_r[x_r > domain - 1] = x_r[x_r > domain - 1] - domain - 1
            if off_y > 0:
                var_data = np.roll(var_data, off_y, axis=1)
                y_r = y_r + off_y
                y_r[y_r > domain - 1] = y_r[y_r > domain - 1] - domain - 1
            # only map the values that are valid for the given type
            indices = np.array((z, y_r, x_r))
            b_map = np.zeros_like(var_data, dtype=bool)
            if tracktype == 'full':
                b_map[:] = True
            elif tracktype == 'base':
                z_base = subdict[('z', 'min')][t_step]
                b_map[z_base, y_r, x_r] = True
            else:
                b_map[tuple(indices)] = True
            var_data[~b_map] = 0
            var_data = np.ma.masked_values(var_data, 0)
            var_data = var_data[:, :, :]
            print(var_data.shape)
            rev_shape = (var_data.shape[::-1])
            string_shape = "{}x{}x{}".format(*rev_shape)
        except KeyError:
            print('variable names are: ', write_error(the_in))
            sys.exit(1)
        tmpname = 'temp.bin'
        fp = np.memmap(tmpname, dtype=np.float32, mode='w+',
                       shape=var_data.shape)
        fp[...] = var_data[...]
        del fp
        raw2vdf = files['raw2vdf']
        thecmd = f'{raw2vdf} -varname {varname}_{tracktype} -ts {t_step:d} {outname}_{cloud_id}.vdf {tmpname}'
        status2, output2 = subprocess.getstatusoutput(thecmd)
        print(status2, output2)
    return out_name, string_shape


def make_parser():
    '''
    command line arguments for calling program
    '''
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('-j', '--c_json', dest='cloud_json', help='json file or directory of json files with lists of parquet and zarr files', required=True)
    # parser.add_argument('-res', '--resolution', dest='resolution', help='resolution of the data in meters', required=True)
    parser.add_argument('-v', '--varname', dest='varname', help='name of 3d variable', required=True)
    parser.add_argument('-t', '--trackt', dest='tracktype', help='name of the type of cloud to visualize (e.g. core, condensed)', required=True)
    parser.add_argument('-o', '--outname', dest='outname', help='name of the outputted vdf file', required=True)
    return parser


def main(args=None):
    '''
    args are required
    '''
    parser = make_parser()
    args = parser.parse_args(args)
    if os.path.isdir(args.cloud_json):
        json_filelist = sorted(glob.glob(f'{args.cloud_json}/*.json'))
        for j in json_filelist:
            binfile, rev_shape = dump_bin(j, args.varname, args.tracktype, args.outname)
    else:
        binfile, rev_shape = dump_bin(args.cloud_json, args.varname, args.tracktype, args.outname)


if __name__ == "__main__":
    sys.exit(main())
