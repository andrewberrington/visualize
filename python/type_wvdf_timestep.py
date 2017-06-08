'''
    convert an LES zarr file to a raw binary file for vapor
    and write out a script that will turn that file into
    vapor vdf (uses parquet files to establish the domain)
    example:  python zarr_wvdf_timestep.py -json BOMEX_indiv.json -v QN -t condensed -o QN_ID
'''
import zarr
import pyarrow.parquet as pq
import numpy as np
import argparse
import sys
import json
import subprocess
import itertools


def write_error(the_in):
    namelist = []
    for name, var in the_in.variables.items():
        if len(var.shape) == 4:
            namelist.append(name)
    return namelist


def dump_bin(filename, varname, tracktype, outname):
    meters2km = 1.e-3
    print(filename)
    with open(filename, 'r') as f:
        files = json.load(f)
    num_ts = len(files['pq_filenames'])
    pq_filelist = sorted(files['pq_filenames'])

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
    xvals_full = []
    yvals_full = []
    xvals_sub = []
    yvals_sub = []
    zvals_sub = []
    z_maxes = []
    x_maxes = []
    x_mins = []
    y_maxes = []
    y_mins = []
    for f in pq_filelist:
        table = pq.read_table(f).to_pandas()
        xvals_full.append(table['x'].values)
        yvals_full.append(table['y'].values)
        z_maxes.append(np.amax(table['z'].values))
        x_maxes.append(np.amax(table['x'].values))
        x_mins.append(np.amin(table['x'].values))
        y_maxes.append(np.amax(table['y'].values))
        y_mins.append(np.amin(table['y'].values))
        cloud_id = table['cloud_id'].values[0]
        tablerows = table['type'] == keys[tracktype]
        df_thetype = table[tablerows]
        xvals_sub.append(df_thetype['x'].values)
        yvals_sub.append(df_thetype['y'].values)
        zvals_sub.append(df_thetype['z'].values)

    
    min_x, max_x = np.amin(x_mins), np.amax(x_maxes)
    min_y, max_y = np.amax(y_mins), np.amax(y_maxes)

    # print(min_x, max_x, min_y, max_y)

    # to handle the cases where the cloud crosses a boundary
    domain = 256

    x = np.array(list(itertools.chain.from_iterable(xvals_full)))
    y = np.array(list(itertools.chain.from_iterable(yvals_full)))
    # hardcoded for bomex currently
    off_x = 0
    off_y = 0
    if (max_x - min_x) > (domain / 2):
        off_x = domain - np.min(x[(x > domain / 2)])
    if (max_y - min_y) > (domain / 2):
        off_y = domain - np.min(y[(y > domain / 2)])

    # define the full domain of the vdf
    x_mean = []
    y_mean = []
    width_y_full = []
    width_x_full = []
    for x, y in zip(xvals_full, yvals_full):
        width_x_full.append(np.amax(x) - np.amin(x))
        width_y_full.append(np.amax(y) - np.amin(y))
        x_mean.append(np.mean(x))
        y_mean.append(np.mean(y))

    x_dim_full = np.amax(width_x_full)
    y_dim_full = np.amax(width_y_full)

    # get the start and end indices for the x, y dimensions of the box
    # containing the cloud at a given timestep
    x_indices = np.array([0, 256]).astype(int)
    y_indices = np.array([0, 256]).astype(int)

    # in order to define the vdf using vdfcreate
    # eventually will add the ability to change resolution for other datasets
    # for now is hardcoded to establish the vdf for BOMEX only
    xvals = np.arange(0, x_indices[1]) * 25. * meters2km
    yvals = np.arange(0, y_indices[1]) * 25. * meters2km
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
             -gridtype stretched -dimension {string_shape} -vars3d {varname}_{tracktype} -numts {num_ts} {outname}_{cloud_id}.vdf'
    status1, output1 = subprocess.getstatusoutput(thecmd)
    out_name = '{}.bin'.format(outname)
    print('writing an array of {}(t,x,y,z) shape {}x{}x{}x{}'.format(varname, *the_shape))

    for t_step, the_file in enumerate(files['var_filenames']):
        the_in = zarr.open_group(the_file, mode='r')
        try:
            startx, stopx = x_indices[0], x_indices[1]
            starty, stopy = y_indices[0], y_indices[1]
            # extra slice 0 is there to remove the time dimension from the zarr data
            var_data = the_in[varname][:][0]
            x_r = xvals_sub[t_step]
            y_r = yvals_sub[t_step]
            z = zvals_sub[t_step]
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
            b_map[tuple(indices)] = True
            var_data[~b_map] = 0
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

if __name__ == "__main__":
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('-json', '--cloud_json', dest='cloud_json', help='json file with list of parquet and zarr files', required=True)
    # parser.add_argument('-res', '--resolution', dest='resolution', help='resolution of the data in meters', required=True)
    parser.add_argument('-v', '--varname', dest='varname', help='name of netcdf 3d variable', required=True)
    # new argument to establish what type of cloud we want to analyze
    parser.add_argument('-t', '--tracktype', dest='tracktype', help='name of the type of cloud to visualize (e.g. core, condensed)', required=True)
    parser.add_argument('-o', '--outname', dest='outname', help='name of the outputted vdf file', required=True)
    args = parser.parse_args()
binfile, rev_shape = dump_bin(args.cloud_json, args.varname, args.tracktype, args.outname)