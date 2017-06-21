'''
    example:  python write_vdf_meta_pq.py -os linux -pdir /mnt/datatmp/visualize/andrew/cloud_11872/11872 -zdir /mnt/datatmp/visualize/andrew/bomex_zarr -j 11872
'''

import json
import glob
import argparse
# import sys
import numpy as np


def main(args):
    '''
       create and populate a json file containing the paths to the vdfcreate and
       raw2vdf command line tools from VAPOR as well as the names of the parquet
       or zarr files to be passed to vdf writer

       Parameters
       ----------

       args: arguments from parser
          required to establish the paths to the vapor tools, parquet and zarr files

       Returns
       -------

       json_name.json: json file
          json file containing the metadata (output to current working directory)
    '''

    try:
        with open(f'{args.json_name}.json', 'r') as f:
            the_dict = json.load(f)
    except:
        if args.os == 'mac':
            the_dict = {'vdfcreate': '/Applications/VAPOR/VAPOR.app/Contents/MacOS/vdfcreate',
                        'raw2vdf': '/Applications/VAPOR/VAPOR.app/Contents/MacOS/raw2vdf'}
        else:
            the_dict = {'vdfcreate': '/usr/local/vaporapp/vapor-2.6.0/bin/vdfcreate',
                        'raw2vdf': '/usr/local/vaporapp/vapor-2.6.0/bin/raw2vdf'}
        with open(f'{args.json_name}.json', 'w') as f:
            json.dump(the_dict, f, indent=4)

    pq_filelist = sorted(glob.glob(f'{args.parqdir}/*.pq'))
    the_dict['pq_filenames'] = pq_filelist
    with open(f'{args.json_name}.json', 'w') as f:
        json.dump(the_dict, f, indent=4)

    # find the start of the pq filelist and convert to the zarr timestamp
    startpq = pq_filelist[0]
    start_int_pq = int(startpq[-6:-3])
    timestep = 60  # s
    start_pq_to_zarr = timestep * start_int_pq

    # in order to get the timestamp from the first zarr file in the directory
    zarr_filelist = sorted(glob.glob(f'{args.zarrdir}/*zarr'))
    startzarr = zarr_filelist[0]
    start_int_zarr = int(startzarr[-10:-5]) + start_pq_to_zarr

    zarr_range = np.arange(start_int_zarr, start_int_zarr + ((len(pq_filelist) * timestep)), timestep)

    var_filelist = []
    for zr in zarr_range:
        var_filelist.append(glob.glob(f'{args.zarrdir}/*{zr}_zarr'))
    var_filelist = np.squeeze(var_filelist).tolist()

    the_dict['var_filenames'] = var_filelist
    with open(f'{args.json_name}.json', 'w') as f:
        json.dump(the_dict, f, indent=4)

if __name__ == "__main__":
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('-os', '--op_sys', dest='os', help='operating system (eg. linux, mac)', required=True)
    parser.add_argument('-pdir', '--parqdir', dest='parqdir', help='path to directory containing the parquet files', required=True)
    parser.add_argument('-zdir', '--zarrdir', dest='zarrdir', help='path to directory containing the zarr files', required=True)
    parser.add_argument('-j', '--json_name', dest='json_name', help='name of outputted json file', required=True)
    args = parser.parse_args()
main(args)