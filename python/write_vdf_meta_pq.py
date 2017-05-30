'''
    create and populate a json file containing the paths to the vdfcreate and
    raw2vdf command line tools from VAPOR as well as the names of the parquet files
    to be passed to wvdf_timestep_pq.py
    example:  python write_vdf_meta_pq.py -os linux -dir /media/loh -j BOMEX_indiv
'''

import json
import glob
import argparse


def main(args):
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

    pq_filelist = sorted(glob.glob(f'{args.filedir}/*.pq'))
    the_dict['pq_filenames'] = pq_filelist
    with open(f'{args.json_name}.json', 'w') as f:
        json.dump(the_dict, f, indent=4)
    
    # change filetype to zarr if using zarr files
    var_filelist = sorted(glob.glob(f'{args.filedir}/*.nc'))
    the_dict['var_filenames'] = var_filelist
    with open(f'{args.json_name}.json', 'w') as f:
        json.dump(the_dict, f, indent=4)

if __name__ == "__main__":
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('-os', '--op_sys', dest='os', help='operating system (eg. linux, mac)', required=True)
    parser.add_argument('-dir', '--filedir', dest='filedir', help='path to directory with parquet and netcdf/zarr files', required=True)
    parser.add_argument('-j', '--json_name', dest='json_name', help='name of outputted json file', required=True)
    args = parser.parse_args()
main(args)