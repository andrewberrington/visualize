'''
    create and populate a json file containing the paths to the vdfcreate and
    raw2vdf command line tools from VAPOR as well as the names of the netcdf files
    to be passed to wvdf_timestep.py

    example:  python test_json_new.py mac path_to_nc bomex
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

    filelist = sorted(glob.glob(f'{args.filedir}/*.nc'))
    the_dict['filenames'] = filelist
    with open(f'{args.json_name}.json', 'w') as f:
        json.dump(the_dict, f, indent=4)

if __name__ == "__main__":
    linebreaks = argparse.RawTextHelpFormatter
    descrip = descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('os', help='operating system (eg. linux, mac)', required=True)
    parser.add_argument('filedir', help='path to directory with netcdf files', required=True)
    parser.add_argument('json_name', help='name of outputted json file', required=True)
    args = parser.parse_args()
    main(args)
    print(args)