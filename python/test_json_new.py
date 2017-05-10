import json
import glob
import argparse

def main():
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
    descrip = globals()['__doc__']
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('os', help='operating system (eg. linux, mac)')
    parser.add_argument('filedir', help='path to directory with netcdf files')
    parser.add_argument('json_name', help='name of outputted json file')
    args = parser.parse_args()
    main()
    print(args)