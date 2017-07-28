import json
import glob

firsttime = False
if firsttime:
    empty_dict = {'vdfcreate': '/Applications/VAPOR/VAPOR.app/Contents/MacOS/vdfcreate',
                  'raw2vdf': '/Applications/VAPOR/VAPOR.app/Contents/MacOS/raw2vdf'}
    with open('meta.json', 'w') as f:
        json.dump(empty_dict, f, indent=4)
else:
    with open('meta.json', 'r') as f:
        the_dict = json.load(f)

filelist = sorted(glob.glob('/media/loh/cloudtracker*nc'))
the_dict['filenames'] = filelist
with open('meta.json', 'w') as f:
    json.dump(the_dict, f, indent=4)