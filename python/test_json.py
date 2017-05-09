import json
import glob

firsttime=False
if firsttime:
    empty_dict={'vdfcreate':'vdfcreate_here','raw2vdf':'raw2vdf here'}
    with open('meta.json', 'w') as f:
        json.dump(empty_dict, f, indent=4)
else:
    with open('meta.json', 'r') as f:
        the_dict=json.load(f)

filelist=sorted(glob.glob('/media/loh/cloudtracker*nc'))
the_dict['filenames']=filelist        
with open('meta.json', 'w') as f:
    json.dump(the_dict,f, indent=4)

