import json
import glob
filelist = glob.glob('cloudtracker/cloudtracker*.nc')
with open('meta.json', 'w') as f:
    json.dump(filelist, f, indent=4)
with open('meta.json', 'r') as f:
    cloudnames = json.load(f)

print(cloudnames)