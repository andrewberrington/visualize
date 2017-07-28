import pickle
import numpy as np
from netCDF4 import Dataset
import matplotlib.pyplot as plt
import glob
import sys
import coord_to_zyx as ctz
global new_x
global new_y
global ix, iy

new_x = []
new_y = []

plt.ion()

pickle_list = sorted(glob.glob('/nodessd/phil/clusters/andrew/pickle_large/*clusters'))
var_list = sorted(glob.glob(f'/newtera/loh/data/GATE/variables/*.nc'))
entrain_list = sorted(glob.glob('/newtera/loh/data/GATE/core_entrain/*.nc'))

ids = []
for f in var_list:
    name = f.split('_')
    number = name[-1].split('.')
    ids.append(int(number[0][-5:]))

numbers = []
for a_file in pickle_list:
    parts = a_file.split('_')
    numbers.append(int(parts[1].split('/')[1]))

selection = []
ent_selection = []
for the_index, n in enumerate(ids):
    for the_num in numbers:
        if n == the_num:
            selection.append(var_list[the_index])
            ent_selection.append(entrain_list[the_index])

selection = selection[0]

coords = []
for the_file in pickle_list:
    with open(the_file, 'rb') as f:
        coords.append(pickle.load(f))

z, y, x = ctz.index_to_zyx(coords, 320, 1728, 1728)
num_ts = 1
y_min = []
x_min = []
y_max = []
x_max = []
x_mean = []
y_mean = []

num_clus = 15

for i in range(0,num_ts):
    for j in range(0, num_clus):
        y_min.append(np.amin(y[i][j]))
        x_min.append(np.amin(x[i][j]))
        y_max.append(np.amax(y[i][j]))
        x_max.append(np.amax(x[i][j]))
        y_mean.append(np.mean(y[i][j]))
        x_mean.append(np.mean(x[i][j]))
        
y_min = np.reshape(y_min, (num_ts,num_clus))
x_min = np.reshape(x_min, (num_ts,num_clus))
y_max = np.reshape(y_max, (num_ts,num_clus))
x_max = np.reshape(x_max, (num_ts,num_clus))
y_mean = np.reshape(y_mean, (num_ts,num_clus))
x_mean = np.reshape(x_mean, (num_ts,num_clus))

time_step = 0
index = 1
h = 120
conv = 50*1.e-3
y_section = np.arange(np.amin(y_min[time_step][index])-50, np.amax(y_max[time_step][index])+51)
x_section = np.arange(np.amin(x_min[time_step][index])-50, np.amax(x_max[time_step][index])+51)
varname = 'W'

the_in = Dataset(selection, 'r')
var_data = the_in[varname][h, y_section, x_section]

def onclick(event):
	global ix, iy
	ix, iy = event.xdata, event.ydata

	global new_x
	new_x.append(int(ix))
	
	global new_y
	new_y.append(int(iy))

	if len(new_y) == 4:
		print(f'finished, x coordinates of new subsetted region are: {new_x}')
		print(f'finished, y coordinates of new subsetted region are: {new_y}')
		print(f'variable names are new_x and new_y')
		fig.canvas.mpl_disconnect(cid)
		plt.close(1)
	return

fig, ax = plt.subplots(1,1,figsize=[14,14])
levels = np.arange(-10, 10.5, 0.5)
cmap = plt.cm.get_cmap('seismic')
cmap.set_over('magenta')
cmap.set_under('cyan')
ax.contourf(x_section, y_section, var_data, levels=levels, cmap=cmap, extend='both')
ax.set(title='Timestep = {}, Variable = {}, Z = {:.2f}'.format(time_step+10, varname, h*conv))

plt.show(block=True)

cid = fig.canvas.mpl_connect('button_press_event', onclick)