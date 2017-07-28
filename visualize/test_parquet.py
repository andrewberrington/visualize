import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import glob
import json

with open('parquet.json', 'r') as f:
	pqfiles = json.load(f)
files = pqfiles['filenames']
x_mins = []
y_mins = []
x_maxes = []
y_maxes = []
x_mean = []
y_mean = []
for f in files:
	table = pq.read_table(f).to_pandas()
	x_mins.append(np.amin(table['x'].values))
	y_mins.append(np.amin(table['y'].values))
	x_maxes.append(np.amax(table['x'].values))
	y_maxes.append(np.amax(table['y'].values))
	# xvals = np.concatenate((xvals,table['x'].values),axis=0)
	# yvals = np.concatenate((yvals,table['y'].values),axis=0)
	x_mean.append(np.mean(table['x'].values))
	y_mean.append(np.mean(table['y'].values))

x_mins = np.array(x_mins)
x_maxes = np.array(x_maxes)
y_maxes = np.array(y_maxes)
y_mins= np.array(y_mins)

x_dim = np.amax((x_maxes - x_mins))
y_dim = np.amax((y_maxes - y_mins))

x_indices = (np.array([(x_mean - (0.5 * x_dim)), (x_mean + (0.5 * x_dim))])).astype(int)
y_indices = (np.array([(y_mean - (0.5 * y_dim)), (y_mean + (0.5 * y_dim))])).astype(int)

print(x_indices)
print(y_indices)

