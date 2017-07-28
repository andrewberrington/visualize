import zarr
import numpy as np

zfile = zarr.open_group('/Users/berringtonaca/research/visualize/python/parquet_zarr/bomex_0000019560_zarr', mode='read')
data = (zfile['QN'][:][0][[3,8],2:4,5:8])

# var_data = the_in[varname][:][0][:, var_y, var_x]
print(np.shape(data))