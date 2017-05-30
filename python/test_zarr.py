import zarr
import numpy as np

zfile = zarr.open_group('/mnt/datatmp/visualize/andrew/parquet_tracking/bomex_0000019560_zarr', mode='read')
data = (zfile['QN'][:][0])
print(np.shape(data))