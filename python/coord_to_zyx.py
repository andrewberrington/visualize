def index_to_zyx(index, nz, ny, nx):
	import numpy as np
	z = np.floor_divide(index, (ny * nx))
	xy = np.mod(index, (ny * nx))
	y = np.floor_divide(xy, nx)
	x = np.mod(xy, nx)
	return (z, y, x)