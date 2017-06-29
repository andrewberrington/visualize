'''
   module for calculating different derived variables from our LES data
'''

import numpy as np

def mix_r(z):
	'''
	   get vapor mixing ratio in kg/kg from zarr QV

	   Parameters
	   ----------

	   z: zarr file
	      file containing the QV variable

       Returns
       -------

       rv: array-like
           array of vapor mixing ratios in kg/kg
	'''
	rv = (z['QV'][:][0]/1.e3) / (1 - (z['QV'][:][0]/1.e3))
	return rv


def calc_td(z):
	'''
	   get dewpoint temperature in K from zarr p and QV

	   Parameters
	   ----------

	   z: zarr file
	      file containing the variables

	   Returns
	   -------

	   Td: array-like
	       array of dewpoint temperatures in K
	'''
	rv = mix_r(z)
	press = z['p'][:] * 1.e2
	e = rv * press[:, None, None] / (0.622 + rv)
	denom = (17.67 / np.log(e / 611.2)) - 1.
	Td = 243.5 / denom
	Td = Td + 273.15
	return Td
