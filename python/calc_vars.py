'''
   module for calculating different derived variables from our LES zarr files
'''

import numpy as np


def calc_rv(z):
	'''
	   get vapor mixing ratio in kg/kg from zarr QV
	   (adopted from Z. Ming ATSC 405 libs)

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


def calc_rl(z):
	'''
	   get liquid water mixing ratio in kg/kg from zarr QN
	   (adopted from Z. Ming ATSC 405 libs)

	   Parameters
	   ----------

	   z: zarr file
	      file containing the QN variable

	   Returns
	   -------

	   rl: array-like
	       array of liquid water mixing ratios in kg/kg
	'''
	rl = (z['QN'][:][0]/1.e3) / (1 - (z['QN'][:][0]/1.e3))
	return rl


def calc_rt(z):
	'''
	   get total water mixing ratio in kg/kg from zarr

	   Parameters
	   ----------

	   z: zarr file
	      file containing the variables

	   Returns
	   -------

	   rt: array-like
	       array of total water mixing ratios in kg/kg
	'''
	rt = calc_rv(z) + calc_rl(z)
	return rt


def calc_Td(z):
	'''
	   get dewpoint temperature in K from zarr p and QV
	   (adopted from find_Td in ATSC 405 libs)

	   Parameters
	   ----------

	   z: zarr file
	      file containing the variables

	   Returns
	   -------

	   Td: array-like
	       array of dewpoint temperatures in K
	'''
	rv = calc_rv(z)
	press = z['p'][:] * 1.e2
	e = rv * press[:, None, None] / (0.622 + rv)
	denom = (17.67 / np.log(e / 611.2)) - 1.
	Td = 243.5 / denom
	Td = Td + 273.15
	return Td


def calc_theta(z):
	'''
	   get theta in K from zarr TABS and p
	   (adopted from Loren's script)

	   Parameters
	   ----------

	   z: zarr file
	      file containing the variables

	   Returns
	   -------

	   theta: array-like
	          array of theta in K
	'''
	cp = 1004
	Rv = 461.
	Rd = 287.
	Lv = 2.5104e6
	lam = 0.61

	press = z['p'][:] * 1.e2
	theta = z['TABS'][:][0] * (1.e5 / press[:, None, None])**(Rd / cp)
	return theta


def calc_Tv(z):
	'''
	   get virtual temperature in K from zarr variables
	   (adopted from Loren's script)

	   Parameters
	   ----------

	   z: zarr file
	      file containing the variables

	   Returns
	   -------

	   Tv: array-like
	       array of virtual temperatures in K
	'''
	lam = 0.61
	rv = calc_rv(z)
	rl = calc_rl(z)
	Tv = z['TABS'][:][0] * (1. + lam * (rv - rl))
	return Tv


def calc_thetav(z):
	'''
	   get the virtual potential temperature in K from zarr variables

	   Parameters
	   ----------

	   z: zarr file
	      file containing the variables

	   Returns
	   -------

	   theta_v: array-like
	            array of virtual potential temperatures in K
	'''
	lam = 0.61

	rv = calc_rv(z)
	rl = calc_rl(z)
	theta = calc_theta(z)
	theta_v = theta * (1. + lam * (rv - rl))
	return theta_v


def calc_buoy(z):
	'''
	   get buoyancy in m/s^2 from zarr variables

	   Parameters
	   ----------

	   z: zarr file
	      file containing the variables
	   
	   Returns
	   -------

	   buoyancy: array-like
	             array of buoyancies in m/s^2 
	'''
	g_0 = 9.81 # m/s^2

	theta = calc_theta(z)
	theta_v = calc_thetav(z)
	theta_v_mean = np.nanmean(theta_v, axis=(1, 2))
	buoyancy = g_0 * ((theta / theta_v_mean) - 1)
	return buoyancy