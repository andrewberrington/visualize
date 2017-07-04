'''
   module for calculating different derived variables from our LES zarr files
'''

import numpy as np


def calc_rv(qv):
	'''
	   get vapor mixing ratio in kg/kg from zarr QV
	   (adopted from Z. Ming ATSC 405 libs)

	   Parameters
	   ----------

	   qv: array-like
	       array of vapor specific humidities in g/kg

	   Returns
	   -------

	   rv: array-like
	       array of vapor mixing ratios in kg/kg
	'''
	rv = (qv[:][0]/1.e3) / (1 - (qv[:][0]/1.e3))
	return rv


def calc_rl(qn):
	'''
	   get liquid water mixing ratio in kg/kg from zarr QN
	   (adopted from Z. Ming ATSC 405 libs)

	   Parameters
	   ----------

	   qn: array-like
	       array of liquid water specific humidities in g/kg

	   Returns
	   -------

	   rl: array-like
	       array of liquid water mixing ratios in kg/kg
	'''
	rl = (qn[:][0]/1.e3) / (1 - (qn[:][0]/1.e3))
	return rl


def calc_rt(qv, qn):
	'''
	   get total water mixing ratio in kg/kg from zarr

	   Parameters
	   ----------

	   qv: array-like
	       array of vapor specific humidities in g/kg

	   qn: array-like
	       array of liquid water specific humidities in g/kg

	   Returns
	   -------

	   rt: array-like
	       array of total water mixing ratios in kg/kg
	'''
	rt = calc_rv(qv) + calc_rl(qn)
	return rt


def calc_Td(qv, p):
	'''
	   get dewpoint temperature in K from zarr p and QV
	   (adopted from find_Td in ATSC 405 libs)

	   Parameters
	   ----------

	   qv: array-like
	       array of vapor specific humidities in g/kg

	   p: array-like
	      array of pressures in hPa

	   Returns
	   -------

	   Td: array-like
	       array of dewpoint temperatures in K
	'''
	rv = calc_rv(qv)
	press = p[:] * 1.e2
	e = rv * press[:, None, None] / (0.622 + rv)
	denom = (17.67 / np.log(e / 611.2)) - 1.
	Td = 243.5 / denom
	Td = Td + 273.15
	return Td


def calc_theta(p, t):
	'''
	   get theta in K from zarr TABS and p
	   (adopted from Loren's script)

	   Parameters
	   ----------

	   p: array-like
	      array of pressures in hPa

	   t: array-like
	      array of temperatures in K

	   Returns
	   -------

	   theta: array-like
	          array of theta in K
	'''
	cp = 1004 # J/kg/K
	Rv = 461. # J/kg/K
	Rd = 287. # J/kg/K
	Lv = 2.5104e6 # J/kg
	lam = 0.61 # kg/kg

	press = p[:] * 1.e2
	theta = t[:][0] * (1.e5 / press[:, None, None])**(Rd / cp)
	return theta


def calc_Tv(qv, qn, t):
	'''
	   get virtual temperature in K from zarr variables
	   (adopted from Loren's script)

	   Parameters
	   ----------

	   qv: array-like
	       array of vapor specific humidities in g/kg

	   qn: array-like
	       array of liquid water specific humidities in g/kg

	   t: array-like
	      array of temperatures in K

	   Returns
	   -------

	   Tv: array-like
	       array of virtual temperatures in K
	'''
	lam = 0.61 # kg/kg
	rv = calc_rv(qv)
	rl = calc_rl(qn)
	Tv = t[:][0] * (1. + lam * (rv - rl))
	return Tv


def calc_thetav(qv, qn, p, t):
	'''
	   get the virtual potential temperature in K from zarr variables

	   Parameters
	   ----------

	   qv: array-like
	       array of vapor specific humidities in g/kg

	   qn: array-like
	       array of liquid water specific humidities in g/kg

	   t: array-like
	      array of temperatures in K

	   p: array-like
	      array of pressures in hPa

	   Returns
	   -------

	   theta_v: array-like
	            array of virtual potential temperatures in K
	'''
	lam = 0.61 # kg/kg

	rv = calc_rv(qv)
	rl = calc_rl(qn)
	theta = calc_theta(p, t)
	theta_v = theta * (1. + lam * (rv - rl))
	return theta_v


def calc_buoy(qv, qn, t):
	'''
	   get buoyancy factor in m/s^2 from zarr variables

	   Parameters
	   ----------

	   qv: array-like
	       array of vapor specific humidities in g/kg

	   qn: array-like
	       array of liquid water specific humidities in g/kg

	   t: array-like
	      array of temperatures in K
	   
	   Returns
	   -------

	   buoyancy: array-like
	             array of buoyancy factors in m/s^2 
	'''
	g_0 = 9.81 # m/s^2

	Tv = calc_Tv(qv, qn, t)
	Tv_mean = np.nanmean(Tv, axis=(1, 2))
	buoyancy = g_0 * ((Tv - Tv_mean[:, None, None]) / Tv_mean[:, None, None])
	return buoyancy