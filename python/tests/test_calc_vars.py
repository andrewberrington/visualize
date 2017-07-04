'''
   contains tests for the functions defined in calc_vars.py
'''

import pytest
import numpy as np


def test_rv():
	'''
	   test vapor mixing ratio function from calc_vars
	   qv must be in g/kg initially
	'''

	the_qvs = np.array([15., 16., 17.])
	out = (the_qvs / 1.e3) / (1 - (the_qvs / 1.e3))
	answer = np.array([0.01523, 0.01626, 0.01729])
	np.testing.assert_array_almost_equal(out, answer, decimal = 5)
	return None


def test_rl():
	'''
	   test liquid water mixing ratio function from calc_vars
	   qn must be in g/kg initially
	'''

	the_qns = np.array([0.5, 1., 1.5])
	out = (the_qns / 1.e3) / (1 - (the_qns / 1.e3))
	answer = np.array([0.00050, 0.00100, 0.00150])
	np.testing.assert_array_almost_equal(out, answer, decimal = 5)
	return None


def test_rt():
	'''
	   test total water mixing ratio function from calc_vars
	   rl and rv must be in kg/kg
	'''

	the_rls = np.array([0.0005, 0.0010, 0.0015])
	the_rvs = np.array([0.0152, 0.0163, 0.0173])
	out = the_rls + the_rvs
	answer = np.array([0.0157, 0.0173, 0.0188])
	np.testing.assert_array_almost_equal(out, answer, decimal = 4)
	return None


def test_Td():
	'''
	   test dewpoint function from calc_vars
	   rv in kg/kg and p in hPa
	'''

	the_rvs = np.array([0.01523, 0.01626, 0.01729])
	press = np.array([1.e3, 9.5e2, 9.e2])
	press = press * 1.e2
	e = the_rvs * press / (0.622 + the_rvs)
	denom = (17.67 / np.log(e / 611.2)) - 1.
	Td = 243.5 / denom
	out = Td + 273.15
	answer = np.array([293.51296, 293.71591, 293.80898])
	np.testing.assert_array_almost_equal(out, answer, decimal = 5)
	return None


def test_theta():
	'''
	   test theta function from calc_vars
	   p in hPa and temp in K
	'''

	cp = 1004
	Rv = 461.
	Rd = 287.
	Lv = 2.5104e6
	lam = 0.61

	press = np.array([1.e3, 9.5e2, 9.e2])
	press = press * 1.e2
	temp = np.array([287., 288., 289.])
	out = temp * (1.e5 / press)**(Rd / cp)
	answer = np.array([287.00000, 292.25392, 297.83650])
	np.testing.assert_array_almost_equal(out, answer, decimal = 5)
	return None


def test_Tv():
	'''
	   test virtual temp function from calc_vars
	   rl, rv in kg/kg, temp in K
	'''

	lam = 0.61

	the_rls = np.array([0.0005, 0.0010, 0.0015])
	the_rvs = np.array([0.0152, 0.0163, 0.0173])
	temp = np.array([287., 288., 289.])
	out = temp * (1. + lam * (the_rvs - the_rls))
	answer = np.array([289.57353, 290.68790, 291.78538])
	np.testing.assert_array_almost_equal(out, answer, decimal = 5)
	return None


def test_thetav():
	'''
	   test virtual potential temp function from calc_vars
	   rl, rv in kg/kg, temp in K, p in hPa
	'''

	cp = 1004
	Rv = 461.
	Rd = 287.
	Lv = 2.5104e6
	lam = 0.61

	press = np.array([1.e3, 9.5e2, 9.e2])
	press = press * 1.e2
	temp = np.array([287., 288., 289.])
	the_rls = np.array([0.0005, 0.0010, 0.0015])
	the_rvs = np.array([0.0152, 0.0163, 0.0173])
	theta = temp * (1.e5 / press)**(Rd / cp)
	out = theta * (1. + lam * (the_rvs - the_rls))
	answer = np.array([289.57353, 294.98152, 300.70705])
	np.testing.assert_array_almost_equal(out, answer, decimal = 5)
	return None


def test_buoy():
	'''
	   test buoyancy function from calc_vars
	   virtual temperature in K
	'''

	g_0 = 9.81 # m/s^2

	the_tvs = np.array([289.57353, 290.68790, 291.78538])
	tv_mean = np.nanmean(the_tvs)
	out = g_0 * ((the_tvs - tv_mean) / tv_mean)
	answer = np.array([-0.03742, 0.00019, 0.03723])
	np.testing.assert_array_almost_equal(out, answer, decimal = 5)
	return None


if __name__ == "__main__":
	print('testing {}'.format(__file__))
	pytest.main([__file__, '-q'])