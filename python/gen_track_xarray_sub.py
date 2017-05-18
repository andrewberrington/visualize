import os, sys, glob
import numpy as np
import numba as nb
import h5py as h5
import json

import xarray as xr
from netCDF4 import Dataset as nc

cp = 1004.    # Heat capacity at constant pressure for dry air [J kg^-1 K^-1]
Rv = 461.      # Gas constant of water vapor [J kg^-1 K^-1]
Rd = 287.      # Gas constant of dry air [J kg^-1 K^-1]
p_0 = 100000.
lam = Rv/Rd - 1.

def T_v(T, qv, qn, qp):
    return T*(1. + lam*qv - qn - qp)

def theta(p, T): return T*(p_0/p)**(Rd/cp)

def theta_v(p, T, qv, qn, qp):
    return theta(p, T_v(T, qv, qn, qp))

def generate_tracking(time, file):
    with xr.open_dataset(file) as f:
        try:
            f = f.squeeze('time')
        except:
            pass
        print(f)

        # for subsetting
        # f = f.isel(x=np.arange(400,800),y=np.arange(400,800))

        print("\t Calculating velocity fields...")
        w_field = f['W'][:]
        w_field = (w_field + np.roll(w_field, 1, axis=0)) / 2

        u_field = f['U'][:]
        u_field = (u_field + np.roll(u_field, 1, axis=1)) / 2

        v_field = f['V'][:]
        v_field = (v_field + np.roll(v_field, 1, axis=2)) / 2

        print("\t Calculating buoynacy fields...")
        qn_field = f['QN'][:] / 1e3
        thetav_field = theta_v(f['p'][:]*100, f['TABS'][:], 
                               f['QV'][:] / 1e3, qn_field, 0)
        buoy_field = (thetav_field > 
                      np.mean(thetav_field, axis=(1,2)))

        print("\t Calculating tracer fields...")
        # Tracer fields
        tr_field = np.array(f['TR01'][:])
        tr_mean = np.mean(tr_field.reshape((len(f.z), len(f.y)*len(f.x))), axis=1)
        tr_stdev = np.sqrt(tr_field.reshape((len(f.z), len(f.y)*len(f.x))).var(1))
        tr_min = .05 * np.cumsum(tr_stdev)/(np.arange(len(tr_stdev))+1)

        #---- Dataset for storage 
        print("\t Saving DataArrays...")
        ds = xr.Dataset(coords= {'z': f.z, 'y':f.y, 'x':f.x})

        # ds['u'] = u_field
        # ds['v'] = v_field
        # ds['w'] = w_field
    
        ds['core'] = (w_field > 0.) * (buoy_field > 0.) & (qn_field > 1e-4)
        ds['condensed'] = (qn_field > 1e-4)
        ds['plume'] = xr.DataArray(tr_field > 
                       np.max(np.array([tr_mean + tr_stdev, tr_min]), 0)[:, None, None],
                       dims=['z', 'y', 'x'])

        meanU = np.mean(u_field, axis=(1,2))
        meanV = np.mean(v_field, axis=(1,2))

        u_field = u_field - meanU
        v_field = v_field - meanV
        
        # put only the velocities into dataset where there is condensation
        # otherwise, set velocities to 0, also subtract mean U and V values
        ds['u'] = np.where(qn_field > 1e-4, 1, 0) * u_field
        ds['v'] = np.where(qn_field > 1e-4, 1, 0) * v_field
        ds['w'] = np.where(qn_field > 1e-4, 1, 0) * w_field

        ds.to_netcdf(f'data/cloudtracker_input_{time:08g}.nc')
        print(ds)

def main():
    global model_config
    with open('model_config.json', 'r') as json_file:
        model_config = json.load(json_file)['BOMEX']

    filelist = sorted(glob.glob('%s/*.nc' % model_config['variables']))
    for time in range(len(filelist)):
        print('\t Working...%s/%s                        ' % (time, 180), end='\r')
        generate_tracking(time, filelist[time])

if __name__ == "__main__":
    main()