from GrasslandModels import et_utils
import numpy as np
import pandas as pd
import xarray as xr

import dask.array as da


def compile_model_scenario(model_name, scenario, run):
    """Return an xarray object with the attributes
    
    coords: time, latitude, longitude
    
    pr     (time, latitude, longitude)
    tasmin (time, latitude, longitude)
    tasmax (time, latitude, longitude)
    et     (time, latitude, longitude)
    Wp     (latitude, longitude)
    Wcap   (latitude, longitude)
    MAP    (latitude, longitude)
    """
    pass


def tile_array_to_shape():
    pass

def create_et_data_array(tmin, tmax, radiation):
    """
    Calculate evapotranspiration and return a dataarray of the same shape
    as the inputs.
    
    Hargreaves function has the form:
    et_utils.hargreaves(tmin, tmax, et_rad)
    """
    return xr.apply_ufunc(et_utils.hargreaves,
                          tmin, tmax, radiation,
                          dask='allowed').rename('et')
    
def create_radiation_data_array(ref):
    """Return an xarray datarray containing
    
    coords: 
        time, latitude, longitude

    variables:
        radiation     (time, latitude, longitude)
        
    ref should be an xarray dataset with the same coordinates
    """
    lat_array = np.tile(ref.latitude.values, (ref.dims['time'],1))
    
    doy_array = pd.to_datetime(ref.time.values).dayofyear.values
    doy_array = np.tile(doy_array, (ref.dims['latitude'],1)).T
    
    latitude_radians = et_utils.deg2rad(lat_array)
    solar_dec = et_utils.sol_dec(doy_array)
    
    sha = et_utils.sunset_hour_angle(latitude_radians, solar_dec)
    ird = et_utils.inv_rel_dist_earth_sun(doy_array)
    
    radiation = et_utils.et_rad(latitude_radians, solar_dec, sha, ird)
    # Now copy radiation to all longitudes, align, and join back in
    radiation = np.repeat(radiation[:,:,np.newaxis], repeats=ref.dims['longitude'], axis=2)

    return xr.DataArray(radiation, name='radiation',
                        dims =   ('time','latitude','longitude'),
                        coords = {'latitude':ref.latitude,
                                  'longitude': ref.longitude,
                                  'time': ref.time})


if __name__ == "__main__":
    # Some testing stuff
     
    temp = xr.open_dataset('data/test_dataset.nc4', chunks={'latitude':2,'longitude':2,'time':30})
    
    # One of the big datasets, if available
    # temp = xr.open_mfdataset(['data/cmip5_nc_files/BCCA_0.125deg_tasmin_day_CCSM4_rcp26_r1i1p1_20060101-20151231.nc4',
    #                           'data/cmip5_nc_files/BCCA_0.125deg_tasmax_day_CCSM4_rcp26_r1i1p1_20060101-20151231.nc4',
    #                           'data/cmip5_nc_files/BCCAv2_0.125deg_pr_day_CCSM4_rcp26_r1i1p1_20060101-20151231.nc4'], 
    #                          chunks={'latitude':100,'longitude':100,'time':500})
    
    rad  = create_radiation_data_array(ref = temp)
    
    # compute is because it gets returned as a dask future
    et = create_et_data_array(temp.tasmin, temp.tasmax, rad).compute()




