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

def create_et_dataset(temp):
    """Return an xarray dataset containing
    
    coords: 
        time, latitude, longitude

    variables:
        et     (time, latitude, longitude)
        
    temp should be an xarray dataset with the same coordinates and the
    data variables tasmax, and tasmin.
    """
    
    # et_array = np.empty((doy_array.shape[0],ref.latitude.shape[0],ref.longitude.shape[0]))
    
    # et = xr.Dataset(data_vars = {'et':(('doy','latitude','longitude'),et_array)},
    #                 coords    = {'doy':doy_array,
    #                              'latitude':ref.latitude.values,
    #                              'longitude':ref.longitude.values})
    #lat_array = np.tile(temp.latitude.values, (temp.dims['time'], temp.dims['longitude'],1)).T
    
    # Create a radiation array of dimensions (latitude,time) fro 
    # the daily radiation values. This does not need longtitude since the
    # radiation equation does not vary with that, and excluding longitude
    # here saves a lot of memory and processing time.
    lat_array = np.tile(temp.latitude.values, (temp.dims['time'],1))
    
    doy_array = pd.to_datetime(temp.time.values).dayofyear.values
    doy_array = np.tile(doy_array, (temp.dims['latitude'],1)).T
    
    latitude_radians = et_utils.deg2rad(lat_array)
    solar_dec = et_utils.sol_dec(doy_array)
    
    sha = et_utils.sunset_hour_angle(latitude_radians, solar_dec)
    ird = et_utils.inv_rel_dist_earth_sun(doy_array)
    
    radiation = et_utils.et_rad(latitude_radians, solar_dec, sha, ird)
    # Now copy radiation to all longitudes, align, and join back in
    radiation = np.repeat(radiation[:,:,np.newaxis], repeats=temp.dims['longitude'], axis=2)

    temp = temp.assign(radiation = (('time','latitude','longitude'),radiation))
   
    et = et_utils.hargreaves(tmin =   temp.tasmin.values, 
                             tmax =   temp.tasmax.values,
                             et_rad = radiation)




