from GrasslandModels import et_utils
import numpy as np
import pandas as pd
import xarray as xr
import glob
import os

import dask.array as da



def cmip_file_query(folder):
    file_list = glob.glob(folder+'/*.nc4')
    all_file_info = []
    for f in file_list:
        filename = os.path.basename(f)
        parts = filename.split('_')
        file_info = {'model'   : parts[4].lower(),
                     'scenario': parts[5],
                     'variable': parts[2],
                     'run'     : parts[6],
                     'decade'  : parts[7][0:4],
                     'full_path': f,
                     'filename':filename}
        
        all_file_info.append(file_info)

    return all_file_info

def compile_model_scenario(available_files,
                           model_name='ccsm4', scenario='rcp26', run='r1i1p1', 
                           chunks = {'latitude':100, 'longitude':100}):
    """
    Put together an xr dataset suitable for GrasslandModel predictions.
    model_name, scneario, and run should be strings refering to cmip5 stuff.
    
    available_files should be a dictionary output by cmip_file_query()
    
    chunks is passed to xarray. note a chunked array is returned so it needs values
    or compute() called to make the various calculations run.
    
    
    Return an xarray object with the attributes
    
    coords: time, latitude, longitude
    
    pr     (time, latitude, longitude)
    tasmin (time, latitude, longitude)
    tasmax (time, latitude, longitude)
    et     (time, latitude, longitude)
    Wp     (latitude, longitude)
    Wcap   (latitude, longitude)
    MAP    (latitude, longitude)
    """
    #TODO: need 15 day running mean on tmean.
    # maybe change the var names here to what phenograss expects
    files = pd.DataFrame(available_files)
    
    datasets = []
    for var in ['tasmin','tasmax','pr']:
        file_paths = files.query('variable==@var & model==@model_name & scenario==@scenario & run==@run')
        # If there is > 1 then it should be for multiple dates
        if len(file_paths) > 1:
            assert len(file_paths.decade.unique()) == len(file_paths)
        elif len(file_paths) == 0:
            raise RuntimeError('model scenario/run/variable not found')
        
        datasets.append(xr.combine_by_coords([xr.open_dataset(f, chunks=chunks) for f in file_paths.full_path]))
    
    
    datasets = xr.merge(datasets)

    rad  = create_radiation_data_array(ref = datasets).chunk(chunks)
    
    # compute is because it gets returned as a dask future
    #et = create_et_data_array(datasets.tasmin, datasets.tasmax, rad).compute().chunk(chunks)
    et = create_et_data_array(datasets.tasmin, datasets.tasmax, rad).chunk(chunks)
    
    # Includes Wp,Wcap & MAP
    other_vars = xr.open_dataset('data/other_variables.nc', chunks=chunks)

    return xr.merge([datasets, et, rad, other_vars])

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
    
def create_radiation_data_array(time, doy, latitude, longitude):
    """Return an xarray datarray containing
    
    coords: 
        time, latitude, longitude

    variables:
        radiation     (time, latitude, longitude)
        
    ref should be an xarray dataset with the same coordinates
    """
    lat_array = np.tile(latitude, (time.shape[0],1))
    
    doy_array = np.tile(doy, (latitude.shape[0],1)).T
    
    latitude_radians = et_utils.deg2rad(lat_array)
    solar_dec = et_utils.sol_dec(doy_array)
    
    sha = et_utils.sunset_hour_angle(latitude_radians, solar_dec)
    ird = et_utils.inv_rel_dist_earth_sun(doy_array)
    
    radiation = et_utils.et_rad(latitude_radians, solar_dec, sha, ird)
    # Now copy radiation to all longitudes, align, and join back in
    radiation = np.repeat(radiation[:,:,np.newaxis], repeats=longitude.shape[0], axis=2)

    return xr.DataArray(radiation, name='radiation',
                        dims =   ('time','latitude','longitude'),
                        coords = {'latitude':  latitude,
                                  'longitude': longitude,
                                  'time': time})

def create_radiation_array_dask(ref):
    doy = ref['time.dayofyear']
    return xr.apply_ufunc(create_radiation_data_array,
                          ref.time, doy, ref.latitude, ref.longitude,
                          dask='allowed')

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




