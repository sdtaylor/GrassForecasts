from GrasslandModels import et_utils
import numpy as np
import pandas as pd
import xarray as xr
import bottleneck as bn
import glob
import os

import dask.array as da


# testing variables
# climate_model_files = glob.glob('data/cmip5_nc_files/*nc4')
# climate_model_files = 'data/NE_CO_ccsm4.nc4'
# climate_model_name = 'ccsm4'
# scenario = 'rcp26'
# chunk_sizes = {'latitude':4,'longitude':4,'time':-1}
# other_var_ds = xr.open_dataset('data/other_variables.nc')

def compile_cmip_model_data(climate_model_name,
                            scenario,
                            climate_model_files,
                            other_var_ds,
                            chunk_sizes):
    """
    Put together a single xarray dataset for a specified cmip model/scenario.
    
    The time range will depend on the files available and passed inside
    climate_model_files.

    Parameters
    ----------
    climate_model_name : str
        name  of model. eg ccsm4, ggfl.
    scenario : str
        scenario name, (rcp26, rcp45, etc)
    climate_model_files : list of strs
        file paths for all associated nc files. passed to xr.open_mfdataset
    other_var_ds : xr.Dataset
        the other_variables.nc dataset object for soil/map variables
    chunk_sizes : dict
        chunk sizes passed to all xarray functions.

    Returns
    -------
    xarray dataset of all phenograss variables for the specified model/scenario

    """
    climate = xr.open_mfdataset(climate_model_files, combine='by_coords', chunks=chunk_sizes)
    
    # Switch from longitude of 0-360 (default in cmip) to -180 - 180
    climate['longitude'] = climate.longitude - 360
    
    # TODO: need to to this inside the dask one, so probably need to non-dask one
    # to accept/return numpy arrays only
    radiation = create_radiation_data_array(ref = climate)
    radiation = radiation.chunk(chunk_sizes)
    
    et = create_et_data_array(tmin = climate.tasmin, tmax = climate.tasmax, radiation=radiation)
    et = et.chunk(chunk_sizes)

    # The other_var ds needs all chunks except time
    other_var_ds = other_var_ds.chunk({k:chunk_sizes[k] for k in ['latitude','longitude']}) 
    
    all_vars = xr.merge([climate, radiation, et, other_var_ds])
    all_vars = all_vars.chunk(chunk_sizes)
    
    # Smoothing the temperature with a simple moving average for now.
    # Methodology from Hufkins uses a window of the  prior 15 days.
    # TODO: setup test to make sure apply_ufunc rolling mean function matches
    # the xarray rolling mean function. Not neccesarrily to a high precicion though.
    #all_vars['tmean'] = ((all_vars.tasmin + all_vars.tasmax) / 2).rolling(time=15, center=False).mean().chunk(chunk_sizes)
    all_vars['tmean'] = rolling_tmean(ds = all_vars, window_size = 15)
    
    
    all_vars = all_vars.expand_dims({'model':[climate_model_name]})
    all_vars = all_vars.expand_dims({'scenario':[scenario]})
    all_vars = all_vars.transpose('time','latitude','longitude','model','scenario')
    
    return all_vars


def rolling_tmean(ds, window_size=15):
    """ 
    xarray as a moving window average method but it does not do lazy computations,
    so here is a custom one using bottleneck.move_mean
    """
    def move_mean_wrapper(tasmin, tasmax):
        return bn.move_mean(( (tasmin + tasmax) / 2), window=window_size, axis=-1)
    
    return xr.apply_ufunc(move_mean_wrapper,
                          ds.tasmin,
                          ds.tasmax,
                          input_core_dims = [['time'],['time']],
                          output_core_dims=[['time']],
                          output_dtypes=[np.float32],
                          dask = 'parallelized',
                          )

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
    # For every datapoint (ie. every time/lat/lon) there needs to be
    # latitude and doy info. Making a 0 filled data array, referenced
    # to the ref dataset, and broadcast the latitude and doy values across it.
    lat_array = xr.zeros_like(ref.pr) + ref.latitude
    doy_array = xr.zeros_like(ref.pr) + ref['time.dayofyear']
    
    latitude_radians = xr.apply_ufunc(et_utils.deg2rad,
                                      lat_array,
                                      dask='allowed')
    
    solar_dec = xr.apply_ufunc(et_utils.sol_dec,
                               doy_array,
                               dask='allowed')
    
    sha = xr.apply_ufunc(et_utils.sunset_hour_angle,
                         solar_dec,
                         solar_dec,
                         dask='allowed')
    
    ird = xr.apply_ufunc(et_utils.inv_rel_dist_earth_sun,
                         doy_array,
                         dask='allowed')
    
    radiation = xr.apply_ufunc(et_utils.et_rad,
                               latitude_radians,
                               solar_dec,
                               sha,
                               ird,
                               dask='allowed')

    return radiation.rename('radiation')

def create_radiation_array_dask(ref):
    return xr.apply_ufunc(create_radiation_data_array,
                          ref.time, ref.doy, ref.lat, ref.lon,
                          dask='allowed')



def apply_phenograss_dask_wrapper(model, ds):
    """
    Apply the phenograss model (from GrasslandModels package)
    to an xarray dataset.
    Specifically this wraps the function around apply_ufunc,
    which allows the process to be split on an HPC via
    dask and dask.distributed.

    Parameters
    ----------
    model : 
        A GrasslandModel.models.PhenoGrass model type.
    ds : 
        Xarray dataset with all required phenograss 

    Returns
    -------
        xarray DataArray of phenograss output. All input coordinates will be
        returned (eg. scenario, model)

    """
    #TODO: make sure to return fCover here as by default it returns GCC
    
    def model_wrapper(precip, evap, Ra, Tm, Wcap, Wp, MAP):
        # ufunc will move the core axis, time, to the end ,but GrasslandModels
        # needs it at the begining.
        precip = np.moveaxis(precip, -1, 0)
        evap = np.moveaxis(evap, -1, 0)
        Ra = np.moveaxis(Ra, -1, 0)
        Tm = np.moveaxis(Tm, -1, 0)
        model_output= model.predict(predictors={'precip': precip.astype('float32'),
                                         'evap'  : evap.astype('float32'),
                                         'Ra'    : Ra.astype('float32'),
                                         'Tm'    : Tm.astype('float32'),
                                         'Wcap'  : Wcap.astype('float32'),
                                         'Wp'    : Wp.astype('float32'),
                                         'MAP'   : MAP.astype('float32')})
        return np.moveaxis(model_output, 0,-1)        
    
    return xr.apply_ufunc(model_wrapper,
                          ds.pr,
                          ds.et,
                          ds.radiation,
                          ds.tmean,
                          ds.Wcap,
                          ds.Wp,
                          ds.MAP,
                          input_core_dims = [['time'],['time'],['time'],['time'],[],[],[]],
                          output_core_dims=[['time']],
                          dask = 'parallelized',
                          output_dtypes=[float]
                          )


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




