import pandas as pd
import numpy as np
import GrasslandModels

from glob import glob
from time import sleep

from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from dask.diagnostics import ProgressBar
import dask

#################################################
# Layout all the data

climate_data_folder = 'data/cmip5_nc_files/'
climate_model_info = [{'climate_model_name':'ccsm4',
                       'scenario':'rcp26',
                       'model_file_search_str': '*CCSM4_rcp26*.nc4'},
                      # {'climate_model_name':'ccsm4',
                      # 'scenario':'rcp45',
                      # 'model_file_search_str': '*CCSM4_rcp45*.nc4'},
                      
                       # {'climate_model_name':'csiro',
                       #  'scenario':'rcp26',
                       #  'model_file_search_str': '*CSIRO-Mk3-6-0_rcp26*.nc4'},
                       # {'climate_model_name':'csiro',
                       #  'scenario':'rcp45',
                       #  'model_file_search_str': '*CSIRO-Mk3-6-0_rcp45*.nc4'},
                      
                       # {'climate_model_name':'gfdl',
                       #  'scenario':'rcp26',
                       #  'model_file_search_str': '*GFDL-ESM2G_rcp26*.nc4'},
                       # {'climate_model_name':'gfdl',
                       #  'scenario':'rcp45',
                       #  'model_file_search_str': '*GFDL-ESM2G_rcp45*.nc4'}
                      ]
    
chunk_sizes = {'latitude':50 ,'longitude':50,'time':-1}

###################################################
# Don't import xarray until here so that it registers with dask
import xarray as xr
from tools import xarray_tools

all_climate = []
for ds_i, ds_info in enumerate(climate_model_info):
    model_files = glob(climate_data_folder + ds_info['model_file_search_str'])
    ds = xarray_tools.compile_cmip_data(climate_model_name =  ds_info['climate_model_name'], 
                                              scenario =            ds_info['scenario'], 
                                              climate_model_files = model_files, 
                                              chunk_sizes =         chunk_sizes)

    ds['time'] = ds['time.year']
    ann_temp = ds.tmean.groupby('time').mean()
    ann_pr   = ds.pr.groupby('time').sum()

    ann = xr.combine_by_coords([ann_temp,ann_pr])

