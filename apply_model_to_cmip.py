import xarray as xr
import pandas as pd
import numpy as np
from tools import xarray_tools
import GrasslandModels

from glob import glob

climate_data_folder = 'data/cmip5_nc_files/'
climate_model_info = [{'climate_model_name':'ccsm4',
                       'scenario':'rcp26',
                       'model_file_search_str': '*CCSM4_rcp26*.nc4'},
                      # {'climate_model_name':'ccsm4',
                      # 'scenario':'rcp45',
                      # 'model_file_search_str': '*CCSM4_rcp45*.nc4'},
                      
                      # {'climate_model_name':'csiro',
                      #  'scenario':'rcp26',
                      #  'model_file_search_str': glob('*CSIRO-Mk3-6-0_rcp26*.nc4')},
                      # {'climate_model_name':'csiro',
                      #  'scenario':'rcp45',
                      #  'model_file_search_str': glob('*CSIRO-Mk3-6-0_rcp45*.nc4')},
                      
                      # {'climate_model_name':'gfdl',
                      #  'scenario':'rcp26',
                      #  'model_file_search_str': glob('*GFDL-ESM2G_rcp26*.nc4')},
                      # {'climate_model_name':'gfdl',
                      #  'scenario':'rcp45',
                      #  'model_file_search_str': glob('*GFDL-ESM2G_rcp45*.nc4')}
                      ]
    

chunk_sizes = {'latitude':4,'longitude':4,'time':-1}

#climate_model_name = 'ccsm4'
#climate_model_files = 'data/NE_CO_ccsm4.nc4'
other_var_ds = xr.open_dataset('data/other_variables.nc')

all_climate_models = []
for ds_info in climate_model_info:
    model_files = glob(climate_data_folder + ds_info['model_file_search_str'])
    ds = xarray_tools.compile_cmip_model_data(climate_model_name =  ds_info['climate_model_name'], 
                                              scenario =            ds_info['scenario'], 
                                              climate_model_files = model_files, 
                                              other_var_ds =        other_var_ds, 
                                              chunk_sizes =         chunk_sizes)
    all_climate_models.append(ds)
    
all_climate_models = xr.merge(all_climate_models)
