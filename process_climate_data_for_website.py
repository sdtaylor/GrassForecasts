import pandas as pd
import numpy as np
import GrasslandModels

from glob import glob
from time import sleep
import os

from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from dask.diagnostics import ProgressBar
import dask

from tools import cmip5_file_tools


"""
This script downscales the original cmip5 data to annual values and a spatial
resolution to match the phenograss output.

I can be setup to run on a dask cluster, but its easier to just get a single HPC 
with 128GB+ of memory and run it there.
"""

#################################################
# Layout all the data

climate_data_folder = 'data/cmip5_nc_files/'
climate_model_info = cmip5_file_tools.get_cmip5_spec(models='all',scenarios='all')
    
chunk_sizes = {'latitude':-1 ,'longitude':-1,'time':-1}

###################################################
#client = Client(n_workers=32, memory_limit='2GB', local_directory='/tmp/')
###################################################
# Don't import xarray until here so that it registers with dask
import xarray as xr
from tools import xarray_tools

###################################################
# A mask of where the model is relavant. most of the USA will be excluded. 
mask = xr.open_dataarray('data/ecoregion_mask.nc')

all_climate = []
for ds_i, ds_info in enumerate(climate_model_info):
    model_files = cmip5_file_tools.get_cmip5_files(model_spec = ds_info,
                                                   base_folder = climate_data_folder,
                                                   get_historic=True)
    
    ds = xarray_tools.compile_cmip_data(climate_model_name =  ds_info['climate_model_name'], 
                                              scenario =            ds_info['scenario'], 
                                              climate_model_files = model_files, 
                                              chunk_sizes =         chunk_sizes)

    ds.load()
    ds['time'] = ds['time.year']
    ann_temp = ds.tmean.groupby('time').mean().compute()
    ann_pr   = ds.pr.groupby('time').sum().compute()

    ann = xr.merge([ann_temp,ann_pr])
    ann = xr.merge([ann, mask.astype(bool)]).to_dataframe().reset_index()
    ann = ann[ann.ecoregion_mask]

    # coursen the cells a tad, agregating to the mean within them
    ann['latitude'] = np.floor(ann.latitude*2)/2
    ann['longitude'] = np.floor(ann.longitude*2)/2

    ann = ann.groupby(['latitude','longitude','model','scenario','time']).agg({'tmean':'mean','pr':'mean'}).reset_index()

    ann = rename(columns={'time':'year'})
  
    # knock of some digits to save space in the csv
    for col in ['tmean','pr']:
        ann[col] = ann[col].round(3)

    all_climate.append(ann)
    ds.close()

pd.concat(all_climate).to_csv('data/climate_annual_data.csv', index=False)
