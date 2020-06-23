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

#################################################
# Layout all the data

climate_data_folder = 'data/cmip5_nc_files/'
climate_model_info = [{'climate_model_name':'ccsm4',
                       'scenario':'rcp26',
                       'model_file_search_str': '*CCSM4_rcp26*.nc4'},
                       {'climate_model_name':'ccsm4',
                       'scenario':'rcp45',
                       'model_file_search_str': '*CCSM4_rcp45*.nc4'},
                      
                        {'climate_model_name':'csiro',
                         'scenario':'rcp26',
                         'model_file_search_str': '*CSIRO-Mk3-6-0_rcp26*.nc4'},
                        {'climate_model_name':'csiro',
                         'scenario':'rcp45',
                         'model_file_search_str': '*CSIRO-Mk3-6-0_rcp45*.nc4'},
                      
                        {'climate_model_name':'gfdl',
                         'scenario':'rcp26',
                         'model_file_search_str': '*GFDL-ESM2G_rcp26*.nc4'},
                        {'climate_model_name':'gfdl',
                         'scenario':'rcp45',
                         'model_file_search_str': '*GFDL-ESM2G_rcp45*.nc4'}
                      ]
    
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
mask['longitude'] = mask.longitude - 360


all_climate = []
for ds_i, ds_info in enumerate(climate_model_info):
    model_files = glob(climate_data_folder + ds_info['model_file_search_str'])
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

    baseline_avg = ann[ann.time<=2020].groupby(['latitude','longitude','model','scenario']).agg({'tmean':'mean','pr':'mean'}).reset_index()
    baseline_avg.rename(columns={'tmean':'tmean_climatology','pr':'pr_climatology'}, inplace=True)

    ann = pd.merge(ann, baseline_avg, on=['latitude','longitude','model','scenario'], how='left')
    ann['tmean_annomoly'] = ann.tmean / ann.tmean_climatology
    ann['pr_anomaly'] = ann.pr / ann.pr_climatology
  
    # knock of some digits to save space in the csv
    for col in ['tmean','tmean_climatology','tmean_annomoly','pr','pr_climatology','pr_anomaly']:
        ann[col] = ann[col].round(3)


    all_climate.append(ann)
    ds.close()

pd.concat(all_climate).to_csv('data/climate_annual_data.csv', index=False)
