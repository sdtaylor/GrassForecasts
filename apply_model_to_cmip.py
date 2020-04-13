import pandas as pd
import numpy as np
import GrasslandModels

from glob import glob
from time import sleep

from dask_jobqueue import SLURMCluster
from dask.distributed import Client
import dask

#################################################
# Layout all the data

climate_data_folder = 'data/cmip5_nc_files/'
climate_model_info = [{'climate_model_name':'ccsm4',
                       'scenario':'rcp26',
                       'model_file_search_str': '*CCSM4_rcp26*.nc4'},
                      #{'climate_model_name':'ccsm4',
                      #'scenario':'rcp45',
                      #'model_file_search_str': '*CCSM4_rcp45*.nc4'},
                      
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
    
phenograss_encoding = {'fCover':{'zlib':True,
                              'complevel':4, 
                              'dtype':'float32', 
                              'scale_factor':0.001,  
                              '_FillValue': -9999}}

phenograss_output_folder = 'data/phenograss_nc_files/'

###############################################3
# Dask/ceres config stuff stuff
ceres_workers          = 100 # Number of slumr jobs started
ceres_cores_per_worker = 1 # number of cores per job
ceres_mem_per_worker   = '2GB' # memory for each job
ceres_worker_walltime  = '48:00:00' # the walltime for each worker, HH:MM:SS
ceres_partition        = 'short'    # short: 48 hours, 55 nodes
                                    # medium: 7 days, 25 nodes
                                    # long:  21 days, 15 nodes

chunk_sizes = {'latitude':4,'longitude':4,'time':-1}

######################################################
# Setup dask cluster
######################################################
cluster = SLURMCluster(processes=1, queue=ceres_partition, cores=ceres_cores_per_worker, memory=ceres_mem_per_worker, walltime=ceres_worker_walltime,
                       job_extra=[],
                       death_timeout=600, local_directory='/tmp/')

print('Starting up workers')
workers = cluster.scale(n=ceres_workers)
dask_client = Client(cluster)
print('Dask scheduler address: {a}'.format(a=dask_client.scheduler_info()['address']))
active_workers =  len(dask_client.scheduler_info()['workers'])
while active_workers < (ceres_workers-1):
    print('waiting on workers: {a}/{b}'.format(a=active_workers, b=ceres_workers))
    sleep(5)
    active_workers =  len(dask_client.scheduler_info()['workers'])
print('all workers online')
#dask_client = Client()


###################################################
# Don't import xarray until here so that it registers with dask
import xarray as xr
from tools import xarray_tools

#climate_model_name = 'ccsm4'
#climate_model_files = 'data/NE_CO_ccsm4.nc4'
other_var_ds = xr.open_dataset('data/other_variables.nc')

phenograss = GrasslandModels.utils.load_prefit_model('PhenoGrass-original')  
phenograss.set_internal_method('numpy')  

#all_phenograss_output = []
for ds_i, ds_info in enumerate(climate_model_info):
    model_files = glob(climate_data_folder + ds_info['model_file_search_str'])
    ds = xarray_tools.compile_cmip_model_data(climate_model_name =  ds_info['climate_model_name'], 
                                              scenario =            ds_info['scenario'], 
                                              climate_model_files = model_files, 
                                              other_var_ds =        other_var_ds, 
                                              chunk_sizes =         chunk_sizes)
    
    phenograss_ds = xarray_tools.apply_phenograss_dask_wrapper(model = phenograss, ds=ds).compute() 
    
    output_file = 'phenograss_file{i}_{m}_{s}.nc4'.format(i=ds_i, m=ds_info['climate_model_name'], s=ds_info['scenario'])
    
    phenograss_ds.to_dataset(name='fCover').to_netcdf(phenograss_output_folder + output_file,
                                                 encoding = phenograss_encoding)
    
    # The files get pretty big, so clear them out for the next round
    ds = phenograss_ds = None
    
    print('dataset {i} processing complete'.format(i=ds_i))