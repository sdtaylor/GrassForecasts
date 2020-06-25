import xarray as xr

from tools import cmip5_file_tools
from tools import xarray_tools

"""
Do some simple checks on all the cmip5 data to make sure its all accounted
for and loading.
"""

climate_data_folder = 'data/cmip5_nc_files/'
climate_model_info = cmip5_file_tools.get_cmip5_spec(models='all', scenarios='all')
    
chunk_sizes = {'latitude':100 ,'longitude':100, 'time':-1}

for ds_i, ds_info in enumerate(climate_model_info):
    model_files = cmip5_file_tools.get_cmip5_files(model_spec = ds_info,
                                                   base_folder = climate_data_folder,
                                                   get_historic=True)
    
    
    ds = xarray_tools.compile_cmip_data(climate_model_name =  ds_info['climate_model_name'], 
                                        scenario =            ds_info['scenario'], 
                                        climate_model_files = model_files, 
                                        chunk_sizes =         chunk_sizes)
    
    cmip5_file_tools.verify_cmip5_parts(ds,
                                        expected_vars=['pr','tasmin','tasmax','tmean'],
                                        expected_start_date = '1980-01-01',
                                        expected_end_date   = '2100-12-31')


# Thats it!
