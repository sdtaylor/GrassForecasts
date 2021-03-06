from glob import glob
from os import path

import xarray as xr
import pandas as pd
import numpy as np


"""
Take the phenograss files from apply_model_to_cmip in
data/phenograss_nc_files, convert to the annual integral
annomoly, upscale to a coarse spatial resolution, and save
to a csv file for use on the website.
"""

chunk_sizes = dict(latitude=100,
                   longitude=100,
                   time=2000)

# A mask of where the model is relavant. most of the USA will be excluded. 
mask = xr.open_dataarray('data/ecoregion_mask.nc')

phenograss_files = glob('data/phenograss_nc_files/phenograss_file*.nc4')

annual_integral_objs = []
for file_i, filepath in enumerate(phenograss_files):
    print('file: {i}'.format(i=file_i))
    ecoregion = path.basename(filepath).split('_')[-2]
    p = xr.open_dataset(filepath, chunks=chunk_sizes)
    
    # add an ecoregion dimension to pair it with the ecoregion mask
    p = p.expand_dims({'ecoregion':[ecoregion]})
    
    # Get annual integral. The sum of all fCover values in a calendar year
    p['time'] = p['time.year']
    annual_fCover = p.groupby('time').sum().compute()
    
    annual_integral_objs.append(annual_fCover)

annual_integral = xr.merge(annual_integral_objs)

# Combine all ecoregions into a single layer here. 
#
# The phenograss models are ecoregion specific, but for simplicity are 
# applied over the entire USA climate grids. So, for each location the correct
# model prediction for that ecoregion must be pulled.
#
# In the ecoregion mask every location has an ecoregion dimension. 
# ie for 3 ecoregions it would be [1,0,0] for ['GrPlains','NWForests','ETemperateForest']
# Thus taking the average using the ecoregion mask as weights here will
# pull the correction value.
annual_integral = annual_integral.weighted(mask.astype(int)).mean('ecoregion') 

annual_integral = annual_integral.to_dataframe().reset_index()

# NA values are locations where no ecoregion was specified. ie. a mask of [0,0,0]
# This happens outside the ecoregions designated in create_ecoregion_mask.py
annual_integral = annual_integral.dropna(0)

# coarsen to 0.5 degree lat/lon
annual_integral['latitude'] = np.floor(annual_integral.latitude*2)/2
annual_integral['longitude'] = np.floor(annual_integral.longitude*2)/2

annual_integral = annual_integral.groupby(['latitude','longitude','model','scenario','time']).agg({'fCover':'mean'}).reset_index()

annual_integral = annual_integral.rename(columns={'time':'year'})

# knock of some digits to save space in the csv
for col in ['fCover']:
    annual_integral[col] = annual_integral[col].round(4)

annual_integral.to_csv('data/phenograss_downscaled_annual_integral.csv', index=False)
