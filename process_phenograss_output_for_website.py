from glob import glob

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
    p = xr.open_dataset(filepath, chunks=chunk_sizes)
    p['time'] = p['time.year']
    annual_fCover = p.groupby('time').sum().compute()
    #upscaled = p.coarsen(latitude=6).mean().coarsen(longitude=6).mean().compute()
    
    annual_integral_objs.append(annual_fCover)

annual_integral = xr.combine_by_coords(annual_integral_objs)

annual_integral = xr.merge([annual_integral, mask.astype(bool)])
annual_integral = annual_integral.to_dataframe().reset_index()

annual_integral = annual_integral[annual_integral.ecoregion_mask]


# coarsen to 0.5 degree lat/lon
annual_integral['latitude'] = np.floor(annual_integral.latitude*2)/2
annual_integral['longitude'] = np.floor(annual_integral.longitude*2)/2

annual_integral = annual_integral.groupby(['latitude','longitude','model','scenario','time']).agg({'fCover':'mean'}).reset_index()

annual_integral = annual_integral.rename(columns={'time':'year'})

# knock of some digits to save space in the csv
for col in ['fCover']:
    annual_integral[col] = annual_integral[col].round(4)

annual_integral.to_csv('phenograss_ann_integral.csv', index=False)
