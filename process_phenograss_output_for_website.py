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

phenograss_files = glob('data/phenograss_nc_files/phenograss_file*.nc4')

annual_integral_objs = []
for file_i, filepath in enumerate(phenograss_files):
    print('file: {i}'.format(i=file_i))
    p = xr.open_dataset(filepath, chunks=chunk_sizes)
    p['time'] = p['time.year']
    annual_fCover = p.groupby('time').sum().compute()
    #upscaled = p.coarsen(latitude=6).mean().coarsen(longitude=6).mean().compute()
    
    annual_integral_objs.append(annual_fCover)

annual_integral = xr.combine_by_coords(annual_integral_objs).to_dataframe().reset_index()

# coarsen to 0.5 degree lat/lon
annual_integral['latitude'] = np.floor(annual_integral.latitude*2)/2
annual_integral['longitude'] = np.floor(annual_integral.longitude*2)/2

annual_integral = annual_integral.groupby(['latitude','longitude','model','scenario','time']).agg({'fCover':'mean'}).reset_index()

baseline_avg = annual_integral[annual_integral.time <= 2020].groupby(['latitude','longitude','model','scenario']).fCover.mean().reset_index()
baseline_avg.rename(columns={'fCover':'fCover_climatology'}, inplace=True)

annual_integral = pd.merge(annual_integral, baseline_avg, on=['latitude','longitude','model','scenario'], how='left')
annual_integral['fCover_annomoly'] = annual_integral.fCover / annual_integral.fCover_climatology

# knock of some digits to save space in the csv
for col in ['fCover','fCover_climatology','fCover_annomoly']:
    annual_integral[col] = annual_integral[col].round(2)

annual_integral.to_csv('phenograss_ann_integral.csv', index=False)