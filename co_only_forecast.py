import xarray as xr
import pandas as pd
import numpy as np
from tools.etc import *
import GrasslandModels


climate_models = {'ccsm4':['data/NE_CO_ccsm4.nc4'],
                  'csiro':['data/NE_CO_csiro.nc4'],
                  'gfdl' :['data/NE_CO_gfdl.nc4']}

chunk_sizes = {'latitude':10,'longitude':10,'time':-1,'scenario':-1}

climate_model_name = 'ccsm4'
climate_model_files = 'data/NE_CO_ccsm4.nc4'

all_phenograss_output = []
all_climate_models = []
for climate_model_name, climate_model_files in climate_models.items():
    
    climate = xr.open_mfdataset(climate_model_files, chunks=chunk_sizes)
    climate['longitude'] = climate.longitude - 360
    radiation = create_radiation_data_array(time = climate.time, doy=climate['time.dayofyear'],
                                            latitude = climate.latitude, longitude = climate.longitude)
    radiation = xr.merge([radiation.expand_dims({'scenario':[s]}) for s in climate.scenario.values])
    radiation = radiation.chunk(chunk_sizes)
    
    et = create_et_data_array(tmin = climate.tasmin, tmax = climate.tasmax, radiation=radiation.radiation)
    et = et.chunk(chunk_sizes)
    
    other_vars = xr.open_dataset('data/other_variables.nc')
    other_vars = other_vars.sel(latitude=climate.latitude, longitude=climate.longitude)
    other_vars = xr.merge([other_vars.expand_dims({'scenario':[s]}) for s in climate.scenario.values])
    other_vars = other_vars.chunk({k:chunk_sizes[k] for k in ['latitude','longitude','scenario']}) # needs all chunks except time
    
    all_vars = xr.merge([climate, radiation, et, other_vars])
    #all_vars['tmean'] = (all_vars.tasmin + all_vars.tasmax) / 2
    
    # Simple moving average for now
    all_vars['tmean'] = ((all_vars.tasmin + all_vars.tasmax) / 2).rolling(time=15, center=False).mean().chunk(chunk_sizes)
    
    all_vars = all_vars.transpose('time','latitude','longitude','scenario')
    all_vars = all_vars.expand_dims({'model':[climate_model_name]})
    
    all_climate_models.append(all_vars)

all_climate_models =  xr.combine_by_coords(all_climate_models)


    
def apply_phenograss_dask_wrapper(model, ds):
    
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


phenograss = GrasslandModels.utils.load_prefit_model('PhenoGrass-original')
phenograss.set_internal_method('numpy')

phenograss_ds = apply_phenograss_dask_wrapper(model = phenograss, ds=all_climate_models).compute()

phenograss_ds = phenograss_ds.to_dataset(name='fCover')

##############
# Create a single northeast CO forecast. this was used in the  ESA abstarct   
#xr.combine_by_coords(all_phenograss_output).mean(['latitude','longitude']).to_dataframe().reset_index().to_csv('data/NE_CO_phenograss.csv', index=False)

#############
# Convert to dataframe, aggregate to larger spatial scales
phenograss_df = phenograss_ds.to_dataframe().reset_index()

phenograss_df['latitude'] =  phenograss_df.latitude.round(0)
phenograss_df['longitude'] =  phenograss_df.longitude.round(0)
phenograss_df = phenograss_df.groupby(['latitude','longitude','model','scenario','time']).agg({'phenograss_prediction':'mean'}).reset_index()

#year_leftover = pd.DatetimeIndex(phenograss_df.time).year % 10
#phenograss_df['decade'] =   pd.DatetimeIndex(phenograss_df.time).year - year_leftover

phenograss_df['doy']    = pd.DatetimeIndex(phenograss_df.time).dayofyear
phenograss_df['year']    =  pd.DatetimeIndex(phenograss_df.time).year


# TODO: implement date of peak yearly production
# needs to be within groupby though....
def peak_doy(df):
    return df.doy[df.phenograss_prediction.argmax()]

x = phenograss_df.groupby(['latitude','longitude','model','scenario','year']).agg({'phenograss_prediction':'sum',
                                                                                              'time':'count'}).reset_index()

# sanity check
assert set(x.time.unique())==set([365,366]), 'Some years with < or > 365/366 days'
assert x.phenograss_prediction.max() < 366, 'some annual productivity > 365'
assert x.phenograss_prediction.min() >= 0, 'some annual productivity < 0'

x.drop(columns=['time']).rename(columns={'phenograss_prediction':'annual_productivity'}).to_csv('data/NE_CO_phenograss.csv',index=False)
