import xarray as xr
import pandas as pd
from tools.etc import *
import GrasslandModels


climate_models = {'ccsm4':['data/NE_CO_ccsm4.nc4'],
                  'csiro':['data/NE_CO_csiro.nc4'],
                  'gfdl' :['data/NE_CO_gfdl.nc4']}
all_phenograss_output = []
for climate_model_name, climate_model_files in climate_models.items():
    
    climate = xr.open_mfdataset(climate_model_files, chunks={'latitude':10,'longitude':10,'time':-1})
    climate['longitude'] = climate.longitude - 360
    radiation = create_radiation_data_array(time = climate.time, doy=climate['time.dayofyear'],
                                            latitude = climate.latitude, longitude = climate.longitude)
    radiation = xr.merge([radiation.expand_dims({'scenario':[s]}) for s in climate.scenario.values])
    et = create_et_data_array(tmin = climate.tasmin, tmax = climate.tasmax, radiation=radiation.radiation).compute()
    
    other_vars = xr.open_dataset('data/other_variables.nc')
    other_vars = other_vars.sel(latitude=climate.latitude, longitude=climate.longitude)
    other_vars = xr.merge([other_vars.expand_dims({'scenario':[s]}) for s in climate.scenario.values])
    
    all_vars = xr.merge([climate, radiation, et, other_vars])
    #all_vars['tmean'] = (all_vars.tasmin + all_vars.tasmax) / 2
    
    # Simple moving average for now
    all_vars['tmean'] = ((all_vars.tasmin + all_vars.tasmax) / 2).rolling(time=15, center=False).mean()
    
    all_vars = all_vars.transpose('time','latitude','longitude','scenario')
    
    # Running on the full climate model on the cluster will look something like this
    # def apply_phenograss(ds, model):
    #     return xr.apply_ufunc(model.predict,
    #                           {'predictors':{'precip': ds.pr.values.astype('float32'),
    #                                          'evap'  : ds.et.values.astype('float32'),
    #                                          'Ra'    : ds.radiation.values.astype('float32'),
    #                                          'Tm'    : ds.tmean.values.astype('float32'),
    #                                          'Wcap'  : ds.Wcap.values.astype('float32'),
    #                                          'Wp'    : ds.Wp.values.astype('float32'),
    #                                          'MAP'   : ds.MAP.values.astype('float32')}})
    
    phenograss = GrasslandModels.utils.load_prefit_model('PhenoGrass-original')
    phenograss.set_internal_method('numpy')
    
    phenograss_output = phenograss.predict(predictors={'precip': all_vars.pr.values.astype('float32'),
                                                                       'evap'  : all_vars.et.values.astype('float32'),
                                                                       'Ra'    : all_vars.radiation.values.astype('float32'),
                                                                         'Tm'    : all_vars.tmean.values.astype('float32'),
                                                                         'Wcap'  : all_vars.Wcap.values.astype('float32'),
                                                                         'Wp'    : all_vars.Wp.values.astype('float32'),
                                                                         'MAP'   : all_vars.MAP.values.astype('float32')})
    
    # year_leftover = all_vars['time.year'].values % 10
    # decade =   all_vars['time.year'] - year_leftover
    # doy    = all_vars['time.dayofyear']
    
    phenograss_ds = xr.DataArray(phenograss_output,
                                 dims=('time','latitude', 'longitude', 'scenario'),
                                 coords= {'time':all_vars.time,
                                          'latitude':all_vars.latitude, 
                                          'longitude':all_vars.longitude, 
                                          'scenario' :all_vars.scenario}).to_dataset(name='phenograss_prediction')
 
    phenograss_ds = phenograss_ds.expand_dims({'model':[climate_model_name]})
    all_phenograss_output.append(phenograss_ds)

all_phenograss_output = xr.combine_by_coords(all_phenograss_output)
##############
# Create a single northeast CO forecast. this was used in the  ESA abstarct   
#xr.combine_by_coords(all_phenograss_output).mean(['latitude','longitude']).to_dataframe().reset_index().to_csv('data/NE_CO_phenograss.csv', index=False)

#############
# Convert to dataframe, aggregate to larger spatial scales
phenograss_df = all_phenograss_output.to_dataframe().reset_index()

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
