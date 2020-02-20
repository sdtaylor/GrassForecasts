import xarray as xr
import numpy as np
import xmap

"""
Take the original soil rasters in data/soil_rasters, convert them to the
CMIP5 grid (also subsetting to north america in the process)
and saving as netcdf

uses xmap, which is no longer maintained but works pretty well with xarray

pip install git+git://github.com/sdtaylor/xmap
"""

# This does not account for a different CRS, but actually changing the CRS results
# in minute differences. It also doesn't explicitely account for the original dataarray being
# worldwide and the target being N. america. But the internals of xmap (a kdtree lookup)
# seem to deal with this fine.
def spatial_downscale(ds, target_array, method = 'distance_weighted', 
                      downscale_args={'k':2}):
    assert isinstance(target_array, xr.DataArray), 'target array must be DataArray'
    ds_xmap = xmap.XMap(ds, debug=False)
    ds_xmap.set_coords(x='longitude',y='latitude')
    downscaled = ds_xmap.remap_like(target_array, xcoord='longitude', ycoord='latitude',
                                    how=method, **downscale_args)
    return downscaled

reference = xr.open_dataset('data/cmip5_nc_files/BCCAv2_0.125deg_pr_day_CCSM4_rcp26_r1i1p1_20060101-20151231.nc4') 

target_array = reference.pr.isel(time=1)
# Need to set the long/lat names explicetly here
# also convert longitude from 0-360 to -180 - 180
# TODO: make sure that conversion applies on other machines
target_array['longitude'] = target_array.longitude - 360
target_array['latitude'] = target_array.latitude



###################################################
###################################################
soil_rasters = {'Wcap':'data/soil_rasters/fieldcap.dat',
                'Wp'  :'data/soil_rasters/wiltpont.dat'}
all_variable_datasets = []
for var, filename in soil_rasters.items():
    ds = xr.open_rasterio(filename)
    ds = ds.rename({'x':'longitude','y':'latitude'})
    ds = ds.sel(band=1)
    
    scaled_ds = spatial_downscale(ds=ds, target_array=target_array).rename(var)
    # 
    na_value = ds.attrs['nodatavals'][0]
    scaled_ds = scaled_ds.where(scaled_ds!= na_value)
    
    all_variable_datasets.append(scaled_ds)
    
    
###################################################
# May or may not process MAP here
###################################################

precip_rasters = {1:'data/annual_precip/wc2.0_10m_prec_01.tif',
                  2:'data/annual_precip/wc2.0_10m_prec_02.tif',
                  3:'data/annual_precip/wc2.0_10m_prec_03.tif',
                  4:'data/annual_precip/wc2.0_10m_prec_04.tif',
                  5:'data/annual_precip/wc2.0_10m_prec_05.tif',
                  6:'data/annual_precip/wc2.0_10m_prec_06.tif',
                  7:'data/annual_precip/wc2.0_10m_prec_07.tif',
                  8:'data/annual_precip/wc2.0_10m_prec_08.tif',
                  9:'data/annual_precip/wc2.0_10m_prec_09.tif',
                 10:'data/annual_precip/wc2.0_10m_prec_10.tif',
                 11:'data/annual_precip/wc2.0_10m_prec_11.tif',
                 12:'data/annual_precip/wc2.0_10m_prec_12.tif'
                  }

precip_vars = []
for month, filename in precip_rasters.items():
    ds = xr.open_rasterio(filename)
    ds = ds.rename({'x':'longitude','y':'latitude'})
    ds = ds.sel(band=1)
    
    scaled_ds = spatial_downscale(ds=ds, target_array=target_array).rename('precip')
    # 
    na_value = ds.attrs['nodatavals'][0]
    scaled_ds = scaled_ds.where(scaled_ds != na_value) 
    
    scaled_ds = scaled_ds.expand_dims('month').assign_coords(month = ([month]))
    precip_vars.append(scaled_ds)
    
# Mean annual precip is the sum of mean monthly precip
MAP = xr.concat(precip_vars, dim='month').sum('month').rename('MAP')
all_variable_datasets.append(MAP)

all_variables = xr.merge(all_variable_datasets)

assert np.all(all_variables.latitude == reference.latitude), 'latitude in new soil datasets not lining up'
assert np.all(all_variables.longitude == reference.longitude), 'longitude in new soil datasets not lining up'

all_variables.to_netcdf('data/other_variables.nc')




