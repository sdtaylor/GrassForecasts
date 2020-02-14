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

soil_rasters = {'Wcap':'data/soil_rasters/fieldcap.dat',
                'Wp'  :'data/soil_rasters/wiltpont.dat'}
converted = []
for var, filename in soil_rasters.items():
    ds = xr.open_rasterio(filename)
    ds = ds.rename({'x':'longitude','y':'latitude'})
    ds = ds.sel(band=1)
    converted.append(spatial_downscale(ds=ds, target_array=target_array).rename(var))
    
both = xr.merge(converted)

assert np.all(both.latitude == reference.latitude), 'latitude in new soil datasets not lining up'
assert np.all(both.longitude == reference.longitude), 'longitude in new soil datasets not lining up'

both.to_netcdf('data/soil_variable.nc')
