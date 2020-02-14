import xarray as xr
import xmap




# This does not account for a different CRS, but actually changing the CRS results
# in minute differences. It also doesn't explicitely account for CFS being
# worldwide and prism being N. america. But the internals of xmap (a kdtree lookup)
# seem to deal with this fine.
def spatial_downscale(ds, target_array, method = 'distance_weighted', data_var='tmean', 
                      time_dim='forecast_time', downscale_args={'k':2}):
    assert isinstance(target_array, xr.DataArray), 'target array must be DataArray'
    ds_xmap = xmap.XMap(ds, debug=False)
    ds_xmap.set_coords(x='longitude',y='latitude')
    downscaled = ds_xmap.remap_like(target_array, xcoord='longitude', ycoord='latitude',
                                    how=method, **downscale_args)
    return downscaled.to_dataset(name=data_var)

ds = xr.open_rasterio('data/soil_rasters/fieldcap.dat')
ds = ds.rename({'x':'longitude','y':'latitude'})

reference = xr.open_dataset('data/cmip5_nc_files/BCCAv2_0.125deg_pr_day_CCSM4_rcp26_r1i1p1_20060101-20151231.nc4') 

# TODO: check to make sure longitude needs the - 360 on other systems
target_array = xr.DataArray(dims=('longitude','latitude'),
                            coords={'longitude':reference.longitude-360, 'latitude':reference.latitude})



x= spatial_downscale(ds=ds, target_array=target_array)
