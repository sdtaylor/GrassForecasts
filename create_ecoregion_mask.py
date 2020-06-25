import geopandas as gpd
import rasterio.mask
import rasterio
import xarray as xr
import numpy as np

# The extent and resolution of this file is used as a reference to make the mask
base_grid_file = 'data/cmip5_nc_files/BCCA_0.125deg_tasmax_day_CCSM4_rcp26_r1i1p1_20060101-20151231.nc4'


# Read in ecoregion shapefile and convert
ecoregions = gpd.read_file('data/ecoregions/NA_CEC_Eco_Level1.shp')

#                     Great plains
ecoregions_to_keep = ['GREAT PLAINS','EASTERN TEMPERATE FORESTS']

ecoregions = ecoregions[ecoregions.NA_L1NAME.isin(ecoregions_to_keep)]

ecoregions['geometry'] = ecoregions.geometry.simplify(2)
ecoregions = ecoregions.to_crs(4326)  
ecoregions = ecoregions.dissolve(by='NA_L1NAME')

# gimmie that shapely object
ecoregions_shp = ecoregions.geometry.values[0]

# Make a mask array based on the ecoregion and  referenced to the
# resolution/extent of the base_grid
# invert = True means locations *within* the shapefile  will be marked tru
with rasterio.open(base_grid_file) as base_grid_rio:
    mask, mask_transform, mask_window = rasterio.mask.raster_geometry_mask(base_grid_rio, 
                                                                           shapes = ecoregions_shp,
                                                                           invert = True)

# rasterio and geopandas getting confused and somehow flipping the y axis here...
mask = np.flip(mask,0)

# Make an empty xarray dataset references to the base_grid
with xr.open_dataset(base_grid_file) as base_grid_nc:
    mask_nc = xr.zeros_like(base_grid_nc.tasmax.isel(time=1)).drop('time').rename('ecoregion_mask')

# Combine the two & save
mask_nc[:] = mask

# match the longitude to phenograss output, website input
mask_nc['longitude'] = mask_nc.longitude - 360

mask_nc.attrs = {'notes':'mask using ecoregions, with spatial extent/scale of the reference dataset. True indicates where the ecoregion is located.',
                 'ecoregions' : ','.join(ecoregions_to_keep),
                 'reference_dataset':base_grid_file}

mask_nc.to_netcdf('data/ecoregion_mask.nc')
