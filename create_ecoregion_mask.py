import geopandas as gpd
import rasterio.mask
import rasterio
import xarray as xr
import numpy as np


"""
This script creates a mask netCDF file for the specified ecoregions below. It's
designed to be used in the process_phenograss_output_for_website script so that
each grid cell gets fCover values from only its respective ecoregion model.
"""


# The extent and resolution of this file is used as a reference to make the mask
base_grid_file = 'data/cmip5_nc_files/BCCA_0.125deg_tasmax_day_CCSM4_rcp26_r1i1p1_20060101-20151231.nc4'


# Read in ecoregion shapefile and convert
ecoregions = gpd.read_file('data/ecoregions/NA_CEC_Eco_Level1.shp')

# The official name along with a short name
ecoregions_to_keep =  ['NORTHWESTERN FORESTED MOUNTAINS',
                      'GREAT PLAINS',
                      'EASTERN TEMPERATE FORESTS']
ecoregion_shortnames = ['NWForests', 'GrPlains','ETempForests']


ecoregions = ecoregions[ecoregions.NA_L1NAME.isin(ecoregions_to_keep)]

ecoregions['geometry'] = ecoregions.geometry.simplify(2)
ecoregions = ecoregions.to_crs(4326)  
#ecoregions = ecoregions.dissolve(by='NA_L1NAME')


# Make a mask array based on each ecoregion and  referenced to the
# resolution/extent of the base_grid
# invert = True means locations *within* the shapefile  will be marked tru
with rasterio.open(base_grid_file) as base_grid_rio:
    mask = []
    for ecoregion_name in ecoregions_to_keep:
        ecoregion_shapely = ecoregions[ecoregions.NA_L1NAME==ecoregion_name].geometry.values
        m, _, _ = rasterio.mask.raster_geometry_mask(base_grid_rio, 
                                                     shapes = ecoregion_shapely,
                                                     all_touched=True,
                                                     invert = True)
        # rasterio and geopandas getting confused and somehow flipping the y axis here...
        m = np.flip(m,0)
        mask.append(m)

# single numpy array of shape (ecoregions, lat, lon) to shove into array
mask = np.stack(mask)

# Make an empty xarray dataset referenced to the base_grid.
# where each ecoregion is a different boolean layer
with xr.open_dataset(base_grid_file) as base_grid_nc:
    mask_nc = xr.DataArray(mask,
                           dims = ('ecoregion','latitude','longitude'),
                           coords = {'ecoregion' : ecoregion_shortnames,
                                     'latitude'  : base_grid_nc.latitude,
                                     'longitude' : base_grid_nc.longitude})

mask_nc = mask_nc.to_dataset(name='ecoregion_mask')

# match the longitude to phenograss output, website input
mask_nc['longitude'] = mask_nc.longitude - 360

mask_nc.attrs = {'notes':'mask using ecoregions, with spatial extent/scale of the reference dataset. True indicates where the ecoregion is located.',
                 'ecoregions' : ','.join(ecoregions_to_keep),
                 'reference_dataset':base_grid_file}

mask_nc.to_netcdf('data/ecoregion_mask.nc')

# Also create a downscaled dataframe to use in website stuff, that way xarray isn't needed there
mask_df = mask_nc.to_dataframe().reset_index()

mask_df['latitude'] = np.floor(mask_df.latitude*2)/2
mask_df['longitude'] = np.floor(mask_df.longitude*2)/2

mask_df = mask_df.groupby(['latitude','longitude']).agg({'ecoregion_mask':'max'}).reset_index()

mask_df['ecoregion_mask'] = mask_df.ecoregion_mask.astype(bool)
mask_df.to_csv('webapp/data/ecoregion_mask.csv', index=False)
