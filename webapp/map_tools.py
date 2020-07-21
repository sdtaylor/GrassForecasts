# for build_geojson_grid
from geopandas import GeoDataFrame, GeoSeries
from shapely.geometry import Polygon


def build_geojson_grid(reference_xarray, polygon_resolution, geojson_file=None):
    """
    Make a geojson of a grid (of polygons) with the extent and resolution of 
    reference_xarray, an xarray dataset/dataArray with latitude/longitude coordinates. 
    polygon_resolution is the size of the grid polygons.
    polygon_resolution gets 0.001 subtracted so the polygons dont overlap
    
    Will save as geojson file specified by filename, if not then it will
    return a geopandas geodataframe.
    """
    polygon_resolution -= 0.001
    full_grid = []
    grid_names = []
    for i, row in reference_xarray[['latitude','longitude']].drop_duplicates().iterrows():
        lat, lon = row['latitude'], row['longitude']
        ur = (lon, lat)      # [-125,28]
        ul = (lon - polygon_resolution, lat)   # [-126,28]
        lr = (lon, lat + polygon_resolution)   # [-125,27]
        ll = (lon - polygon_resolution, lat + polygon_resolution) # [-126,27]
               
        full_grid.append(Polygon([ur,ul,ll,lr]))
        grid_names.append({'latitude':lat,'longitude':lon})
            
    gdf = GeoDataFrame(grid_names,geometry = GeoSeries(full_grid), crs='EPSG:4326')
    if geojson_file:
        gdf.to_file(geojson_file, driver='GeoJSON')
    else:
        return gdf
