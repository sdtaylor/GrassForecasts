import glob
import os

# for build_geojson_grid
from geopandas import GeoDataFrame, GeoSeries
from shapely.geometry import Polygon

def cmip_file_query(folder):
    file_list = glob.glob(folder+'/*.nc4')
    all_file_info = []
    for f in file_list:
        filename = os.path.basename(f)
        parts = filename.split('_')
        file_info = {'model'   : parts[4].lower(),
                     'scenario': parts[5],
                     'variable': parts[2],
                     'run'     : parts[6],
                     'decade'  : parts[7][0:4],
                     'full_path': f,
                     'filename':filename}
        
        all_file_info.append(file_info)

    return all_file_info



def build_geojson_grid(reference_xarray, geojson_file=None, polygon_buffer=0.999):
    """
    Make a geojson of a grid (of polygons) with the extent and resolution of 
    reference_xarray, an xarray dataset/dataArray with latitude/longitude coordinates. 
    polygon_buffer is the  spacing between grid polygons.
    
    Will save as geojson file specified by filename, if not then it will
    return a geopandas geodataframe.
    """
    full_grid = []
    grid_names = []
    for i, row in reference_xarray[['latitude','longitude']].drop_duplicates().iterrows():
        lat, lon = row['latitude'], row['longitude']
        ur = (lon, lat)      # [-125,28]
        ul = (lon - polygon_buffer, lat)   # [-126,28]
        lr = (lon, lat + polygon_buffer)   # [-125,27]
        ll = (lon - polygon_buffer, lat + polygon_buffer) # [-126,27]
               
        full_grid.append(Polygon([ur,ul,ll,lr]))
        grid_names.append({'latitude':lat,'longitude':lon})
            
    gdf = GeoDataFrame(grid_names,geometry = GeoSeries(full_grid), crs='EPSG:4326')
    if geojson_file:
        gdf.to_file(geojson_file, driver='GeoJSON')
    else:
        return gdf