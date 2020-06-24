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

def get_cmip5_spec(models='all',scenarios='all'):
    """ 
    Get a dictionary of specifications for specific models and scenarios
    Used in combination with get_cmip5_files()
    """
    assert isinstance(models, list) or models=='all', 'models must be list or all'
    assert isinstance(scenarios, list) or scenarios=='all', 'scenarios must be list or all'
    
    all_models = ['ccsm4','csiro']
    all_scenarios = ['rcp26','rcp45','rcp60','rcp85']
    
    if models == 'all':
        models = all_models
            
    if scenarios == 'all':
        scenarios = all_scenarios

    assert all([m in all_models for m in models]), 'not all models available. got:{g}, available:{a}'.format(g=models,a=all_models)
    assert all([m in all_scenarios for m in scenarios]), 'not all scenarios available. got:{g}, available:{a}'.format(g=scenarios,a=all_scenarios)    
    
    full_spec = [{'climate_model_name':'ccsm4',
                  'scenario':'rcp26',
                  'model_file_search_str': '*CCSM4'},
                 {'climate_model_name':'ccsm4',
                  'scenario':'rcp45',
                  'model_file_search_str': '*CCSM4'},
                 {'climate_model_name':'ccsm4',
                  'scenario':'rcp60',
                  'model_file_search_str': '*CCSM4'},
                 {'climate_model_name':'ccsm4',
                  'scenario':'rcp85',
                  'model_file_search_str': '*CCSM4'},
               
                 {'climate_model_name':'csiro',
                  'scenario':'rcp26',
                  'model_file_search_str': '*CSIRO-Mk3-6-0'},
                 {'climate_model_name':'csiro',
                  'scenario':'rcp45',
                  'model_file_search_str': '*CSIRO-Mk3-6-0'},
                 {'climate_model_name':'csiro',
                  'scenario':'rcp60',
                  'model_file_search_str': '*CSIRO-Mk3-6-0'},
                 {'climate_model_name':'csiro',
                  'scenario':'rcp85',
                  'model_file_search_str': '*CSIRO-Mk3-6-0'},
               
                 {'climate_model_name':'gfdl',
                  'scenario':'rcp26',
                  'model_file_search_str': '*GFDL-ESM2G'},
                 {'climate_model_name':'gfdl',
                  'scenario':'rcp45',
                  'model_file_search_str': '*GFDL-ESM2G'},
                 {'climate_model_name':'gfdl',
                  'scenario':'rcp60',
                  'model_file_search_str': '*GFDL-ESM2G'},
                 {'climate_model_name':'gfdl',
                  'scenario':'rcp85',
                  'model_file_search_str': '*GFDL-ESM2G'}
                 ]
    
    to_return = []
    for m in full_spec:
        if m['climate_model_name'] in models and m['scenario'] in scenarios:
            to_return.append(m)
    
    return to_return

def get_cmip5_files(model_spec, base_folder, get_historic=True):
    """
    The cmip5 files are usually spread across numers netCDF files with different time
    ranges (usually 10 year chunks) and variables (precip, tmin, tmax).
    This gits a list of all of them to pass to xarray.open_mfrdataset().
    Historic is the pre-2006 data which is not tied to any scenario.
    
    Used in combination with load_cmip5_spec()
    """
    forecast_search = model_spec['model_file_search_str'] + '_' + model_spec['scenario'] + '*.nc4'
    historic_search = model_spec['model_file_search_str'] + '_historic*.nc4'
    
    model_files = glob.glob(climate_data_folder + historic_search)
    assert len(model_files) > 5, 'no model files found for {m} - {s}'.format(m = model_spec['climate_model_name'] , s = model_spec['scenario'])
    
    if get_historic:
        historic_files = glob.glob(climate_data_folder + historic_search)
        assert len(historic_files) > 3, 'no historic model files found for {m}'.format(m = model_spec['climate_model_name'])
        model_files.extend(historic_files)        

    return model_files
        

def verify_cmip5_parts(xr_obj, 
                       expected_vars = ['pr','tasmin','tasmax'],
                       expected_start_time = '1980-01-01',
                       expected_end_time   = '2100-12-31'):
    pass

    