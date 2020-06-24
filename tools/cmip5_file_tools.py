import glob

import numpy as np

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
    
    # The rcpXX files
    forecast_search = model_spec['model_file_search_str'] + '_' + model_spec['scenario'] + '*.nc4'
    historic_search = model_spec['model_file_search_str'] + '_historic*.nc4'
    
    model_files = glob.glob(base_folder + forecast_search)
    assert len(model_files) > 5, 'no model files found for {m} - {s}'.format(m = model_spec['climate_model_name'] , s = model_spec['scenario'])
    
    # The historic files
    if get_historic:
        historic_files = glob.glob(base_folder + historic_search)
        assert len(historic_files) > 3, 'no historic model files found for {m}'.format(m = model_spec['climate_model_name'])
        model_files.extend(historic_files)        

    return model_files
        

def verify_cmip5_parts(xr_obj, 
                       expected_vars = ['pr','tasmin','tasmax','tmean'],
                       expected_start_date = '1980-01-01',
                       expected_end_date   = '2100-12-31'):
    """ 
    The intention here is to account for the several hundred netcdf files. They 
    are organized by time period, thus as long as each variable has a fully intact
    timeseries at this one location then everything should be intact. 
    """
    
    expected_start_date = np.datetime64(expected_start_date)
    expected_end_date   = np.datetime64(expected_end_date)
    expected_length = (expected_end_date - expected_start_date).astype(int) + 1
    
    print('checking model {m} - {s}'.format(m=xr_obj.model.values, s=xr_obj.scenario.values))
    
    start_date = xr_obj.time.values.min().astype('datetime64[D]')
    end_date   = xr_obj.time.values.max().astype('datetime64[D]')
    length     = len(xr_obj.time)
    
    start_date_off = start_date != expected_start_date
    end_date_off   = end_date   != expected_end_date
    length_off     = length != expected_length
    
    if start_date_off or end_date_off:
        UserWarning('Dates are off, got {s1} - {s2}, expected {e1} - {e2}'.format(s1=start_date,
                                                                                  s2=end_date,
                                                                                  e1=expected_start_date,
                                                                                  e2=expected_end_date))
    if length_off:
        UserWarning('Number of days off, got {n1}, expected {n2} days'.format(n1=length,
                                                                              n2=expected_length))
    
    for var in expected_vars:
        timeseries = xr_obj[var].isel(latitude=150, longitude=200).values
        missing_entries = np.where(np.isnan(timeseries))[0]
        if len(missing_entries)>0:
            missing_dates = xr_obj.time[missing_entries].values
            UserWarning('missing {n} entries in {v}, from {d1} to {d2}'.format(v=var, 
                                                                               n=len(missing_dates),
                                                                               d1=missing_dates.min(),
                                                                               d2=missing_dates.max()))
