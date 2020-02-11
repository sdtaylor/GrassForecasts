from tools.cmip_download_tool import CMIP_FTP_TOOL




models    = ['ccsm4','csiro-mk3-6-0']
scenarios = ['rcp26']
variables = ['pr','tasmax','tasmin']
decades   = ['2006','2016']
max_runs  = 1

cmip = CMIP_FTP_TOOL()
for m in models:
    for s in scenarios:
        for v in variables:
            
            cmip.download_cmip5_file(scenario = s,
                                     climate_model=m,
                                     variable = v,
                                     decades = decades,
                                     n_runs=max_runs,
                                     dest_folder = './data/')