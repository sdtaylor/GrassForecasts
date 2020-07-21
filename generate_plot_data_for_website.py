import pandas as pd

import site_config


"""
Generate the final data used in the timeseries plots on the site. These are 
derived from the data/climate_annual_data.csv and data/phenograss_downscaled_annual_integral.csv
files. This step is memory intensive so the final file, data/phenograss_timeseries_plot_data.csv,
is made here and loaded by dash_test.py on the server.
"""

climatology_years = site_config.climatology_years
display_years = site_config.display_years
year_resolution = site_config.year_resolution # final figure will display the average of this many years.

debug=site_config.debug

################################
climate_data = pd.read_csv('data/climate_annual_data.csv')
phenograss_data = pd.read_csv('data/phenograss_downscaled_annual_integral.csv')
phenograss_data = pd.merge(phenograss_data, climate_data, how='right', on=['latitude', 'longitude', 'model', 'scenario', 'year'])

# TODO: quick check that all timeseries are intact, and all models/secnarios avaialble

climatology = phenograss_data[phenograss_data.year.isin(climatology_years)]
climatology = climatology.groupby(['latitude','longitude','model','scenario']).agg({'fCover':'mean','tmean':'mean','pr':'mean'}).reset_index()
                                                                                      
climatology.rename(columns={'fCover':'fCover_climatology','tmean':'tmean_climatology','pr':'pr_climatology'}, inplace=True)

# subset to desired years and aggregate to larger temporal resolution
phenograss_data = phenograss_data[phenograss_data.year.isin(display_years)]
phenograss_data['year'] = phenograss_data.year - (phenograss_data.year % year_resolution)
phenograss_data = phenograss_data.groupby(['latitude','longitude','model','scenario','year']).agg({'fCover':'mean','tmean':'mean','pr':'mean'}).reset_index()

phenograss_data = pd.merge(phenograss_data, climatology, on=['latitude','longitude','model','scenario'], how='left')

phenograss_data['fCover_annomoly'] = (phenograss_data.fCover - phenograss_data.fCover_climatology) / phenograss_data.fCover_climatology
phenograss_data['tmean_annomoly']  = (phenograss_data.tmean  - phenograss_data.tmean_climatology)
phenograss_data['pr_anomaly']      = (phenograss_data.pr     - phenograss_data.pr_climatology)     / phenograss_data.pr_climatology

# Aggregate everything per decade.
# This should not be so convoluted but omg doing groupby stuff in pandas is a total chore

annual_mean = phenograss_data.groupby(['latitude','longitude','year','scenario']).agg({'fCover_annomoly':'mean',
                                                                                       'tmean_annomoly':'mean',
                                                                                       'pr_anomaly':'mean'}).reset_index()
annual_mean.rename(columns={'fCover_annomoly':'fCover_annomoly_mean','tmean_annomoly':'tmean_annomoly_mean','pr_anomaly':'pr_anomaly_mean'}, inplace=True)

annual_std = phenograss_data.groupby(['latitude','longitude','year','scenario']).agg({'fCover_annomoly':'std',
                                                                                       'tmean_annomoly':'std',
                                                                                       'pr_anomaly':'std'}).reset_index()
annual_std.rename(columns={'fCover_annomoly':'fCover_annomoly_std','tmean_annomoly':'tmean_annomoly_std','pr_anomaly':'pr_anomaly_std'}, inplace=True)

phenograss_plot_data = pd.merge(annual_mean, annual_std, on=['latitude','longitude','year','scenario'] , how='left')

phenograss_plot_data.to_csv('data/phenograss_timeseries_plot_data.csv')