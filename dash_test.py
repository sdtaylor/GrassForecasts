import os

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots

from textwrap import dedent as d

import numpy as np
import geopandas as gp
import json

from tools.etc import build_geojson_grid

#################################################                                    
#################################################
# Loading data for the map and figures
#################################################                                    
#################################################

climatology_years = range(1990,2011)
display_years = range(1990,2100)
year_resolution = 10 # final figure will display the average of this many years.

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

phenograss_data['fCover_annomoly'] = (phenograss_data.fCover / phenograss_data.fCover_climatology) - 1
phenograss_data['tmean_annomoly']  = (phenograss_data.tmean  / phenograss_data.tmean_climatology)  - 1
phenograss_data['pr_anomaly']      = (phenograss_data.pr     / phenograss_data.pr_climatology)     - 1



# The 5 year moving window average
#running_avg = phenograss_data.groupby(['latitude','longitude','model','scenario']).rolling(window=5,min_periods=5,on='year').fCover_annomoly.mean().reset_index()
#phenograss_data = phenograss_data.drop(columns='fCover_annomoly').merge(running_avg, how='left', on=['latitude','longitude','model','scenario','year'])

# One data point per year/scenario, different models are averaged together for a mean/sd
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


# Setup the USA grid. The mask contains the bounderies of the full grid, though
# not every area will have data.
mask = pd.read_csv('data/ecoregion_mask.csv')

pixel_ids = mask[['latitude','longitude']].drop_duplicates().reset_index().drop(columns=['index'])
pixel_ids['pixel_id'] = pixel_ids.index

us_grid = build_geojson_grid(mask, polygon_resolution=0.499)
us_grid = json.loads(us_grid.to_json())
n_features = len(us_grid['features'])
[f.update(id=i) for i,f in enumerate(us_grid['features'])]
# TODO: make feature numbers based on the pixel_id column in phenograss_data

# Assign the pixel id's back to data
phenograss_data = pd.merge(phenograss_data ,pixel_ids, on=['latitude','longitude'], how='left')
phenograss_plot_data = pd.merge(phenograss_plot_data ,pixel_ids, on=['latitude','longitude'], how='left')

# Mean climatology for each pixel, which varies slightly among models/scenarios
map_data = phenograss_data[['pixel_id','fCover_climatology']].groupby(['pixel_id']).fCover_climatology.mean().reset_index()


#################################################                                    
#################################################
# Define the different components
#################################################                                    
#################################################

selectable_years = phenograss_data.year.drop_duplicates()
selectable_scenarios = phenograss_data.scenario.drop_duplicates().to_list()
climate_models = phenograss_data.model.unique()


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

page_title_text = html.Div([html.H1("Grassland Productivity Long Term Forecast")],
                                style={'textAlign': "center", "padding-bottom": "30"})

description_container = html.Div([
                           dcc.Markdown(d('''
                            Selected within the shaded areas of the map to view long-term forecasts for that location.                           
                           ''')),
                           html.Pre(id='page-description')
                           ]
                           )

response_radio_container = html.Div(id='response-radio-container',
                                    children = [
                                        html.P(
                                            id='response-radio-text',
                                            children = 'Select a year'
                                            ),
                                        dcc.RadioItems(
                                            id = 'year-select',
                                            options = [2010,2020,2030,2040],
                                            value  = 2010
                                            )
                                        ])

map_container = html.Div(id='map-container',
                           children = [
                               html.P(id='map-title',
                                      children=''),
                               dcc.Graph(id='map')
                               ]
                           )

timeseries_container = html.Div(id='timeseries-container',
                           children = [
                               html.P(id='timeseries-title',
                                      children=''),
                               dcc.Graph(id='timeseries')
                               ]
                           )

markdown_container = html.Div([
                           dcc.Markdown(d('''
                             Map click output             
                           
                           ''')),
                           html.Pre(id='click-output')
                           ]
                           )

#################################################                                    
#################################################
# Define layout of all components
#################################################                                    
#################################################
                           
app.layout = html.Div(id='page-container',
                      children=[
                          html.Div(id='header-text',
                                   children = [
                                       page_title_text,
                                       description_container
                                       ],style={'columnCount':1}),
                          
                          html.Div(id='figure-container',
                                   children = [
                                       map_container,
                                       timeseries_container
                                       ],style={'columnCount':2}),
                          
                          markdown_container,
                          response_radio_container
                          ], style={'align-items':'center',
                                    'justify-content':'center'})
                                 
#################################################                                    
#################################################
# Interactions / callbacks
#################################################                                    
#################################################                        
                              
@app.callback(
    dash.dependencies.Output('click-output', 'children'),
    [dash.dependencies.Input('map', 'clickData')])
def display_click_data(clickData):
    return json.dumps(clickData, indent=2)


@app.callback(
    dash.dependencies.Output('timeseries', 'figure'),
    [dash.dependencies.Input('map', 'clickData')])
def update_timeseries(clickData):
    print(clickData)
    try:
        selected_pixel = clickData['points'][0]['location']
    except:
        selected_pixel = 4681
    print(selected_pixel)
      
    pixel_data = phenograss_plot_data[(phenograss_plot_data.pixel_id==selected_pixel)]
    rcp26 = pixel_data.scenario=='rcp26'
    rcp45 = pixel_data.scenario=='rcp45'
    
    traces_to_add = [{'name':'Change in Productivity',
                      'color':'green',
                      'mean_var':'fCover_annomoly_mean',
                      'std_var':'fCover_annomoly_std',
                      'offset':-0.2},
                     {'name':'Change in Temperature',
                      'color':'red',
                      'mean_var':'tmean_annomoly_mean',
                      'std_var':'tmean_annomoly_std',
                      'offset':0.1},
                     {'name':'Change in Rain',
                      'color':'blue',
                      'mean_var':'pr_anomaly_mean',
                      'std_var':'pr_anomaly_std',
                      'offset':0.3}]
    
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=['Best Case Scenario (RCP26)','Moderate Case Scenario (RCP45)'])
    
    # Add the 3 different markers in the top figure for rcp26
    for t in traces_to_add:
        fig.append_trace(go.Scatter(x=pixel_data.year[rcp26] + t['offset'], y=pixel_data[t['mean_var']][rcp26],
                                    error_y = dict(type='data',array=pixel_data[t['std_var']][rcp26], width=0),
                                    mode='markers', marker=dict(color=t['color']),
                                    name=t['name']),
                         row=1,col=1)    
        
        # The rcp45 figure doesn't get any name text
        fig.append_trace(go.Scatter(x=pixel_data.year[rcp26] + t['offset'], y=pixel_data[t['mean_var']][rcp45],
                                    error_y = dict(type='data',array=pixel_data[t['std_var']][rcp45], width=0),
                                    mode='markers', marker=dict(color=t['color']),
                                    name=''),
                         row=2,col=1) 
    
    fig.update_layout(title = '', height=500, width=800)

    return fig

@app.callback(
    dash.dependencies.Output("map", "figure"),
    [dash.dependencies.Input('year-select','value')]
)
def update_map(value):
    dff = map_data
    
    trace = go.Choroplethmapbox(
                    geojson=us_grid,
                    z = np.repeat(1,len(dff)), # Make all fill values the same so it displays a single color
                    showscale=False,
                    marker = dict(line_color='red', line_width=0.2),
                    colorscale="Reds",
                    locations = dff['pixel_id'],
                    featureidkey='id',
                    hoverinfo='location',
                    selectedpoints = [842], # The index of pixel_id 4681
                    selected = dict(marker_opacity=1.0),
                    unselected = dict(marker_opacity=0.2),
                    )
    
    return {"data": [trace],
             "layout": go.Layout(title=None,
                                 height=500,width=700,
                                 mapbox_style='stamen-terrain',
                                 mapbox_zoom=3, mapbox_center = {"lat": 40, "lon": -100})}


#################################################                                    
#################################################
if __name__ == '__main__':
    app.run_server(debug=True)
    
