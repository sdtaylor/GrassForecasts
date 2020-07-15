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

# clear this out to save memory
climate_data = None

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
map_data = phenograss_data[['latitude','longitude','pixel_id','fCover_climatology']].groupby(['latitude','longitude','pixel_id']).fCover_climatology.mean().reset_index()

def map_hover_text(row):
    return '{lat} Latitude\n{lon} Longitude'.format(lat=row.latitude, lon=row.longitude)

map_data['hover_text'] = map_data.apply(map_hover_text, axis=1)

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

# server object used for wsgi integration
server = app.server

# Title text and lower description
page_title_text = html.Div([html.H1("Grassland Productivity Long Term Forecast")],
                                style={'textAlign': "center", "padding-bottom": "30"})

description_container = html.Div([
                           dcc.Markdown(d('''
                            Selected within the shaded areas of the map to view long-term forecasts for that location.                           
                           ''')),
                           html.Pre(id='page-description')
                           ]
                           )


######################
# The map
map_trace = go.Choroplethmapbox(
                    geojson=us_grid,
                    z = np.repeat(1,len(map_data)), # Make all fill values the same so it displays a single color
                    showscale=False,
                    marker = dict(opacity=0.2,line_color='red', line_width=0.2),
                    colorscale="Reds",
                    locations = map_data['pixel_id'],
                    featureidkey='id',
                    hoverinfo='text',
                    hovertext = map_data['hover_text'],
                    #selectedpoints = [842], # The index of pixel_id 4681     # docs sort of imply these control the "on/off"
                    #selected = dict(marker_opacity=1.0),                     # of the selected location, but that doesn't seem
                    #unselected = dict(marker_opacity=0.2),                   # to be the case. Likely need to implement in a callback
                    )
map_layout = go.Layout(title=None,
                       height=900,width=500,
                       margin=dict(l=20, r=20, t=20, b=20),
                       mapbox_style='stamen-terrain',
                       mapbox_zoom=3, mapbox_center = {"lat": 40, "lon": -100})

map_container = html.Div(id='map-container',
                           children = [
                                   html.P(id='map-title',
                                          children=''),
                                   dcc.Graph(id='map',
                                             figure = {
                                                 'data': [map_trace],
                                                 'layout': map_layout
                                                 })
                                      ])

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
                          ], style={'align-items':'left',
                                    'justify-content':'left'})

def generate_hover_str(variable, timeperiod, percent_change):
    if timeperiod in ["1990's","2000's","2010's",]:
        # dont make claims about the past
        return ''

    s = '{v} is expected to {m} {p}% by the {t} in this scenario'
    
    if not np.isnan(percent_change):
        change_verb = 'increase' if percent_change>0 else 'decrease'
    else:
        return ''
    
    s = s.format(v = variable,
                 m = change_verb,
                 p = int(percent_change*100),
                 t = timeperiod)
    
    return s
    
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


# Setup the timeseries axis
x_axis_values = np.unique(np.array(display_years) - (np.array(display_years) % year_resolution))
x_axis_labels = ["{y}'s".format(y=y) for y in x_axis_values]
y_axis_range  = [-0.3,0.3]
y_axis_values = [-0.3,-0.2, -0.1, 0, 0.1, 0.2, 0.3]
y_axis_labels = ['-30%','-20%', '-10%', 'No Change', '+10%', '+20%', '+30%']



@app.callback(
    dash.dependencies.Output('timeseries', 'figure'),
    [dash.dependencies.Input('map', 'clickData')])
def update_timeseries(clickData):
    print(clickData)
    try:
        selected_pixel = clickData['points'][0]['location']
    except:
        selected_pixel = 3664
    print(selected_pixel)
      
    pixel_data = phenograss_plot_data[(phenograss_plot_data.pixel_id==selected_pixel)]
    
    rcp26 = pixel_data.scenario=='rcp26'
    rcp45 = pixel_data.scenario=='rcp45'
    scenarios = ['rcp26','rcp45']
    scenario_titles = ['Best Case Scenario (RCP26)','Moderate Case Scenario (RCP45)']
    
    traces_to_add = [{'variable_desc':'Change in Grassland Productivity',
                      'variable': 'Grassland productivity',
                      'color':'#009E73',
                      'mean_var':'fCover_annomoly_mean',
                      'std_var':'fCover_annomoly_std',
                      'offset':-1},
                     {'variable_desc':'Change in Average Yearly Temperature',
                      'variable': 'Average yearly temperature',
                      'color':'#d5000d',
                      'mean_var':'tmean_annomoly_mean',
                      'std_var':'tmean_annomoly_std',
                      'offset':0},
                     {'variable_desc':'Change in Average Yearly Rain',
                      'variable': 'Average yearly rain',
                      'color':'#0072B2',
                      'mean_var':'pr_anomaly_mean',
                      'std_var':'pr_anomaly_std',
                      'offset':1}]
    
    fig = make_subplots(rows=len(scenarios), cols=1, shared_xaxes=True,
                        subplot_titles=scenario_titles)
    
    # Add the 3 different markers, 1 for each variable, for each of the scenarios
    for t in traces_to_add:
        for scenario_i, s in enumerate(scenarios):
            scenario_index = pixel_data.scenario==s
            
            # Tie togther the y + x info to generate a unique string
            hover_attributes = zip(x_axis_labels,pixel_data[t['mean_var']][scenario_index])
            hover_text = [generate_hover_str(t['variable'], *attr) for attr in hover_attributes]
            
            fig.append_trace(go.Scatter(x=pixel_data.year[scenario_index] + t['offset'], y=pixel_data[t['mean_var']][scenario_index],
                                        error_y = dict(type='data',array=pixel_data[t['std_var']][scenario_index], width=0, thickness=3),
                                        mode='markers', marker=dict(color=t['color'], size=10),
                                        hovertext = hover_text, hoverinfo = "text",
                                        name=t['variable_desc'] if s=='rcp26' else '',  # only the top gets legend entries
                                        showlegend=True if s=='rcp26' else False),
                             row=scenario_i+1,col=1)
            
    
    # Horizontal lines 
    hline = dict(type='line', 
                x0=x_axis_values.min()-10,x1=x_axis_values.max()+10,
                y0=0,y1=0, 
                line=dict(color='black',width=2))
    for scenario in range(len(scenarios)):
        fig.add_shape(hline, row=scenario+1,col=1)
    
    # Specifying axis labels
    fig.update_xaxes(tickmode='array', tickangle=-45,
                     tickvals = x_axis_values, ticktext = x_axis_labels,
                     gridcolor='grey')
    fig.update_yaxes(tickmode='array', range=y_axis_range,
                     tickvals = y_axis_values, ticktext = y_axis_labels,
                     gridcolor='grey')
    
    fig.update_layout(legend=dict(x=0, y=-0.5))
    fig.update_layout(margin=dict(l=50, r=50, t=50, b=50))
    fig.update_layout(title = '', height=600, plot_bgcolor='white')

    return fig

#################################################                                    
#################################################
if __name__ == '__main__':
    app.run_server(debug=True)
    
