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


phenograss_data = pd.read_csv('phenograss_ann_integral.csv')
phenograss_data['year'] = phenograss_data.time

phenograss_data['fCover_annomoly'] = phenograss_data.fCover_annomoly -1
#phenograss_data = phenograss_data[phenograss_data.year >= min_year]

# The 5 year moving window average
#running_avg = phenograss_data.groupby(['latitude','longitude','model','scenario']).rolling(window=5,min_periods=5,on='year').fCover_annomoly.mean().reset_index()
#phenograss_data = phenograss_data.drop(columns='fCover_annomoly').merge(running_avg, how='left', on=['latitude','longitude','model','scenario','year'])

# One data point per year/scenario, different models are averaged togehter for a mean/sd
# This should not be so confoluted but omg doing groupby stuff in pandas is a total chore
fCover_mean = phenograss_data.groupby(['latitude','longitude','year','scenario']).fCover_annomoly.mean().reset_index().rename(columns={'fCover_annomoly':'fCover_annomoly_mean'})
fCover_std = phenograss_data.groupby(['latitude','longitude','year','scenario']).fCover_annomoly.std().reset_index().rename(columns={'fCover_annomoly':'fCover_annomoly_std'})
phenograss_plot_data = pd.merge(fCover_mean, fCover_std, on=['latitude','longitude','year','scenario'] , how='left')


pixel_ids = phenograss_data[['latitude','longitude']].drop_duplicates().reset_index().drop(columns=['index'])
pixel_ids['pixel_id'] = pixel_ids.index
phenograss_data = pd.merge(phenograss_data ,pixel_ids, on=['latitude','longitude'], how='left')
phenograss_plot_data = pd.merge(phenograss_plot_data ,pixel_ids, on=['latitude','longitude'], how='left')

us_grid = build_geojson_grid(phenograss_data, polygon_buffer=0.9)
us_grid = json.loads(us_grid.to_json())
n_features = len(us_grid['features'])
[f.update(id=i) for i,f in enumerate(us_grid['features'])]
# TODO: make feature numbers based on the pixel_id column in phenograss_data

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
                          
                          page_title_text,
                          
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
    selected_pixel = clickData['points'][0]['location']
    print(selected_pixel)
    pixel_data = phenograss_plot_data[(phenograss_plot_data.pixel_id==selected_pixel)]
    rcp26 = pixel_data.scenario=='rcp26'
    rcp45 = pixel_data.scenario=='rcp45'
    
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True)
    fig.append_trace(go.Scatter(x=pixel_data.year[rcp26], y=pixel_data.fCover_annomoly_mean[rcp26],
                             mode='lines+markers',
                             name='rcp26'),
                     row=1,col=1)    
    fig.append_trace(go.Scatter(x=pixel_data.year[rcp45], y=pixel_data.fCover_annomoly_mean[rcp45],
                             mode='lines+markers',
                             name='rcp45'),
                     row=2,col=1)  

    return fig

@app.callback(
    dash.dependencies.Output("map", "figure"),
    [dash.dependencies.Input('year-select','value')]
)
def update_map(value):
    dff = map_data
    
    trace = go.Choropleth(
                    geojson=us_grid,
                    z = dff['fCover_climatology'],
                    locations = dff['pixel_id'],
                    featureidkey='id',
                    locationmode='geojson-id'
                    )
    
    return {"data": [trace],
             "layout": go.Layout(title='layout title',height=500,width=700,geo_scope='usa',geo={'showframe': False,'showcoastlines': False})}


#################################################                                    
#################################################
if __name__ == '__main__':
    app.run_server(debug=True)
    
