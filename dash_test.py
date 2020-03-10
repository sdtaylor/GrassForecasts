import os

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go

from textwrap import dedent as d

import numpy as np
import geopandas as gp
import json

#################################################                                    
#################################################
# Loading data for the map and figures
#################################################                                    
#################################################
with open('usa_latlon_grid.geojson','r') as f:
    us_grid = json.load(f)

n_features = len(us_grid['features'])
[f.update(id=i) for i,f in enumerate(us_grid['features'])]

reference_years = list(range(2010,2020))
min_year = 2010

phenograss_data = pd.read_csv('data/NE_CO_phenograss.csv')
phenograss_data = phenograss_data[phenograss_data.year >= min_year]

pixel_ids = phenograss_data[['latitude','longitude']].drop_duplicates().reset_index()
pixel_ids['pixel_id'] = pixel_ids.index
phenograss_data = pd.merge(phenograss_data ,pixel_ids, on=['latitude','longitude'], how='left')

# TODO: adjust to anomaly via reference years

reference_values = phenograss_data[phenograss_data.year.isin(reference_years)]
map_data = phenograss_data.groupby(['pixel_id']).annual_productivity.mean().reset_index()

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

year_slider_labels = list(range(selectable_years.min(), selectable_years.max(), 5))
year_slider_container = html.Div(id='year-slider-container',
                                children = [
                                    html.P(
                                        id='year-slider-text',
                                        children='Drag the slider the the descired decade',
                                        ),
                                    dcc.Slider(
                                        id='year-slider',
                                        min=min(selectable_years),
                                        max=max(selectable_years),
                                        value=min(selectable_years),
                                        marks={
                                            year: {'label':str(year)}
                                             for year in year_slider_labels}
                                        )], style={'width':'50%'})

scenario_radio_container = html.Div(id='scenario-radio-container',
                                    children = [
                                        html.P(
                                            id='scenario-radio-text',
                                            children = 'Select a scenario'
                                            ),
                                        dcc.RadioItems(
                                            id='scenario-select',
                                            options = [{'label':s,'value':s} for s in selectable_scenarios],
                                            value  = selectable_scenarios[0]
                                            )
                                        ])

response_radio_container = html.Div(id='response-radio-container',
                                    children = [
                                        html.P(
                                            id='response-radio-text',
                                            children = 'Select a variable to forecast'
                                            ),
                                        dcc.RadioItems(
                                            options = [{'label':'Change in Annual Productivity','value':'change_in_productivity'}],
                                            value  = 'change_in_productivity'
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
                          
                          # combo of column counts to make the scenario on
                          # the left, with reponse/year stacked on right
                          html.Div(id='selection-container',
                                   children = [
                                       scenario_radio_container,
                                       html.Div([response_radio_container,
                                                 year_slider_container],
                                                style={'columnCount':1})                                       
                                       ], style={'columnCount':2}),
                          
                          html.Div(id='figure-container',
                                   children = [
                                       map_container,
                                       timeseries_container
                                       ],style={'columnCount':2}),
                          
                          markdown_container
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
    [dash.dependencies.Input('map', 'clickData'),
     dash.dependencies.Input('scenario-select', 'value'),
     dash.dependencies.Input("year-slider", "value")])
def update_timeseries(clickData, scenario, year):
    print(clickData)
    selected_pixel = clickData['points'][0]['location']
    print(selected_pixel)
    pixel_data = phenograss_data[(phenograss_data.pixel_id==selected_pixel) & (phenograss_data.scenario==scenario) & (phenograss_data.year<=year)]
    
    traces = []
    for model in climate_models:
        model_data = pixel_data[pixel_data.model==model]
        traces.append(go.Scatter(x=model_data.year, y=model_data.annual_productivity,
                                 mode='lines+markers',
                                 name=model))
    
    
    #trace = go.Scatter(x=pixel_data.year, y=pixel_data.tasmax)
    return {'data': traces,
             "layout": go.Layout(title='Yearly fCover',height=500,width=900)}

@app.callback(
    dash.dependencies.Output("map", "figure"),
    [dash.dependencies.Input("year-slider", "value")]
)
def update_map(year):
    dff = map_data
    
    trace = go.Choropleth(
                    geojson=us_grid,
                    z = dff['annual_productivity'],
                    locations = dff['pixel_id'],
                    featureidkey='id',
                    locationmode='geojson-id'
                    )
    
    #return {'data':[trace]}
    return {"data": [trace],
             "layout": go.Layout(title='layout title',height=500,width=700,geo_scope='usa',geo={'showframe': False,'showcoastlines': False})}


#################################################                                    
#################################################
if __name__ == '__main__':
    app.run_server(debug=True)
    
