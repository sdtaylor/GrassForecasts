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

with open('usa_latlon_grid.geojson','r') as f:
    us_grid = json.load(f)

n_features = len(us_grid['features'])
[f.update(id=i) for i,f in enumerate(us_grid['features'])]

latlon_df = pd.read_csv('data/NE_CO_phenograss.csv')
pixel_ids = latlon_df[['latitude','longitude']].drop_duplicates().reset_index()
pixel_ids['pixel_id'] = pixel_ids.index


#latlon_df['year'] = pd.DatetimeIndex(latlon_df.time).year
#latlon_df = latlon_df.groupby(['year','latitude','longitude']).tasmax.mean().reset_index()
latlon_df = pd.merge(latlon_df ,pixel_ids, on=['latitude','longitude'], how='left')

selectable_years = latlon_df.decade.drop_duplicates()
selectable_scenarios = latlon_df.scenario.drop_duplicates().to_list()

map_color = latlon_df.groupby(['decade','pixel_id']).phenograss_prediction.mean().reset_index()




external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([html.Div([html.H1("Grassland Productivity")],
                                style={'textAlign': "center", "padding-bottom": "30"}),
                       
                       html.Div(id='slider-container',
                                children = [
                                    html.P(
                                        id='slider-text',
                                        children='Drag the slider the the descired decade',
                                        ),
                                    dcc.Slider(
                                        id='decade-slider',
                                        min=min(selectable_years),
                                        max=max(selectable_years),
                                        value=min(selectable_years),
                                        marks={
                                            year: {'label':str(year)}
                                             for year in selectable_years}
                                        )]),
                       
                       html.Div(
                           id='map-container',
                           children = [
                               html.P(id='map-title',
                                      children=''),
                               dcc.Graph(id='map')
                               ]
                           ),
                       
                       html.Div(
                           id='timeseries-container',
                           children = [
                               html.P(id='timeseries-title',
                                      children=''),
                               dcc.Graph(id='timeseries')
                               ]
                           ),
                       
                       html.Div([
                           dcc.Markdown(d('''
                             Map click output             
                           
                           ''')),
                           html.Pre(id='click-output')
                           ]
                           )
                       ], style={'align-items':'center',
                                 'justify-content':'center',
                                 'width':'50%'})


@app.callback(
    dash.dependencies.Output('click-output', 'children'),
    [dash.dependencies.Input('map', 'clickData')])
def display_click_data(clickData):
    return json.dumps(clickData, indent=2)


@app.callback(
    dash.dependencies.Output('timeseries', 'figure'),
    [dash.dependencies.Input('map', 'clickData'),
     dash.dependencies.Input("decade-slider", "value")])
def update_timeseries(clickData, decade):
    print(clickData)
    selected_pixel = clickData['points'][0]['location']
    print(selected_pixel)
    pixel_data = latlon_df[(latlon_df.pixel_id==selected_pixel) & (latlon_df.decade==decade)]
    
    traces = []
    for scenario in selectable_scenarios:
        scenario_data = pixel_data[pixel_data.scenario==scenario]
        traces.append(go.Scatter(x=scenario_data.doy, y=scenario_data.phenograss_prediction,
                                 mode='lines+markers',
                                 name=scenario))
    
    
    #trace = go.Scatter(x=pixel_data.year, y=pixel_data.tasmax)
    return {'data': traces,
             "layout": go.Layout(title='Yearly fCover',height=500,width=900)}

@app.callback(
    dash.dependencies.Output("map", "figure"),
    [dash.dependencies.Input("decade-slider", "value")]
)
def update_figure(decade):
    dff = map_color[map_color.decade==decade]
    

    # trace = go.Choropleth(geojson=us_grid, locations=dff['id'], z=dff['tasmax'],
    #                       locationmode='geojson-id',
    #                       featureidkey='id',
    #                       autocolorscale=False, 
    #                       colorscale="YlGnBu",marker={'line': {'color': 'rgb(180,180,180)','width': 0.5}},
    #                       colorbar={"thickness": 10,"len": 0.3,"x": 0.9,"y": 0.7,
    #                                 'title': {"text": 'temp', "side": "bottom"}})
    trace = go.Choropleth(
                    geojson=us_grid,
                    z = dff['phenograss_prediction'],
                    locations = dff['pixel_id'],
                    featureidkey='id',
                    locationmode='geojson-id'
                    )
    
    #return {'data':[trace]}
    return {"data": [trace],
             "layout": go.Layout(title='layout title',height=500,width=700,geo_scope='usa',geo={'showframe': False,'showcoastlines': False})}

if __name__ == '__main__':
    app.run_server(debug=True)
    
