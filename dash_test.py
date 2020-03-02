import os

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go

import numpy as np
import geopandas as gp
import json

with open('usa_latlon_grid.geojson','r') as f:
    us_grid = json.load(f)

n_features = len(us_grid['features'])
[f.update(id=i) for i,f in enumerate(us_grid['features'])]

latlon_df = pd.read_csv('data/downscaled_test_data.csv')
pixel_ids = latlon_df[['latitude','longitude']].drop_duplicates().reset_index()
pixel_ids['pixel_id'] = pixel_ids.index


latlon_df['year'] = pd.DatetimeIndex(latlon_df.time).year
latlon_df = latlon_df.groupby(['year','latitude','longitude']).tasmax.mean().reset_index()
latlon_df = pd.merge(latlon_df ,pixel_ids, on=['latitude','longitude'], how='left')

selectable_years = latlon_df.year.drop_duplicates()


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
                           )
                       ])


@app.callback(
    dash.dependencies.Output("map", "figure"),
    [dash.dependencies.Input("decade-slider", "value")]
)
def update_figure(decade):
    dff = latlon_df[latlon_df.year==decade]

    # trace = go.Choropleth(geojson=us_grid, locations=dff['id'], z=dff['tasmax'],
    #                       locationmode='geojson-id',
    #                       featureidkey='id',
    #                       autocolorscale=False, 
    #                       colorscale="YlGnBu",marker={'line': {'color': 'rgb(180,180,180)','width': 0.5}},
    #                       colorbar={"thickness": 10,"len": 0.3,"x": 0.9,"y": 0.7,
    #                                 'title': {"text": 'temp', "side": "bottom"}})
    trace = go.Choropleth(
                    geojson=us_grid,
                    z = dff['tasmax'],
                    locations = dff['pixel_id'],
                    featureidkey='id',
                    locationmode='geojson-id'
                    )
    
    #return {'data':[trace]}
    return {"data": [trace],
             "layout": go.Layout(title='layout title',height=800,geo_scope='usa',geo={'showframe': False,'showcoastlines': False})}

if __name__ == '__main__':
    app.run_server(debug=True)
    
