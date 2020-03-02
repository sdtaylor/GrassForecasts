from urllib.request import urlopen
import json
# with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
#     counties = json.load(response)

import pandas as pd
import numpy as np

# df = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/fips-unemp-16.csv",
#                    dtype={"fips": str})

with open('usa_latlon_grid.geojson','r') as f:
    us_grid = json.load(f)

n_features = len(us_grid['features'])

[f.update(id=i) for i,f in enumerate(us_grid['features'])]


latlon_df = pd.read_csv('data/downscaled_test_data.csv')
#latlon_df['year'] = pd.DatetimeIndex(latlon_df.time).year
latlon_df = latlon_df.groupby(['latitude','longitude']).tasmax.mean().reset_index()
latlon_df['id'] = latlon_df.index

#import plotly.express as px
import plotly.graph_objects as go


fig = go.Figure(data = go.Choropleth(
    geojson=us_grid,
    z=latlon_df['tasmax'],
    locations=latlon_df['id'],
    featureidkey='id'
    #locationmode='geojson-id'
    ))

# fig = px.choropleth(data_frame = latlon_df, geojson=us_grid, locations='id', color='tasmax',
#                     locationmode='geojson-id',
#                     featureidkey='id',
#                      color_continuous_scale="Viridis",
#                      #range_color=(0, 12),
#                      scope="usa")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0},
                  geo_scope='usa')
fig.show()
