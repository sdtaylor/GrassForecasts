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

import site_text
                                  
#################################################

import site_config

climatology_years = site_config.climatology_years
display_years = site_config.display_years
year_resolution = site_config.year_resolution # final figure will display the average of this many years.

debug=site_config.debug


################################

phenograss_plot_data = pd.read_csv('data/phenograss_timeseries_plot_data.csv')

# Setup the USA grid. The mask contains the bounderies of the full grid, though
# not every area will have data.
mask = pd.read_csv('data/ecoregion_mask.csv')

pixel_ids = mask[['latitude','longitude']].drop_duplicates().reset_index().drop(columns=['index'])
pixel_ids['pixel_id'] = pixel_ids.index

us_grid = build_geojson_grid(mask, polygon_resolution=0.499)
us_grid = json.loads(us_grid.to_json())
[f.update(id=i) for i,f in enumerate(us_grid['features'])]
# TODO: make feature numbers based on the pixel_id column in phenograss_data

# Assign the pixel id's back to data
phenograss_plot_data = pd.merge(phenograss_plot_data ,pixel_ids, on=['latitude','longitude'], how='left')

# Need a data.frame to fill in the dash map. the only thing it actually holds
# is the hover text values.
map_data = phenograss_plot_data[['latitude','longitude','pixel_id']].drop_duplicates()

def map_hover_text(row):
    return '{lat} Latitude\n{lon} Longitude'.format(lat=row.latitude, lon=row.longitude)

map_data['hover_text'] = map_data.apply(map_hover_text, axis=1)

#################################################                                    
#################################################
# Setup the dash app components
#################################################                                    
#################################################

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# server object used for wsgi integration
server = app.server

# Title text 
page_title_text = html.Div([html.H1("Long Term Grassland Productivity Forecast")],
                                style={'textAlign': "center", "padding-bottom": "30"})


app.title = 'Grassland Forecasts'

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

# Timeseries container, also includes the markdown container for 
# the text within the tabs.

timeseries_container = html.Div(id='timeseries-container',
                           children = [
                               dcc.Markdown(id='rcp_description'),
                               html.Div(id='timeseries')
                               ]
                           )

# This is a container displaying text from clicked values, only needed 
# during development.
if debug:
    markdown_container = html.Div([
                               dcc.Markdown(d('''
                                 Map click output             
                               
                               ''')),
                               html.Pre(id='click-output')
                               ]
                               )                 
else:
    markdown_container = html.Div()

                                          
                                         
#################################################
# Put the different components together in the layout.
#################################################                                    
                           
app.layout = html.Div(id='page-container',
                      children=[
                          html.Div(id='header-text',
                                   children = [
                                       page_title_text,
                                       ],style={'columnCount':1}),
                          
                          html.Div(id='figure-container',
                                   children = [
                                       map_container,
                                       html.Div([
                                           dcc.Tabs(id='timeseries-tabs', value='about', children=[
                                               dcc.Tab(label='About', value='about'),
                                               dcc.Tab(label='RCP 2.6', value='rcp26'),
                                               dcc.Tab(label='RCP 4.5', value='rcp45'),
                                               dcc.Tab(label='RCP 6.0', value='rcp60'),
                                               dcc.Tab(label='RCP 8.5', value='rcp85')
                                           ]),
                                           timeseries_container
                                           ])
                                       ],style={'columnCount':2}),
                          
                          markdown_container,
                          ], style={'align-items':'left',
                                    'justify-content':'left'})

    
#################################################                                    
#################################################
# Interactions / callbacks
#################################################                                    
#################################################                        

######################
# This fills in the text at the bottom with the data from the clicked map
# only with debug=True
######################
if debug:
    @app.callback(
        dash.dependencies.Output('click-output', 'children'),
        [dash.dependencies.Input('map', 'clickData')])
    def display_click_data(clickData):
        return json.dumps(clickData, indent=2)

######################
# Fills in the text above the timeseries figures, and also the about tab
######################
@app.callback(
    dash.dependencies.Output('rcp_description', 'children'),
    [dash.dependencies.Input('timeseries-tabs', 'value')])
def update_tabtext(value):
    if value == 'about':
        return d(site_text.about_tab_text)
    else:
        return d(site_text.rcp_tab_text[value])

#######################
# The primary timeseries plots
######################

# Setup the timeseries axis
x_axis_values = np.unique(np.array(display_years) - (np.array(display_years) % year_resolution))
x_axis_labels = ["{y}'s".format(y=y) for y in x_axis_values]

# Make labels like -30%, +20%, and No change for 0
y_axis_percent_values = [-0.2, -0.1, 0, 0.1, 0.2, 0.3]
y_axis_percent_labels = []
for v in y_axis_percent_values:
    if v < 0:
        y_axis_percent_labels.append(str(int(v*100))+'%')
    elif v > 0:
        y_axis_percent_labels.append('+'+str(int(v*100))+'%')
    else:
        y_axis_percent_labels.append('No Change')

# For temerature make labels like +2 C, -1 C, and No Change
y_axis_temp_values = [-1,0,1,2,3,4]
y_axis_temp_labels = []
for v in y_axis_temp_values:
    if v < 0:
        y_axis_temp_labels.append(str(v)+'° C')
    elif v > 0:
        y_axis_temp_labels.append('+'+str(v)+'° C')
    else:
        y_axis_temp_labels.append('No Change')

# Special function for the timeseries hover text
#TODO: need different wording for temperature
def generate_hover_str(variable, timeperiod, percent_change):
    if timeperiod in ["1990's","2000's","2010's",]:
        # dont make claims about the past
        return ''

    s = '{v} is expected to <br><b>{m}</b> {p}% by the {t} in this scenario'
    
    if not np.isnan(percent_change):
        change_verb = 'increase' if percent_change>0 else 'decrease'
    else:
        return ''
    
    s = s.format(v = variable,
                 m = change_verb,
                 p = int(percent_change*100),
                 t = timeperiod)
    
    return s

# Primary callback which queries the location and scenario-tab, parses the
# needed data, creates hover text, and builds the timeseries figures.
@app.callback(
    dash.dependencies.Output('timeseries', 'children'),
    [dash.dependencies.Input('map', 'clickData'),
     dash.dependencies.Input('timeseries-tabs', 'value')])
def update_timeseries(clickData, value):
    if value == 'about':
        # For the about tab return a blank list here so the 'timeseries' div
        # becomes empty
        return []
    
    print(clickData)
    try:
        selected_pixel = clickData['points'][0]['location']
    except:
        selected_pixel = 3664
    print(selected_pixel)
    
    print('selected_tab: '+str(value))
    selected_scenario = value
    
    pixel_data = phenograss_plot_data[(phenograss_plot_data.pixel_id==selected_pixel) & (phenograss_plot_data.scenario==selected_scenario)]
    
    
    variable_info = [{'variable_desc':'Change in Grassland Productivity',
                      'variable': 'Grassland productivity',
                      'color':'#009E73',
                      'mean_var':'fCover_annomoly_mean',
                      'std_var':'fCover_annomoly_std',
                      'y_labels':'percent',
                      'offset':0},
                     {'variable_desc':'Change in Average Yearly Temperature',
                      'variable': 'Average yearly temperature',
                      'color':'#d5000d',
                      'mean_var':'tmean_annomoly_mean',
                      'std_var':'tmean_annomoly_std',
                      'y_labels':'temp',
                      'offset':0},
                     {'variable_desc':'Change in Average Yearly Rain',
                      'variable': 'Average yearly rain',
                      'color':'#0072B2',
                      'mean_var':'pr_anomaly_mean',
                      'std_var':'pr_anomaly_std',
                      'y_labels':'percent',
                      'offset':0}]
    
    variable_title_text = [v['variable_desc'] for v in variable_info]
    
    fig = make_subplots(rows=len(variable_info), cols=1, shared_xaxes=True,
                        subplot_titles=variable_title_text)
    
    # Add the data to each of the plots
    for v_i, v in enumerate(variable_info):
        # Tie togther the y + x info to generate a unique string
        hover_attributes = zip(x_axis_labels,pixel_data[v['mean_var']])
        hover_text = [generate_hover_str(v['variable'], *attr) for attr in hover_attributes]
        
        fig.append_trace(go.Scatter(x=pixel_data.year + v['offset'], y=pixel_data[v['mean_var']],
                                    error_y = dict(type='data',array=pixel_data[v['std_var']], width=0, thickness=3),
                                    mode='markers', marker=dict(color=v['color'], size=10),
                                    hovertext = hover_text, hoverinfo = "text",
                                    name=v['variable_desc'],  # only the top gets legend entries
                                    showlegend=False),
                         row=v_i+1,col=1)
        
        # Specifying axis labels
        fig.update_xaxes(tickmode='array', tickangle=-45,
                          tickvals = x_axis_values, ticktext = x_axis_labels,
                          gridcolor='grey')
        
        if v['y_labels'] == 'percent':
            fig.update_yaxes(tickmode='array', range=[min(y_axis_percent_values),max(y_axis_percent_values)],
                              tickvals = y_axis_percent_values, ticktext = y_axis_percent_labels,
                              gridcolor='grey', row=v_i+1, col=1)
        elif v['y_labels'] == 'temp':
            fig.update_yaxes(tickmode='array', range=[min(y_axis_temp_values),max(y_axis_temp_values)],
                              tickvals = y_axis_temp_values, ticktext = y_axis_temp_labels,
                              gridcolor='grey', row=v_i+1, col=1)
            
    
    # Horizontal lines 
    hline = dict(type='line', 
                x0=x_axis_values.min()-10,x1=x_axis_values.max()+10,
                y0=0,y1=0, 
                line=dict(color='black',width=2))
    for variable_i in range(len(variable_info)):
        fig.add_shape(hline, row=variable_i+1,col=1)
    

    # fig.update_layout(legend=dict(x=0, y=-0.5)) # if a legend is ever added this will adjust the location.
    fig.update_layout(margin=dict(l=50, r=50, t=50, b=50))
    fig.update_layout(title = '', height=600, plot_bgcolor='white')

    return dcc.Graph(figure = fig)

#################################################                                    
#################################################
if __name__ == '__main__':
    app.run_server(debug=debug)
    
