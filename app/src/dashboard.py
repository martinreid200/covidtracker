''' Classes for Dashboard layouts with interactive Maps '''

import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import plotly.graph_objects as go
import plotly.express as px

rowspacer = dbc.Row(style={"height": "1rem"})

class DashboardLayout:
    ''' Main application dashboard with dropdowns, map and daily/weekly trend chart layouts '''

    def __init__(self, mapparams, cases):

        self.mapparams = mapparams
        self.cases = cases
        self.layout = self._get_layout()


    def _get_layout(self):
        ''' Create HTML layout for map dashboard '''

        return html.Div(id="mapdashboard",children=[
            dbc.Row([
                dbc.Col(self._create_map_measure_dd_div()),
                dbc.Col(self._create_map_level_dd_div()),
            ],no_gutters=True), 
            rowspacer, 
            dbc.Card(
                dbc.Row([
                    dbc.Col(dcc.Graph(id='map', figure={}, config={'displayModeBar': False}),width=6),
                    dbc.Col(html.Div(id='table' ),width=6),
                ],no_gutters=True)
            ), 
            rowspacer, 
            dbc.Row([
                dbc.Col(dbc.Card(dcc.Graph(id='mapchart', figure={})),width=12),
            ]),
            rowspacer, 
            dbc.Row([
                html.Div("Data updated at: "+self.cases.latest_data_load_timestamp.strftime( "%H:%M %d/%m/%Y"), style={"font-size":"80%", "padding-left": "1rem"}),
            ])
        ])


    def _create_map_level_dd_div (self):
        ''' Map hierachy level dropdown box '''

        return html.Div([
            dcc.Dropdown(
                id='map_level_dd',
                options=[{'label': "Map by "+i, 'value': i} for i in self.cases.levels[1:]],
                value=self.mapparams['plotlevel'],clearable=False
            )
        ])


    def _create_map_measure_dd_div (self):
        ''' Map measure dropdown box '''

        return html.Div([
            dcc.Dropdown(
                id='map_measure_dd',
                options=[{'label': i, 'value': i} for i in self.cases.map_measures],
                value=self.mapparams['plotmeasure'],clearable=False
            )
        ])


class Map:
    ''' Interactive map figure for given mapparameters '''

    def __init__(self, mapparams, cases):
        
        self.mapparams = mapparams
        self.cases = cases
        self.figure = self._create_map()


    def _create_map(self):
        ''' Generate choropleth map for given plot level'''

        mapdf = self.cases.summarydf.loc[(self.cases.summarydf['Area type'] == self.mapparams['plotlevel'])]

        f = go.Figure(go.Choroplethmapbox(
            geojson=self.cases.geo_data[self.mapparams['plotlevel']]["data"],
            locations=mapdf['Area code'], 
            z=mapdf[self.mapparams['plotmeasure']],
            featureidkey="properties." + self.cases.geo_data[self.mapparams['plotlevel']]["key"],
            colorscale=px.colors.sequential.YlGn, 
            text=mapdf['Area name'],
            customdata=mapdf,
            colorbar=dict(thickness=10,ypad=0,xpad=0, x=0),
            marker_opacity=0.5, 
            marker_line_width=0)
        )

        f.update_layout(mapbox_style="carto-positron",autosize=True,clickmode="event", hovermode="closest", 
                        mapbox_zoom=5, mapbox_center = {"lat": 53, "lon": -1.9},#height=468,
                        margin={"l":20,"t":20,"r":20,"b":20}, 
                        annotations=[dict(x=0.99,y=0.99,showarrow=False,text="Weekly metrics to "+self.cases.latest_complete_week),
                        dict(x=0.99,y=0.96,showarrow=False,text="Zoom / click on map area for detail")] )

        return f



class MapCardLayout:
    ''' Layout for card showing stats and small daily trend chart'''

    def __init__(self, plotparams, cases, cardfigs, chart):
    
        self.plotparams = plotparams
        self.cases = cases
        self.cardfigs = cardfigs
        self.chart = chart
        self.layout = self._create_mapcard()


    def _create_mapcard(self):

        # Set hyperlink button on card next to map
        # Include Tests data if available at the selected plot level
        measures = 'Cases|Average'
        if self.plotparams['plotlevel'] in self.cases.measures_availability['Tests']:
            measures = measures + '|Tests:y2'

        chart_link = '/chart7/'+self.plotparams['plotlevel']+'/'+self.plotparams['plotareas']+'/'+measures+'/Daily Cases:*/28/'

        # Create layout
        return html.Div(
            [
                dbc.Row(children=[
                    dbc.Col(html.H5(self.plotparams['plotareas'],className='card-title'),width='auto'),
                    dbc.Col(html.A(html.Img(src='assets/line-chart-icon.jpg', height='20px'),href=chart_link),width='auto',className='pl-0')
                ],justify='between'),
                html.P('Population: '+str(self.cardfigs[3]) + ' - Total Cases: '+str(self.cardfigs[4]) + ' - Peak Daily Cases: '+str(self.cardfigs[6]) ,style={'font-size':'80%'}),
                dcc.Graph(figure=self.chart,config={'displayModeBar': False})
            ]
        ,style={'padding-top': '15px','padding-right': '15px','padding-bottom': '5px'})