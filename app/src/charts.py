import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import pandas as pd
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_core_components as dcc

rowspacer = dbc.Row(style={'height': '1rem'})
IGNORE_DAYS = -3

class Chart:

    ''' Classes for Charting, plots are controlled by the plotparams dictionary as follows:

        plotlevel   : Hierachy level (e.g Nation, Region)
        plotareas   : Text string of areas to plot (separated by pipe character), e.g.  'London|East Midlands'
        plotvars    : Text string of variables to plot, separated by pipe chracter
                      Axes and plot types are separated by colon character e.g. 'Cases:y1:line|Average:y2:bar'
        plottitle   : Title of chart - if contains ':*' then title will include area names
        showtitle   : Flag to show chart title or not
        plotdays    : Controls how many days to plot (if zero then we plot everything)
        periodicity : Controls whether we plot daily or weekly data
    ''' 

    def __init__(self, plotparams, cases):

        self.plotparams = plotparams
        self.figure = self.create_trend_chart(cases)


    def get_plot_attributes(self, var, i):
        ''' Determine plot attributes (colour, line style) for given variable '''

        colours = ['Blue','Red','Green','Yellow','Pink','Cyan','Purple','Black','Orange','Grey']

        if var.startswith('Average'):
            # Plot averages as full line
            return dict(color=colours[i]), 1
        elif var == 'Cases' or var == 'Hospital Cases' or var.startswith('Death'):
            # Plot dotted with opaqueness
            return dict(dash='dot', color=colours[i]), 0.3
        elif 'Tests' in var:
            return dict(dash='dash', color='Pink'), 1
        else:
            return dict(color=colours[i]), 1


    def get_subplots(self):
        ''' Create our initial plot figure with secondary axis if needed '''

        for v in self.plotparams['plotvars'].split('|'):
            if 'y2' in v.split(':'):
                return make_subplots(specs=[[{'secondary_y': True}]])

        return make_subplots()


    def get_plot_name(self, area, var, plotvars):
        ''' Define name of plot that appears on legend '''

        if var.startswith('Average') and len(plotvars) > 1:
            suffix = ' 7 Day Avg'
        elif var == 'Tests':
            suffix = ' Tests'
        elif var == 'Hospital Cases' or var.startswith('Death'):
            suffix = ' ' + var
        else:
            suffix = ''

        # Exclude area name on mini chart (i.e. if we don't have a plot title)
        
        return area + suffix if self.plotparams['showtitle'] else suffix.strip()


    def get_plot_config(self, vars):
        ''' Determine yaxis and plot type (line, bar) from plot vars '''

        yaxis, ptype = 'y1', 'line'
        if 'y2' in vars:
            yaxis = 'y2'
        if 'bar' in vars:
            ptype = 'bar'

        return yaxis, ptype


    def create_trend_chart(self, cases):
        ''' Function to create daily line chart for given plot parameters '''

        print('Creating trend chart...')
        print(self.plotparams)

        # Set up plot figure and axes 

        fig = self.get_subplots()
        axes_defaults = {'showgrid' : True, 'gridwidth' : 1, 'gridcolor': '#E8E8E8', 'showline': True, 'linewidth': 1, 'linecolor': 'black'}

        fig.update_xaxes(axes_defaults)
        fig.update_yaxes(axes_defaults)
    
        if self.plotparams['periodicity'] == 'weekly':
            fig.update_yaxes(zeroline=True, zerolinecolor='black', zerolinewidth=1)

        for i,area in enumerate(self.plotparams['plotareas'].split('|')):

            # Creating plotting dataframe for required area / level / periodicity

            if self.plotparams['periodicity'] == 'weekly':

                weekly = cases.weeklydf.loc[(cases.weeklydf['Area name'] == area) & (cases.weeklydf['Area type'] == self.plotparams['plotlevel']) ].groupby(['Area code','Area name','Area type','Date']).sum().reset_index()
                weekly.set_index('Date',inplace=True)
                change = weekly['Cases'] - weekly['Cases'].shift(1) 
                iplotdf = pd.DataFrame(change,columns=['Cases'])
            
            else:
                # Daily data
                iplotdf = cases.dailydf.loc[(cases.dailydf['Area name'] == area) & (cases.dailydf['Area type'] == self.plotparams['plotlevel']) ]
                iplotdf = iplotdf.reindex(cases.date_index) 

                # Create rolling averages
                iplotdf['Average'] = iplotdf['Cases'][:IGNORE_DAYS].rolling(window=7).mean().round(2)
                iplotdf['Tests'] = iplotdf['Tests'].loc[~(iplotdf['Tests']==0)]
                iplotdf['Tests'] = iplotdf['Tests'].rolling(window=7).mean().round(2)[:IGNORE_DAYS]
                iplotdf['Average Deaths'] = iplotdf['Deaths within 28 Days of Positive Test'].rolling(window=7).mean().round(2)[:IGNORE_DAYS]  
                iplotdf['Hospital Cases'] = iplotdf['Hospital Cases'].loc[~(iplotdf['Hospital Cases']==0)]
                iplotdf['Average Hospital Cases'] = iplotdf['Hospital Cases'].rolling(window=7).mean().round(2)[:IGNORE_DAYS]

                # Limit data to last n days if plotdays is set
                if self.plotparams['plotdays'] > 0:
                    iplotdf = iplotdf[self.plotparams['plotdays'] * -1:]

            plotvars = self.plotparams['plotvars'].split('|')

            for plotvar in plotvars:
                
                vars = plotvar.split(':')

                # Decode plot definitions for this plot variable (cases = dotted lines, averages = solid etc) e.g. Cases|Tests:y2:line
                style, opacity = self.get_plot_attributes(vars[0],i)
                yaxis, ptype = self.get_plot_config(vars)

                # Update secondary axis if we have one
                if yaxis == 'y2':
                    fig.update_yaxes(title_text=vars[0],color='pink', rangemode='tozero', showgrid=False, secondary_y=True) 
                    fig.update_yaxes(title_text=plotvars[0], secondary_y=False)

                if ptype == 'line':
                    fig.add_trace(
                        go.Scatter( x=iplotdf.index, y=iplotdf[vars[0]], mode='lines', name=self.get_plot_name(area, vars[0], plotvars),
                                    opacity=opacity, line=style, yaxis=yaxis )
                    )
                else:
                    fig.add_trace(
                        go.Bar( x=iplotdf.index, y=iplotdf[vars[0]], name=self.get_plot_name(area, vars[0], plotvars),
                                marker=dict(color=np.where(iplotdf[vars[0]] > 0, 'red', 'green').tolist()),  yaxis=yaxis ) 
                )

        # Modify chart title depending on number of days, plot areas  
        title_suffix = ''
        if self.plotparams['plotdays'] > 0:
            title_suffix = ' - Last '+str(self.plotparams['plotdays'])+' Days'

        plottitle = self.plotparams['plottitle'].split(':')  
        if len(plottitle) > 1:
            title = plottitle[0] + ' - '+ self.plotparams['plotareas'].replace('|',', ')  + title_suffix
        else:
            title = plottitle[0] + title_suffix

        if self.plotparams['showtitle']:
            fig.update_layout( plot_bgcolor='white', title_text=title, title_x=0.01,legend=dict(orientation='h'),
                            margin=dict(l=20, r=20, t=60, b=30), )
        else:
            fig.update_layout( plot_bgcolor='white', height=355, showlegend=False,margin=dict(l=0, r=0, t=0, b=0) )

        return fig



class ChartLayout:
    ''' Chart HTML layout, with dropdowns for interactive charts '''

    def __init__(self, plotparams, cases):

        self.plotparams = plotparams
        self.cases = cases
        self.layout = self._get_layout()


    # Charts layout creation
    def _get_layout(self):
        ''' Creates charts html layout '''
        
        # Adhoc chart with dropdowns and empty charts - callback handles actual chart creation
        if self.plotparams['link'] == 'chart7':
            
            return html.Div(id='graphs',children=[
                self._create_dropdowns_div(),
                rowspacer,
                dbc.Card(dcc.Graph(id='graph1', figure={})),
                rowspacer,
                dbc.Card(dcc.Graph(id='graph2', figure={}))
            ])

        # 4 chart comparison - split plot areas into 4 separate charts
        elif self.plotparams['link'] == 'chart4':

            areas = self.plotparams['plotareas'].split('|')
            
            self.plotparams['plotareas'] = areas[0]
            fig1 = Chart(self.plotparams, self.cases)
            self.plotparams['plotareas'] = areas[1]
            fig2 = Chart(self.plotparams, self.cases)
            self.plotparams['plotareas'] = areas[2]
            fig3 = Chart(self.plotparams, self.cases)
            self.plotparams['plotareas'] = areas[3]
            fig4 = Chart(self.plotparams, self.cases)
        
            return html.Div(id='graphs',children=[
                dbc.Row([
                    dbc.Col(dbc.Card(dcc.Graph(id='graph1', figure=fig1.figure,config={'displayModeBar': False})),width=6,className='pr-3'),
                    dbc.Col(dbc.Card(dcc.Graph(id='graph2', figure=fig2.figure,config={'displayModeBar': False})),width=6),
                ],no_gutters=True),
                rowspacer, 
                dbc.Row([
                    dbc.Col(dbc.Card(dcc.Graph(id='graph3', figure=fig3.figure,config={'displayModeBar': False})),width=6,className='pr-3'),
                    dbc.Col(dbc.Card(dcc.Graph(id='graph4', figure=fig4.figure,config={'displayModeBar': False})),width=6),
                ],no_gutters=True)
            ])

        # Pre-defined charts - no callback associated with these, so we generate the charts here
        else:
            
            fig1 = Chart(self.plotparams, self.cases)
            self.plotparams['plotdays'] = 0
            fig2 = Chart(self.plotparams, self.cases)
            
            return html.Div(id='graphs',children=[
                dbc.Card(dcc.Graph(id='graph1', figure=fig1.figure)),
                rowspacer, 
                dbc.Card(dcc.Graph(id='graph2', figure=fig2.figure))
            ])


    def _create_dropdowns_div(self):
        '''' Creates drop down menus for interactive charts '''

        # Hide level dropdown if we are plotting data that is only held at one hierachy level
        # We still need to create it so that the callbacks get activated

        single_level = False
        vars = self.plotparams['plotvars'].split('|')
        for v in vars:
            #print(v, (cases.measures_availability[v]))
            if len(self.cases.measures_availability[v.split(':')[0]]) == 1:
                single_level = True

        if single_level :
            return html.Div(id='dropdowns',children=[
                dbc.Row([
                    dbc.Col(self._create_area_dd_div())
                ],no_gutters=True), 
                html.Div(self._create_level_dd_div(),style={'display': 'none'})
            ])
        else:
            return html.Div(id='dropdowns',children=[
                dbc.Row([
                    dbc.Col(self._create_level_dd_div()), 
                    dbc.Col(self._create_area_dd_div())
                ],no_gutters=True) 
            ])


    def _create_area_dd_div (self):
        ''' Adhoc chart area dropdown selector '''

        plotlist = self.cases.hierachy[self.plotparams['plotlevel']] 

        return html.Div([
            dcc.Dropdown(
                id='plotareadd',
                options=[{'label': i, 'value': i} for i in plotlist],
                value=self.plotparams['plotareas'],clearable=False
            )
        ])


    def _create_level_dd_div (self):
        ''' Adhoc chart hierarchy level dropdown selector '''

        return html.Div([
            dcc.Dropdown(
                id='plotleveldd',
                options=[{'label': i, 'value': i} for i in self.cases.levels],
                value=self.plotparams['plotlevel'],clearable=False
            )
        ])

