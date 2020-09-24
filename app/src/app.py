############################################################################################
# Main dash web application
############################################################################################

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import flask
from dash.dependencies import Input, Output, State
import pandas as pd
import json
import os
import urllib

server = flask.Flask(__name__)
app = dash.Dash(
    name=__name__ , external_stylesheets=[dbc.themes.BOOTSTRAP, 'assets/styles.css'],
    server=server, url_base_pathname = '/',
    meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}]
)
app.config.suppress_callback_exceptions = True
app.title = 'Covid-19 Tracker'
      
PLOTLY_LOGO = 'https://images.plot.ly/logo/new-branding/plotly-logomark.png'
DEFAULT_MAP_MEASURE = 'Last 14 Days Trend Slope'
REFRESH_INTERVAL = 60 * 1000 * 5   # Check for new data every 5 minutes

# Load our app utilities

from dataframes import CasesData
from tables import TableLayout
from charts import ChartLayout, Chart
from dashboard import DashboardLayout, Map, MapCardLayout


def decode_urlpath(path):
    ''' Decodes application URL path, returns list of parameters with starting path as the first '''

    urlparams = path.split('/') 
    print('\n\nMAIN CALLBACK:',urlparams)

    # Correct dash table markdown links to skip first invalid link in the URL (dash creates link as /table1/link7/.....)
    offset = 0
    if len(urlparams) > 3:
        if urlparams[3].startswith('chart'):
            offset = 2

    # Replace unescaped list of params
    return list(map(lambda st: urllib.parse.unquote(st), urlparams[offset:] ))


######################################################################################################
# Dash application call backs


@app.callback(
    Output('navbar-collapse', 'is_open'),
    [Input('navbar-toggler', 'n_clicks')],
    [State('navbar-collapse', 'is_open')],
)
def toggle_navbar_collapse(n, is_open):
    ''' Header Toggle - Callback for toggling the collapse on small screens '''
    if n:
        return not is_open
    return is_open



@app.callback(
    [Output('mapchart', 'figure'),
    Output('table', 'children')],
    [Input('map', 'clickData')]
)
def display_mapchart(clickData):
    '''  Plot trend charts when map area is clicked '''

    print('MAP TREND CALLBACK')

    # Initial default line chart options - England
    plotparams = { 
        'plotlevel' : cases.levels[0],
        'plotareas' : 'England',
        'plotvars' : 'Cases|Average',
        'plottitle' : '',
        'plotdays' : 28,
        'showtitle' : False,
        'periodicity' : 'daily'
    } 
    
    if clickData is not None:
        # Get plot parameters from clicked map area
        
        print(clickData)
        data = clickData['points'][0]['customdata']
        plotparams['plotareas']  = data[1]
        plotparams['plotlevel']  = cases.get_plot_level(data[1]) 

    else:
        # Use initial default parameters, if nothing selected on map
        data = pd.Series(cases.summarydf.loc[(cases.summarydf['Area type'] == plotparams['plotlevel']) & (cases.summarydf['Area name'] == plotparams['plotareas'])].iloc[0])

    # Create daily trend chart next to map
    daily_chart = Chart(plotparams, cases)

    # Create weekly bar chart below map
    plotparams['plottitle'] = 'Weekly Change in Cases - '+ plotparams['plotareas']
    plotparams['plotvars'] = 'Cases:bar'
    plotparams['periodicity'] = 'weekly'
    plotparams['showtitle'] = True
    plotparams['plotdays'] = 0

    weekly_chart = Chart(plotparams, cases)

    map_card = MapCardLayout(plotparams=plotparams, cases=cases, cardfigs=data, chart=daily_chart.figure)

    return weekly_chart.figure, map_card.layout



@app.callback(
    Output('map', 'figure'),
    [Input('map_level_dd', 'value'),
    Input('map_measure_dd', 'value')]
)
def display_map(selmaplevel, selmapmeasure):
    ''' Main Dashboard - Display map when map level and measure dropdowns are selected '''

    print('MAP CALLBACK - map plot level',selmaplevel,'map plot measure',selmapmeasure)

    mapparams = {
        'plotlevel' : selmaplevel,
        'plotmeasure' : selmapmeasure
    }
    map = Map(mapparams, cases)
    return map.figure #create_map(selmaplevel, selmapmeasure, cases)



@app.callback(
    Output('plotareadd', 'options'),
    [Input('plotleveldd', 'value')],
)
def set_plotarea_options(selplotlevel):
    ''' Populate chart plot area dropdown when plot level dropdown is set (Interactive chart) '''

    print('PLOT AREA CALLBACK - selplotlevel',selplotlevel)

    return [{'label': i, 'value': i} for i in cases.hierachy[selplotlevel]]



@app.callback(
    [Output('graph1', 'figure'),
    Output('graph2', 'figure')],
    [Input('plotleveldd', 'value'),
    Input('plotareadd', 'value'),
    Input('intermediate-value', 'children')]
    )
def update_adhoc_graphs(selected_plotlevel, selected_plotarea, json_params):
    ''' Update Interactive graphs when plot level and plot area selected '''

    print('ADHOC CALLBACK selplotlevel:',selected_plotlevel, 'sel area:',selected_plotarea)

    # Get current plot parameters from hidden intermediate-value div
    plotparams = json.loads(json_params)

    if selected_plotarea.replace('%20',' ') not in cases.hierachy[selected_plotlevel] :
        print('Warning, area doesnt exist')
        area = ''
    else:
        area = selected_plotarea

    plotparams['plotlevel'] = selected_plotlevel
    plotparams['plotareas'] = area

    # Update charts
    chart1 = Chart(plotparams, cases)
    plotparams['plotdays'] = 0
    chart2 = Chart(plotparams,cases)

    return chart1.figure, chart2.figure



@app.callback(Output('latest_cases_date', 'children'),
            [Input('interval-component', 'n_intervals')])
def update_data(n):
    ''' Data refresh interval callback.  Loads new data if we have any '''

    print('UPDATE DATA CALLBACK STARTED')
    cases.load()
    return html.Div('Data to: '+cases.latest_case_date)



@app.callback([Output('page-content', 'children'),
            Output('intermediate-value', 'children')], 
            [Input('url', 'pathname')])
def render_page_content(path):
    ''' Main application start callback, serves up appropriate page depending on URL '''

    if path is not None:
        
        # decode url path to set up plot parameters
        urlparams = decode_urlpath(path)

        print('MAIN CALLBACK PARAMS:',urlparams)

        # Main home page Map dashboard
        if urlparams[1] in ['', 'link1']:
        
            mapparams = {
                'plotlevel' : cases.levels[2],
                'plotmeasure' : DEFAULT_MAP_MEASURE
            }

            print('CREATING DASHBOARD LAYOUT',mapparams)
            dashboard = DashboardLayout(mapparams=mapparams, cases=cases)
            return dashboard.layout, None


        # Trend charts
        elif urlparams[1].startswith("chart"):  # in ['link2', 'link4', 'link7']:

            plotparams = {
                'link' : urlparams[1],
                'plotlevel' : urlparams[2],
                'plotareas' : urlparams[3],
                'plotvars' : urlparams[4],
                'plottitle' : urlparams[5],
                'plotdays' : int(urlparams[6]),
                'showtitle' : True,
                'periodicity' : 'daily'
            }

            print('CREATING CHARTS LAYOUT',plotparams)
            chart = ChartLayout(plotparams=plotparams, cases=cases)
            return chart.layout, json.dumps(plotparams)


        # Tables
        elif urlparams[1].startswith('table'):
            
            tableparams = {
                'link' : urlparams[1],
                'plotlevel' : urlparams[2],
                'plottitle' : urlparams[3],
                'hyperlink' : True
            }

            print('CREATING TABLE LAYOUT',tableparams)
            table = TableLayout(tableparams=tableparams, df=cases.summarydf)
            return table.layout, None


        # Batch log
        elif urlparams[1] in ['log']:
            log_file = os.path.dirname(__file__)+'/data/batch.log'
            if os.path.exists(log_file):
                with open(log_file) as f:
                    data = f.readlines()
                return html.Div(id='log',children=[html.P(data)], style={'whiteSpace': 'pre-line'}), None
            else:
                return html.Div(id='log',children=[html.P("No batch log found.")]), None


        # If the user tries to reach a different page, return a 404 message
        return dbc.Jumbotron(
            [
                html.H1('404: Not found', className='text-danger'),
                html.Hr(),
                html.P(f'The pathname {path} was not recognised...'),
            ]
        ), None

    else:
        return None, None

# Load our data structures
cases = CasesData()


######################################################################################################
# Application layout

# Links
links_bar = dbc.Row(
    [
        dbc.Col(
            children=[
                dbc.Row(html.A(html.Div('Source Data: coronavirus.data.gov.uk'),href='https://coronavirus.data.gov.uk/about',style={'font-size':'80%','text-decoration':'none','color':'white'}),justify='end'),
                dbc.Row(html.A(html.Div('Data to: '+cases.latest_case_date,id='latest_cases_date'),href='https://coronavirus.data.gov.uk/about',style={'font-size':'70%','text-decoration':'none','color':'white'}),justify='end')
            ],width='auto',className='pr-3'
        ),
        dbc.Col(dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem('England', href='/chart2/'+cases.levels[0]+'/England/Cases|Average|Tests:y2/Daily Cases:*/28/'),
                dbc.DropdownMenuItem('Ealing, Nottingham & Windsor', href='/chart2/'+cases.levels[2]+'/Ealing|Nottingham|Windsor and Maidenhead/Cases|Average/Daily Cases:*/28/'),
                dbc.DropdownMenuItem('London, East Midlands & South East', href='/chart2/'+cases.levels[1]+'/London|East Midlands|South East/Cases|Average/Daily Cases:*/28/'),
                dbc.DropdownMenuItem('Liverpool, Bromley & Dacorum', href='/chart2/'+cases.levels[3]+'/Liverpool|Bromley|Dacorum/Cases|Average/Daily Cases:*/28/'),
                dbc.DropdownMenuItem('Region Comparison', href='/chart2/'+cases.levels[1]+'/South West|South East|London|East of England|West Midlands|East Midlands|Yorkshire and The Humber|North West|North East/Average/7 Day Rolling Average Cases - Regions/28/'),
                dbc.DropdownMenuItem('Nation Comparison', href='/chart4/'+cases.levels[0]+'/England|Scotland|Wales|Northern Ireland/Cases|Average|Tests:y2/Cases:*/90/'),
                dbc.DropdownMenuItem('Interactive - Cases', href='/chart7/'+cases.levels[1]+'/East Midlands/Cases|Average/Daily Cases:*/28/'),
                dbc.DropdownMenuItem('Interactive - Hospital Cases', href='/chart7/'+cases.levels[0]+'/England/Hospital Cases|Average Hospital Cases/Hospital Cases:*/28/'),
                dbc.DropdownMenuItem('Interactive - Deaths', href='/chart7/'+cases.levels[0]+'/England/Deaths within 28 Days of Positive Test|Average Deaths/Deaths:*/28/'),
            ],
            right=True,
            label='Charts',
        ),width='auto',className='pr-0'),
        dbc.Col(dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem('Local Authorities with Increasing Fortnightly Cases', href='/table1/'+cases.levels[3]+'/Local Authorities with Increasing Fortnightly Cases (To Week Ending '+cases.latest_complete_week+')'),
                dbc.DropdownMenuItem('Local Authorities with Rising 14 Day Case Trend', href='/table2/'+cases.levels[3]+'/Local Authorities with Rising 14 Day Case Trend'),
                dbc.DropdownMenuItem('Summarised Data - Region', href='/table3/'+cases.levels[1]+'/Summarised Data - Region'),
                dbc.DropdownMenuItem('Summarised Data - Upper Tier Authorities', href='/table3/'+cases.levels[2]+'/Summarised Data - Upper Tier Authorities'),
                dbc.DropdownMenuItem('Summarised Data - Lower Tier Authorities', href='/table3/'+cases.levels[3]+'/Summarised Data - Lower Tier Authorities')
            ],
            right=True,
            label='Tables',
        ),width='auto'),
    ],
    no_gutters=False,
    className='ml-auto flex-nowrap mt-3 mt-md-0',
    align='center', justify='end'
)

# Header bar with Links in collapsible div
header = dbc.Navbar(
    [
        html.A(
            dbc.Row(
                [
                    dbc.Col(html.Img(src=PLOTLY_LOGO, height='30px'),width = 2),
                    dbc.Col(dbc.NavbarBrand('UK Covid-19 Tracker'),className='pl-2'),
                    dbc.Col(id='live-update-text'),  # Dummy div used to force data updates on polling interval
                ],
                align='center',
                no_gutters=True,
            ),
            href='/',
        ),
        dbc.NavbarToggler(id='navbar-toggler'),
        dbc.Collapse(links_bar, id='navbar-collapse', navbar=True),
    ],
    color='dark',
    dark=True,
)

# Main content div
content = html.Div(id='page-content')


def serve_layout():
    ''' Main application layout function, called on every browser force refresh and app home page '''

    print('\nSERVING LAYOUT')

    # Make sure we have the latest data loaded
    cases.load()

    # Return our web page header and main content 
    return html.Div([dcc.Location(id='url'), header, content,  
            html.Div(id='intermediate-value', style={'display': 'none'}),
            dcc.Interval(id='interval-component', interval=REFRESH_INTERVAL, n_intervals=0)])

app.layout = serve_layout


print('\nFinished Loading....')

# # Start the app if running locally

if __name__ == '__main__':
    print('Starting server')
    server.run(debug=True)
    #app.run_server(debug=True)