import dash
import dash_html_components as html
import dash_table

TABLE_METADATA = {
    'table1' : { 
        'cols' : ['Area name','Population','All Time Cases','Cases in Last Fortnight','Cases in Previous Fortnight','Fortnightly % Change'],
        'filtercol' : 'Fortnightly % Change',
        'filtervalue' : 0,
        'sortcol' : 'Fortnightly % Change',
        'ascending' : False
    },
    'table2' : { 
        'cols' : ['Area name','Population','All Time Cases','Last 14 Days Trend Slope','Last 4 Weeks Cases','Cases in Last Fortnight'],
        'filtercol' : 'Last 14 Days Trend Slope',
        'filtervalue' : 0.05,
        'sortcol' : 'Last 14 Days Trend Slope',
        'ascending' : False
    },
    'table3' : { 
        'cols' : [],
        'filtercol' : None,
        'filtervalue' : None,
        'sortcol' : 'Area name',
        'ascending' : True
    },
}

class TableLayout:
    ''' Dash Tables html layout creation '''

    def __init__(self, tableparams, df):
        
        self.tableparams = tableparams
        self.tabledf = self._filter_dataframe(df)
        self.layout = self._get_layout()


    def _filter_dataframe(self, df):
        ''' Create data frame based on table metadata '''

        table = self.tableparams['link']

        # Set table columns
        cols = TABLE_METADATA[table]['cols']
        if len(cols) == 0:   # None defined so default to all columns exception code and type
            cols = [c for c in df.columns if (c!= 'Area code' and c!= 'Area type') ] 

        # Filter & sort
        tdf = df.loc[df['Area type'] == self.tableparams['plotlevel']][cols]

        if TABLE_METADATA[table]['filtercol']:
            tdf = tdf.loc[(tdf[TABLE_METADATA[table]['filtercol']] > TABLE_METADATA[table]['filtervalue'])]
        tdf = tdf.sort_values(by=[TABLE_METADATA[table]['sortcol']],ascending=TABLE_METADATA[table]['ascending'])

        return tdf


    def _get_layout(self):
        ''' Generates table layout '''

        if self.tableparams['hyperlink']:
            self.tabledf['Area name'] = self.tabledf.apply(lambda x: self._make_hyperlink(x['Area name'],self.tableparams['plotlevel']), axis=1)
        
        return html.Div(children=[
            html.H4(children=self.tableparams['plottitle']),
            dash_table.DataTable(
                id='datatable-interactivity',
                columns=[
                    {'name': i, 'id': i, 'selectable': True, 'presentation': self._column_presentation(i) } for i in self.tabledf.columns
                ],
                sort_action='native',
                markdown_options={'link_target': '_self'},
                style_table={'width': '100%', 'minWidth': '100%', 'overflow':'auto'},
                style_cell={'height':'25px','vertical-align':'top','padding-top':'1px','font-family':'sans-serif', 'padding-left':'5px','padding-right':'5px','border': 'solid 1px #DDEEEE'},
                style_header={'vertical-align':'bottom','whiteSpace':'normal','font-family':'sans-serif','background-color': '#5A6268', 'color': 'white', 'border': 'solid 1px #5A6268'},
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(248, 248, 248)'
                    },
                    {
                        'if': {'state': 'selected'},              # 'active' | 'selected'
                        'backgroundColor': 'transparent',
                        'border': 'solid 1px #DDEEEE'
                    }
                    ],
                style_cell_conditional=[
                    {
                        'if': { 'column_id': 'Area name' },
                        'text-align': 'left','min-width':'120px' ,'overflow':'auto', 'whiteSpace': 'normal'
                    },
                    {
                        'if': {
                            'column_id': 'Fortnightly % Change',
                            'filter_query': '{Fortnightly % Change} > 0'
                        },
                        'color': 'red'
                    },
                    {
                        'if': {
                            'column_id': 'Last 14 Days Trend Slope',
                            'filter_query': '{Last 14 Days Trend Slope} > 0'
                        },
                        'color': 'red'
                    },
                    ],
                data=self.tabledf.to_dict('records')
            )
        ])


    def _column_presentation(self, col):
        ''' Determines whether to represent column as markdown '''
        return 'markdown' if col=='Area name' else 'input'


    def _make_hyperlink(self, area, plotlevel):
        ''' Generates area name table hyperlink '''
        url = 'chart7/'+plotlevel.replace(' ','%20')+'/'+area.replace(' ','%20')+'/Cases|Average/Daily%20Cases:*/28/'
        return '['+area+']('+url+')'
