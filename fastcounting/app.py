from datetime import datetime as dt
import dash
from dash.exceptions import PreventUpdate
import dash_daq as daq
import dash_table
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd
import redis

from fastcounting import helper

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
params = ['generalID', 'accountID', 'text', 'amount',
          'kontenseite', 'batchID', 'Buchungsdatum']

theme = {'dark': True,
         'detail': '#007439',
         'primary': '#00EA64',
         'secondary': '#6E6E6E'}
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


app.layout = html.Div(children=[
    html.Div(className='row', children=[
    dcc.DatePickerRange(
        id='my-date-picker-range',
        min_date_allowed=dt(1995, 8, 5),
        max_date_allowed=dt(2021, 9, 19),
        initial_visible_month=dt(2017, 8, 5),
        end_date=dt(2017, 8, 25).date(),
        className='five columns'),
    daq.ToggleSwitch(
        id='toggle-theme',
        color=theme['primary'],
        label=['Light', 'Dark'],
        value=True,
        className='one columns'),
    ]),
    html.Br(),
    # you cant put this inside dark themes!
    dash_table.DataTable(
        id='table',
        columns=([{'id': p, 'name': p} for p in params]),
        data=[dict({param: 0 for param in params})],
        style_cell={'backgroundColor': 'white',
                    'color': 'black'},
        style_header={'backgroundColor': 'white'})
])

# date filter -> update table data
@app.callback(
    [dash.dependencies.Output('table', 'columns'),
     dash.dependencies.Output('table', 'data')],
    [dash.dependencies.Input('my-date-picker-range', 'start_date'),
     dash.dependencies.Input('my-date-picker-range', 'end_date')])
def date_filter(start_date, end_date):
    if start_date and end_date:
        start_date = dt.strptime(start_date.split('T')[0], '%Y-%m-%d')
        end_date = dt.strptime(end_date.split('T')[0], '%Y-%m-%d')
        atomics = r.zrangebyscore(
            'atomic:date', start_date.timestamp(),
            end_date.timestamp(), withscores=True)
        result_list = []
        for atomic in atomics[:100]:
            row = r.hgetall(f'atomicID:{atomic[0]}')
            row.update({'Buchungsdatum': dt.fromtimestamp(atomic[1])})
            result_list.append(row)
        df = pd.DataFrame(result_list)
        format_columns = [{"name": i, "id": i} for i in df.columns]
        print(df.head())
        return [format_columns, df.to_dict('records')]
    else:
        raise PreventUpdate


# dark theme table 1
@app.callback(
    dash.dependencies.Output('table',  'style_header'),
    [dash.dependencies.Input('toggle-theme', 'value')])
def switch_header(dark):
    if(dark):
        style_header={'backgroundColor': 'black'}
    else:
        style_header={'backgroundColor': 'white'}
    return style_header

# dark theme table 2
@app.callback(
    dash.dependencies.Output('table', 'style_cell'),
    [dash.dependencies.Input('toggle-theme', 'value')])
def switch_table(dark):
    if(dark):
        style_cell={
            'backgroundColor': 'rgb(50, 50, 50)',
            'color': 'white'}
    else:
        style_cell={
            'backgroundColor': 'white',
            'color': 'black'}
    return style_cell


if __name__ == '__main__':
    app.run_server(debug=True)
