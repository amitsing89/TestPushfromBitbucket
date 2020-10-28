import dash
import dash_core_components as dcc
import dash_html_components as html
import RediswithPython as r
import datetime as dt
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

redisval = r.retrieve()
print(list(map(int, redisval.values())))
app.layout = html.Div(children=[
    html.H1('The time is: ' + str(dt.datetime.now())),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'y': list(map(int, redisval.values())), 'x': list(map(str, redisval.keys())), 'type': 'bar', 'name': 'SF'},
                # {'x': list(map(int, redisval.values())), 'y': list(map(int, redisval.values())), 'type': 'bar', 'name': u'Montréal'},
            ],
            'layout': {
                'title': 'Dash Data Visualization'
            }
        }
    ),
    dcc.Interval(id='interval-component',interval=1*10,n_intervals=0)# Millisecondsn_interval=0)]
])

if __name__ == '__main__':
    app.run_server(debug=True)
