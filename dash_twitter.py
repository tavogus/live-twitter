import dash
from dash.dependencies import Output, Event, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objs as go
from collections import deque
import sqlite3
import pandas as pd

conn = sqlite3.connect('twitter.db', check_same_thread=False)

MAX_DF_LENGTH = 100

#popular topics: google, olympics, trump, gun, usa

app = dash.Dash(__name__)
app.layout = html.Div(
    [   html.H2('Live Twitter Sentiment'),
        dcc.Input(id='sentiment_term', value='trump', type='text'),
        dcc.Graph(id='live-graph', animate=True),
        dcc.Interval(
            id='graph-update',
            interval=1*1000
        ),
    ]
)

def df_resample_sizes(df, maxlen=MAX_DF_LENGTH):
    df_len = len(df)
    resample_amt = 100
    vol_df = df.copy()
    vol_df['volume'] = 1

    ms_span = (df.index[-1] - df.index[0]).seconds * 1000
    rs = int(ms_span / maxlen)

    df = df.resample('{}ms'.format(int(rs))).mean()
    df.dropna(inplace=True)

    vol_df = vol_df.resample('{}ms'.format(int(rs))).sum()
    vol_df.dropna(inplace=True)

    df = df.join(vol_df['volume'])

    return df

@app.callback(Output('live-graph', 'figure'),
              [Input(component_id='sentiment_term', component_property='value')],
              events=[Event('graph-update', 'interval')])
def update_graph_scatter(sentiment_term):
    try:
        if sentiment_term:
            df = pd.read_sql("SELECT sentiment.* FROM sentiment_fts fts LEFT JOIN sentiment ON fts.rowid = sentiment.id WHERE fts.sentiment_fts MATCH ? ORDER BY fts.rowid DESC LIMIT 1000", conn, params=(sentiment_term+'*',))
        else:
            df = pd.read_sql("SELECT * FROM sentiment ORDER BY id DESC, unix DESC LIMIT 1000", conn)
        df.sort_values('unix', inplace=True)
        df['date'] = pd.to_datetime(df['unix'], unit='ms')
        df.set_index('date', inplace=True)
        init_length = len(df)
        df['sentiment_smoothed'] = df['sentiment'].rolling(int(len(df)/5)).mean()
        df = df_resample_sizes(df)
        X = df.index
        Y = df.sentiment_smoothed.values
        Y2 = df.volume.values
        data = plotly.graph_objs.Scatter(
                x=X,
                y=Y,
                name='Sentiment',
                mode= 'lines',
                yaxis='y2',
                )

        data2 = plotly.graph_objs.Bar(
                x=X,
                y=Y2,
                name='Volume',
                )

        return {'data': [data,data2],'layout' : go.Layout(xaxis=dict(range=[min(X),max(X)]),
                                                          yaxis=dict(range=[min(Y2),max(Y2*4)], title='Volume', side='right'),
                                                          yaxis2=dict(range=[min(Y),max(Y)], side='left', overlaying='y',title='sentiment'),
                                                          title='Live sentiment for: "{}"'.format(sentiment_term))}

    except Exception as e:
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')


if __name__ == '__main__':
    app.run_server(debug=True)