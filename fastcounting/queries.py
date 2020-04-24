"""Queries and data processing classes you can use or get inspired by."""
import datetime as dt
import numpy as np
import pandas as pd
import redis

from . import helper, files, views

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)


def stream_to_dataframe(streamdata):
    """Parse List of tuples, where second tuple is a dictionary to pandas dataframe."""
    data = np.array(streamdata).flatten()
    df = pd.DataFrame(data[1::2].tolist(), index=data[0::2])
    df['date'] = df['date'].astype(int)
    df['amount'] = df['amount'].astype(float)
    df['date'] = pd.to_datetime(df['date'], unit='s')
    df['jourdate'] = pd.to_datetime(df['jourdate'], unit='s')
    df['amount'] = df['amount']/100
    df['date'] = df['date'].dt.date
    df['jourdate'] = df['jourdate'].dt.date
    return df


# datefilter
def string_parser(start, end):
    """We use to cleanup plotly date picker output values."""
    start = dt.datetime.strptime(start.split('T')[0], '%Y-%m-%d')
    end = dt.datetime.strptime(end.split('T')[0], '%Y-%m-%d')
    print(start, end)
    return int(start.timestamp()), int(end.timestamp())


def query_atomicview(start_date, end_date):
    """Expects timestamp integers, returns list of atomicIDs, Timestamp pairs."""
    return r.xrange('atomicview', start_date, end_date)


def df_atomicview(start_date, end_date):
    """Takes 2 timestamps as input."""
    df = query_atomicview(start_date, end_date)
    df['generalID'] = df['generalID'].astype(np.int64)
    return df


def query_accountview(account, start_date=0, end_date=int(dt.datetime.today().timestamp())):
    return r.xrange(f'account:{account}', start_date, end_date)


def account_name_pairs():
    view = []
    for account in views.return_all_accounts():
        accountsystem = r.hget(f'accountsystem:{account}', 'Kontenbezeichnung')
        view.append({'value': account, 'label': accountsystem})
    return view