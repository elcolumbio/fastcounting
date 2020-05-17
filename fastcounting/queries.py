"""Queries and data processing classes you can use or get inspired by."""
import datetime as dt
import numpy as np
import pandas as pd
import redis

from fastcounting import helper, views

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


def query_atomicview(start_date, end_date, count=None):
    """Expects timestamp integers, returns list of atomicIDs, Timestamp pairs."""
    return r.xrange('atomicview', start_date, end_date, count)


def query_accountview(account, start_date=0, end_date=int(dt.datetime.today().timestamp()), count=None):
    return r.xrange(f'account:{account}', start_date, end_date, count)


def account_name_pairs():
    view = []
    for account in views.return_all_accounts():
        accountsystem = r.hget(f'accountsystem:{account}', 'Kontenbezeichnung')
        view.append({'value': account, 'label': accountsystem})
    return view


def general_context(generalid):
    """Get all childs wich map to the same generalid."""
    atomicids = r.zrangebyscore('general:atomic', generalid, generalid)
    data = [r.hgetall(f'atomicID:{atomicid}') for atomicid in atomicids]
    return data
