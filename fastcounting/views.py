"""Queries and data processing classes you can use or get inspired by."""
import datetime as dt
import numpy as np
import pandas as pd
import redis

from . import helper, files

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)


# datefilter
def string_parser(start, end):
    """We use for plotly date filter."""
    start = dt.datetime.strptime(start.split('T')[0], '%Y-%m-%d')
    end = dt.datetime.strptime(end.split('T')[0], '%Y-%m-%d')
    return start.timestamp(), end.timestamp()


def query_atomic_date(start_date, end_date):
    """Expects timestamp integers, returns list of atomicIDs, Timestamp pairs."""
    return r.zrangebyscore('atomic:date', start_date, end_date, withscores=True)


def query_atomics(atomic_list):
    result_list = []
    for atomic in atomic_list:
        row = r.hgetall(f'atomicID:{atomic[0]}')
        row.update({'Buchungsdatum':dt.datetime.fromtimestamp(atomic[1])})
        result_list.append(row)
    return pd.DataFrame(result_list)


def main_datefilter(start_date, end_date):
    """Takes 2 timestamps as input."""
    atomic_list = query_atomic_date(start_date, end_date)
    df = query_atomics(atomic_list)
    df['generalID'] = df['generalID'].astype(np.int64)

    return df