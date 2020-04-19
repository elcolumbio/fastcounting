"""Queries and data processing classes you can use or get inspired by."""
import datetime as dt
import numpy as np
import pandas as pd
import redis

from . import helper, files

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)


# datefilter
def string_parser(start, end):
    """We use to cleanup plotly date picker output values."""
    start = dt.datetime.strptime(start.split('T')[0], '%Y-%m-%d')
    end = dt.datetime.strptime(end.split('T')[0], '%Y-%m-%d')
    return start.timestamp(), end.timestamp()


def query_atomicview(start_date, end_date):
    """Expects timestamp integers, returns list of atomicIDs, Timestamp pairs."""
    return r.xrange('atomicview', start_date, end_date)


def main_datefilter(start_date, end_date):
    """Takes 2 timestamps as input."""
    df = query_atomicview(start_date, end_date)
    df['generalID'] = df['generalID'].astype(np.int64)
    return df