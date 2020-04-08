"""Main ETL writing to our redis database."""
import numpy as np
import pandas as pd
import redis

from fastcounting import helper, read_files

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)

def first_walk(df, batchtext):
    """
    First of two walks for splitted multirow transactions only the first row
    has general information, we run only over those here.
    """
    batchID = r.incr('next_batchID')  # rollback and diff functionality
    r.hmset(f'batchID:{batchID}',
        {'text': batchtext,
         'date': str(int(dt.datetime.today().timestamp()))})
    for i in df.index:
        # get unique id from database (threadsave)
        generalID = r.incr('next_generalID')
        # create mapping for rollback if we only run first_walk
        r.sadd(f'batch:general:{batchID}', generalID)
        # create temporary mapping
        r.set(df.at[i, 'Nr.'], generalID, ex=300)
        # store data in hash
        r.hmset(f'generalID:{generalID}',
            {'date': df.at[i, 'Belegdat.'],
            'jourdat': df.at[i, 'Jour. Dat.'],
            'buchdat': df.at[i, 'Buchdat.'],
            'status': df.at[i, 'Status'],
            'belegnr': df.at[i, 'Belegnr.']})

        
def atomic_to_redis(i, konto, betrag, kontenseite, date, text, nr, ust=None):
    # get unique id from database (threadsave)
    atomicID = r.incr('next_atomicID')
    # get temporary mapping we created in the first walk
    generalID = r.get(nr)
    batchID = r.get('next_batchID')
    # create a lookup set for all atomis in a batch
    r.sadd(f'batch:atomic:{batchID}', atomicID)
    # create stable mapping- general:atomic:
    r.sadd(f'general:atomic:{generalID}', atomicID)
    # create mapping accountID:atomicID
    r.sadd(f'account:atomic:{konto}', atomicID)
    # create datefilter atomic:date
    r.zadd('atomic:date', {atomicID: date}) # could think about splitting the key into years
    # store data in hash + mapping atomic:general + mapping atomic+account
    r.hmset(f'atomicID:{atomicID}',
           {'generalID': generalID,
            'accountID': konto,
            'text': text,
            'amount': betrag,
            'kontenseite': kontenseite,
            'batchID': batchID})
        
def second_walk(df):
    """
    Second and last walk, now we walk over every row and we unpack up to 4 dimensions per row.
    There are 3 types of accounting transactions in this row based lexware export.
    1. automatic transaction, like ust payment on revenues
    2. split multirow transaction, like payment of import taxes and handling with dhl.
        important to note you can make split multirow transactions with duplicated 
        accounts thats why we cant use a dictionary here.
    3. standard account to account mapping
    + every combination from the above
    It comes handy that split multirow transaction are seperated in rows.
    """
    for i in df.index:
        # date and nr are more like general fields, but text is different for split multirow.
        # for easy acces we store them in the atomic hash
        date = int(df.at[i, 'Belegdat.'])
        text = df.at[i, 'Buchungstext']
        nr = df.at[i, 'Nr.']
        if df.at[i, 'Sollkto']:
            atomic_to_redis(i, df.at[i, 'Sollkto'], -df.at[i, 'SollEUR'], 'Soll', date, text, nr)

        if df.at[i, 'Habenkto']:
            atomic_to_redis(i, df.at[i, 'Habenkto'], df.at[i, 'HabenEUR'], 'Haben', date, text, nr)

        if df.at[i, 'USt Kto-H']:
            atomic_to_redis(i, df.at[i, 'USt Kto-H'], df.at[i, 'USt H-EUR'], 'Haben', date, text, nr)

        if df.at[i, 'USt Kto-S']:
            atomic_to_redis(i, df.at[i, 'USt Kto-S'], -df.at[i, 'USt-S EUR'], 'Soll', date, text,nr)


def main(month):
    df, filename = read_files.main_etl(month)
    first_walk(df, filename)
    df['Nr.'].ffill(inplace=True) # this we have to do between first and second walk
    second_walk(df)
    return True

