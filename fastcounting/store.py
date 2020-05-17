"""Main ETL writing to our redis database."""
import datetime as dt
import importlib.resources as pkg_resources
import redis

from fastcounting import helper, files, lua

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)

# lua scripts we import from static files under the lua folder.
general_relation = pkg_resources.read_text(lua, 'general_relation.lua')


def first_walk(df, batch):
    """
    First of two walks for splitted multirow transactions only the first row
    has general information, we run only over those here.
    """
    for i in df.index:
        # get unique id from database (threadsave)
        generalID = r.incr('next_generalID')
        # create temporary mapping
        r.set(df.at[i, 'Nr.'], generalID, ex=500)
        # create batch mapping
        r.zadd('batch:general', {generalID: batch})
        # store data in hash
        r.hset(f'generalID:{generalID}', mapping={
            'date': df.at[i, 'Belegdat.'],
            'jourdat': df.at[i, 'Jour. Dat.'],
            'buchdat': df.at[i, 'Buchdat.'],
            'status': df.at[i, 'Status'],
            'belegnr': df.at[i, 'Belegnr.']
        })


def atomic_to_redis(i, account, amount, kontenseite, date, text, nr, batch, ust=None):
    """Create all atomic hashes there can be up to 4 hashes for each row in our input file."""
    account = int(account)  # account and amount (*100) should be int
    # this depends on which standard accounting frame you use
    # i wrote my own mapping function in systems
    if int(r.hget(f'accountsystem:{account}', 'bool_incr_left')):
        # active account increases in 'Soll'
        amount = amount
    else:
        # passive account increases in 'Haben'
        amount = (-1) * amount

    # get unique id from database (threadsave)
    atomicID = r.incr('next_atomicID')
    # get temporary mapping we created in the first walk
    generalID = r.get(nr)

    # create batch mapping
    r.zadd('batch:atomic', {atomicID: batch})

    # create stable mapping- general:atomic:
    r.zadd('general:atomic', {atomicID: generalID})
    # create mapping accountID:atomicID
    r.zadd('account:atomic', {atomicID: account})
    # create datefilter atomic:date
    r.zadd('atomic:date', {atomicID: date})  # could think about splitting the key into years
    # store data in hash + mapping atomic:general + mapping atomic+account
    r.hset(f'atomicID:{atomicID}', mapping={
            'generalID': generalID,
            'accountID': account,
            'text': text,
            'amount': amount,
            'kontenseite': kontenseite,
            'batchID': batch})


def second_walk(df, batch):
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
            atomic_to_redis(i, df.at[i, 'Sollkto'], df.at[i, 'SollEUR'], 'Soll', date, text, nr, batch)

        if df.at[i, 'Habenkto']:
            atomic_to_redis(i, df.at[i, 'Habenkto'], -df.at[i, 'HabenEUR'], 'Haben', date, text, nr, batch)

        if df.at[i, 'USt Kto-H']:
            atomic_to_redis(i, df.at[i, 'USt Kto-H'], -df.at[i, 'USt H-EUR'], 'Haben', date, text, nr, batch)

        if df.at[i, 'USt Kto-S']:
            atomic_to_redis(i, df.at[i, 'USt Kto-S'], df.at[i, 'USt-S EUR'], 'Soll', date, text, nr, batch)


def create_relation(batch):
    """We create feature to summarize all atomics for one generalized accounting transaction."""
    r.eval(general_relation, 1, 'batch:general', batch, batch)
    return True


def main(month):
    # This should be fine.
    batch = str(int(dt.datetime.today().timestamp()))
    df, filename = files.main_etl(month)
    print(filename)
    first_walk(df, batch)
    df['Nr.'].ffill(inplace=True)  # this we have to do between first and second walk
    second_walk(df, batch)
    # you can add your own additional features like that too
    create_relation(batch)
    return batch
