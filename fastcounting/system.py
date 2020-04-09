"""To display you can import your standard form of accounts here. We join it with accountID."""
import pandas as pd
import redis

from . import helper


r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)


def accountsystem_toredis(filename='kontenrahmen.txt'):
    """Push hash. E.g.: r.hgetall(f'accountsystem:{100}')."""
    file = helper.Helper().datafolder('kontenrahmen.txt')
    system = pd.read_table(file, sep='\t', engine='python', decimal=',')
    for row in system.iterrows():  # iterrow for 1000 rows is ok
        data = dict(row[1])  # shortcut we are not explicit
        r.hmset(f"accountsystem:{data['Konto-Nummer']}", data)
    return True