"""To display you can import your standard form of accounts here. We join it with accountID."""
import pandas as pd
import redis

from . import helper


r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)


def accountsystem_toredis(filename='kontenrahmen.txt'):
    """Push hash. E.g.: r.hgetall(f'accountsystem:{100}')."""
    file = helper.Helper().datafolder(filename)
    system = pd.read_table(file, sep='\t', engine='python', decimal=',')
    # some accounts you personal defined are not in the standard accountsystem
    system.loc[1] = len(system.columns) * ['special_account']
    for row in system.iterrows():  # iterrow for 1000 rows is ok
        data = dict(row[1])  # shortcut we are not explicit
        r.hset(f"accountsystem:{data['Konto-Nummer']}", data)
    return True