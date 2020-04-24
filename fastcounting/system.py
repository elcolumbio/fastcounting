"""To display you can import your standard form of accounts here. We join it with accountID."""
import pandas as pd
import redis

from . import helper


r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)


def accountsystem_toredis(filename='kontenrahmen.txt'):
    """Write account metainformation. E.g.: r.hgetall(f'accountsystem:{100}')."""
    file = helper.Helper().datafolder(filename)
    system = pd.read_table(file, sep='\t', engine='python', decimal=',')

    # on which side does the account increase?
    # filters:
    active_accounts = system['Konto-Nummer'] < 2000
    passive_accounts = (2000 <= system['Konto-Nummer']) & (system['Konto-Nummer'] < 4000)
    revenues = (4000 <= system['Konto-Nummer']) & (system['Konto-Nummer'] < 5000)
    expenditures = (5000 <= system['Konto-Nummer']) & (system['Konto-Nummer'] < 7000)
    mixed = (7000 <= system['Konto-Nummer']) & (system['Konto-Nummer'] < 8000)

    revenues_from_mixed = (system['Kontenkategorie'] == 'Einnahmen') & mixed
    expenditures_from_mixed = (
        (system['Kontenkategorie'] == 'Betriebsausgaben'
         ) | (system['Kontenkategorie'] == 'Abschreibungen')) & mixed
    debitors = (system['Kontenkategorie'] == 'Debitoren') | (system['Kontenunterart'] == 'Debitoren')
    # in the 8000 + range we have creditor and debitor accounts
    # debitors should be treated as bool_incr_left = False
    # because in some reports they get summed up into an active account
    # creditors are bool_incr_left = True
    # apply filter
    system['bool_incr_left'] = 1
    system.loc[passive_accounts | revenues | revenues_from_mixed | debitors,
               'bool_incr_left'] = 0
    # when you made accounts which are in the 'wrong place' and you dont have reliable meta information
    # e.g given i made an interimskonto which has no parent account so it is like an active account.
    system.loc[system['Konto-Nummer'] == 3631, 'bool_incr_left'] = 1

    # maybe use them later
    [active_accounts, passive_accounts, revenues, expenditures,
     revenues_from_mixed, expenditures_from_mixed]

    # some accounts are maybe missing or got deleted over time
    # we use this as a backup
    system.loc[1] = len(system.columns) * ['special_account']

    # for every account write a redis hash with the metainformation
    for row in system.iterrows():  # iterrow for 1000 rows is ok
        data = dict(row[1])  # shortcut we are not explicit
        r.hset(f"accountsystem:{data['Konto-Nummer']}", mapping=data)
    return True
