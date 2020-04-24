"""
Queries and data processing classes you can use or get inspired by.
For more examples and help we have a notebook called lua_examples.
"""
import importlib.resources as pkg_resources
import numpy as np
import pandas as pd
import redis

from fastcounting import helper, files, lua

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)

# lua scripts we import from our static lua script folder lua.
lua_sum = pkg_resources.read_text(lua, 'sum_accounts.lua')


def sum_account(start_account, end_account):
    """Example how you can use the lua_sum script."""
    response = r.eval(lua_sum, 1, 'account:atomic', start_account, end_account, 'WITHSCORES')
    # like in this case you only want the sums and not the accountIDs
    middle = int(len(response)/2)
    sums = [response[:middle], response[middle:]]
    df = pd.DataFrame(sums).T
    # we prepare the dataframe for an outer join on validate
    df.columns = ['account', 'redis_amount']
    df = df.astype({'redis_amount': np.float, 'account': np.int})
    df['redis_amount'] *= 1/100

    df.set_index('account', inplace=True)
    return df


def total_diff(validate, redis_sum):
    """Check total sum which is aggregation of EB + Saldo Haben + Saldo Soll."""
    validate = validate.join(redis_sum, how='outer')

    validate['checksum'] = validate['Saldo Soll'] - validate['Saldo Haben']
    # if we combine soll and haben we have to respect passive accounts like this
    validate.loc[validate.index >= 2900, 'checksum'] *= (-1)
    validate = validate.round(decimals=2)  # normally we dont need this since we save *100 in redis

    return validate[validate['checksum'] != validate['redis_amount']]


def main_total_diff(month, start_account=0, end_account=99999):
    """Connects read_summe and total diff, returns diff dataframe."""
    validate = files.main_summe(month)
    redis_sum = sum_account(start_account, end_account)
    return total_diff(validate, redis_sum)
