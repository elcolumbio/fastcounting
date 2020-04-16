"""Queries and data processing classes you can use or get inspired by."""
import datetime as dt
import numpy as np
import pandas as pd
import redis

from . import helper, files

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)

# the idea behind the following lua script is good
# you can specify a range of accounts or all accounts and the complete query runs on redis
# that is very fast and a nice usecase
# before i added this we had a hash wich was easier to access but this thing is easier to rollback
# and faster.
# i am a lua beginner here is what i did
# first i looped over all values with scores (account number) in 'account:atomic'
# i made a unique array from all the scores or from the range of scores you specified
# second i loop over the same store again but now only one account a time
# i use the atomics from this second loop to lookup their amount in the according hash
# i sum everything together and create a new item in the list result for every account
# thirdly i concat the two lists from 1 and 2
# so you use the final list somehow like:
# sums = dict(response[middle:], zip(response[:middle]))

lua_sum = """
local accounts = {}
local hash = {}
for i, list in ipairs(redis.call(
    'ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], ARGV[3])) do
if i%2==0 and not hash[list] then
accounts[#accounts+1] = list
hash[list] = true
end
end

local result = {}
for idx, account in ipairs(accounts) do
result[idx] = 0
for i, atomic in pairs(redis.call(
    'ZRANGEBYSCORE', KEYS[1], account, account)) do
result[idx] = result[idx] + redis.call('HGET', 'atomicID:' .. atomic, 'amount')
end
end

for i=1,#accounts do
    result[#result+1] = accounts[i]
end
return result
"""

def sum_account(start_account, end_account):
    """Example how you can use the lua_sum script."""
    response = r.eval(lua_sum, 1, 'account:atomic', start_account, end_account, 'WITHSCORES')
    # like in this case you only want the sums and not the accountIDs
    middle = int(len(response)/2)
    sums = [response[:middle], response[middle:]]
    df = pd.DataFrame(sums).T
    # we prepare the dataframe for an outer join on validate
    df.columns = ['redis_amount', 'account']
    df = df.astype({'redis_amount': np.float, 'account': np.int})
    df['redis_amount'] *= 1/100

    df.set_index('account', inplace=True)
    return df

def total_diff(validate, redis_sum):
    """Check total sum which is aggregation of EB + Saldo Haben + Saldo Soll."""
    validate = validate.join(redis_sum, how='outer')
            
    validate['checksum'] =  validate['Saldo Haben'] - validate['Saldo Soll']
    validate = validate.round(decimals=2) # normally we dont need this since we save *100 in redis

    return validate[validate['checksum'] != validate['redis_amount']]

def main_total_diff(month, start_account=0, end_account=99999):
    """Connects read_summe and total diff, returns diff dataframe."""
    validate = files.main_summe(month)
    redis_sum = sum_account(start_account, end_account)
    return total_diff(validate, redis_sum)


# datefilter
def string_parser(start, end):
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