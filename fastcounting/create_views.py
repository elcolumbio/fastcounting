"""Queries and data processing classes you can use or get inspired by."""
import datetime as dt
import pandas as pd
import redis

from . import helper, read_files

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)

# total diff, mainly to validate our handling of all accounting math.
lua_total = """
local result = 0.0
for l, atomic in ipairs(redis.call('SMEMBERS', KEYS[1]))
do result = result + redis.call('HGET', 'atomicID:' .. atomic, 'amount')
end
return result
"""

def total_diff(validate, lua_script):
    """Check total sum which is aggregation of EB + Saldo Haben + Saldo Soll."""
    redis_sum_list = []
    for konto in validate.index:
        redis_sum_list.append(
            r.eval(lua_script, 1, f'account:atomic:{float(konto)}'))
    validate['test_redis'] = redis_sum_list
    validate['test_redis'] = validate['test_redis'] / 100
            
    validate['checksum'] =  validate['Saldo Haben'] - validate['Saldo Soll']
    return validate[validate['checksum'] != validate['test_redis']]

def main_total_diff(month):
    """Connects read_summe and total diff."""
    validate = read_files.main_summe(month)
    total_diff(validate, month)


# datefilter
def query_atomic_date(start_date, end_date):
    """Expects timestamp integers, returns list of atomicIDs, Timestamp pairs."""
    return r.zrangebyscore('atomic:date', start_date, end_date, withscores=True)


def query_atomics(atomic_list):
    result_list = []
    for atomic in atomics:
        row = r.hgetall(f'atomicID:{atomic[0]}')
        row.update({'Buchungsdatum':dt.datetime.fromtimestamp(atomic[1])})
        result_list.append(row)
    return pd.DataFrame(result_list)
    