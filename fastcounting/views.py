"""Here we create the 2 essential views: account:xy and atomicview."""
import datetime as dt
import pandas as pd
import numpy as np
import redis

from fastcounting import helper, views, store, files, system

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)

lua_accounts = """
redis.setresp(3)
local accounts = {}
local hash = {}
for i, value in pairs(redis.call(
    'ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], ARGV[3])) do
    local account = value[2]['double']
    if not hash[account] then
        accounts[#accounts+1] = account
        hash[account] = true
    end
end
if ARGV[4] == nil then return accounts end
"""


lua_delete_account_views = lua_accounts + """
for i, account in pairs(accounts) do
    redis.call('DEL', 'account:' .. account)
end
return true
"""


lua_account_views = """
redis.setresp(3)
for i, atomicID in pairs(redis.call(
    'ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])) do

    local atomic = redis.call('HGETALL', 'atomicID:' .. atomicID)['map']
    local general = redis.call('HGETALL', 'generalID:' .. atomic['generalID'])['map']
    local system = redis.call('HGETALL', 'accountsystem:' .. atomic['accountID'])['map']
    if next(system)==nil then
        system = redis.call('HGETALL', 'accountsystem:special_account')['map'] end
    
    redis.call('XADD','account:' .. atomic['accountID'],
        general['date'] .. '-' .. i, 
        'general', atomic['generalID'],
        'date', general['date'],
        'jourdate', general['jourdat'],
        'status', general['status'],
        'text', atomic['text'],
        'kontenseite', atomic['kontenseite'],
        'amount', atomic['amount'],
        'account', atomic['accountID'],
        'batchID', atomic['batchID'],
        'account_name', system['Kontenbezeichnung'],
        'system_kat', system['Kontenkategorie'],
        'system_type', system['Kontenunterart'],
        'system_tax', system['Steuer']
        )
end
return true
"""


def create_account_views():
    """We create a stream for every account: e.g.: account:4400 ."""
    response = r.eval(
        lua_account_views, 1, 'atomic:date',
        0, dt.datetime(2018, 1, 2).timestamp())
    return response


def delete_account_views():
    """Delete all account views in one run."""
    response = r.eval(
        lua_delete_account_views, 1, 'account:atomic',
        0, 9999999, 'WITHSCORES', 'dont')
    return response


lua_atomic_view = """
redis.setresp(3)
for i, atomicID in pairs(redis.call(
    'ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])) do
    local atomic = redis.call('HGETALL', 'atomicID:' .. atomicID)['map']
    local general = redis.call('HGETALL', 'generalID:' .. atomic['generalID'])['map']
    local system = redis.call('HGETALL', 'accountsystem:' .. atomic['accountID'])['map']
    if next(system)==nil then
        system = redis.call('HGETALL', 'accountsystem:special_account')['map'] end

    
    local inwords = {}
    local haben = {}
    for i, atomicx in pairs(redis.call(
        'ZRANGEBYSCORE', 'general:atomic', atomic['generalID'], atomic['generalID'])) do
        local inatomic = redis.call('HGETALL', 'atomicID:' .. atomicx)['map']
        local part = inatomic['accountID'] .. ':' .. inatomic['amount']/100 .. '€'
        if inatomic['kontenseite'] == 'Soll' then
            inwords[#inwords+1] = part
        else
            haben[#haben+1] = part
        end
    end

    haben[#haben] = '### ' .. haben[#haben]
    for i=1, #haben do
        inwords[#inwords+1] = haben[i]
    end
    local relation = table.concat(inwords, ', ')


    redis.call('XADD','atomicview',
        general['date'] .. '-' .. i, 
        'general', atomic['generalID'],
        'date', general['date'],
        'jourdate', general['jourdat'],
        'status', general['status'],
        'text', atomic['text'],
        'kontenseite', atomic['kontenseite'],
        'amount', atomic['amount'],
        'account', atomic['accountID'],
        'batchID', atomic['batchID'],
        'account_name', system['Kontenbezeichnung'],
        'system_kat', system['Kontenkategorie'],
        'system_type', system['Kontenunterart'],
        'system_tax', system['Steuer'],
        'relations', relation
        )
end
return true
"""


def create_atomic_view():
    """We create one stream with all atomics stacked by date."""
    response = r.eval(
        lua_atomic_view, 1, 'atomic:date',
        0, dt.datetime(2018, 1, 2).timestamp())
    return response


def delete_atomic_view():
    """That's easy just for completeness."""
    return r.delete('atomicview')


def return_all_accounts():
    """Returns a list of all unique accounts with transactions."""
    return r.eval(
        lua_accounts, 1, 'account:atomic',
        0, 9999999, 'WITHSCORES')