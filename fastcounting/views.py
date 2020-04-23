"""Here we create the 2 essential views: account:xy and atomicview."""
import datetime as dt
import importlib.resources as pkg_resources
import pandas as pd
import pathlib
import numpy as np
import redis

from fastcounting import helper, views, store, files, system, lua

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)

# lua scripts we import from static files under the lua folder.
lua_accounts = pkg_resources.read_text(lua, 'all_accounts.lua')

lua_account_views = pkg_resources.read_text(lua, 'account_views.lua')

lua_delete_account_views = lua_accounts + pkg_resources.read_text(
    lua, 'delete_account_views.lua')


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


lua_atomic_view = pkg_resources.read_text(lua, 'atomic_view.lua')


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