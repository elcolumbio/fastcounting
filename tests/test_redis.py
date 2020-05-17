"""Let's test the local redis installation."""
import redis

from fastcounting import helper

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)

mapping = {'hash1': '1', 'hash2': '2'}


def test_redis_installation():
    r.set('stringkey', 'stringvalue')
    assert r.get('stringkey') == 'stringvalue'
    assert r.delete('stringkey') == 1
    assert r.get('stringkey') is None


def test_pyredis_version():
    """Since by now its prerelease we do some ducktyping."""
    # HSET supports a mapping of key value pairs since v3.4.2.
    name = 'newstuff'
    r.hset(name, mapping=mapping)
    assert r.hgetall(name) == mapping


def test_lua_resp3():
    """Can we use named arrays in lua?"""
    lua_query = """
    redis.setresp(3)
    local map = redis.call('HGETALL', KEYS[1])['map']['hash2']
    return map
    """
    name = 'hashname'
    r.hset(name, mapping=mapping)
    lua_result = r.eval(lua_query, 1, name, 'hash2')
    assert lua_result == mapping['hash2']
