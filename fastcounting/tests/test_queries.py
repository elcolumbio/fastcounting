import redis

from fastcounting import helper

r = redis.Redis(**helper.Helper().rediscred, decode_responses=True)


def test_redis_installation():
    r.set('stringkey', 'stringvalue')
    assert r.get('stringkey') == 'stringvalue'
