import os
import time
from uuid import uuid4

import pytest
from fastapi.encoders import jsonable_encoder
from redis import Redis

from ticketing.application.reservations import Reservations
from ticketing.domain import Failure
from ticketing.infrastructure.order_status_cache import RedisOrderStatusCache
from ticketing.workers import consume_event

pytestmark = pytest.mark.integration


def payment(svc, hold, actor="owner"):
    attempt = svc.initiate_payment(actor, hold['order_id'], str(uuid4()), 'SUCCEEDED', 0, 1)
    return {'callback_id': str(uuid4()), 'payment_id': attempt['payment_id'],
            'order_id': hold['order_id'], 'amount': hold['total'], 'currency': hold['currency'],
            'outcome': 'SUCCEEDED'}


@pytest.fixture
def cache_factory():
    url = os.getenv('TEST_REDIS_URL')
    if not url:
        pytest.skip('TEST_REDIS_URL not configured')
    redis = Redis.from_url(url, decode_responses=True)
    keys = []

    def build(actor, order_id, age_ms=3000):
        cache = RedisOrderStatusCache(redis, age_ms)
        keys.append(cache.key(actor, order_id))
        return cache

    yield build
    if keys:
        redis.delete(*keys)
    redis.close()


def test_actor_scoped_readthrough_ttl_and_mutation_authority(system, cache_factory):
    svc, db, event = system
    hold = svc.reserve('owner', event, ['A'], 'cached-hold')
    cache = cache_factory('owner', hold['order_id'], 300)
    cache_factory('other', hold['order_id'], 300)
    reader = Reservations(svc.store, cache)
    original_put = cache.put

    def check_released(*args):
        stats = db.pool.get_stats()
        assert stats['pool_available'] == stats['pool_size']
        return original_put(*args)

    cache.put = check_released
    first = reader.get_order('owner', hold['order_id'])
    assert cache.lookup('owner', hold['order_id'])[0]['status'] == 'PENDING'
    assert jsonable_encoder(reader.get_order('owner', hold['order_id'])) == jsonable_encoder(first)
    with pytest.raises(Failure, match='ORDER_NOT_FOUND'):
        reader.get_order('other', hold['order_id'])
    assert reader.callback(payment(reader, hold, actor='owner'))['status'] == 'book'
    assert reader.get_order('owner', hold['order_id'])['status'] == 'PENDING'
    ttl = cache.redis.pttl(cache.key('owner', hold['order_id']))
    time.sleep(max(0, ttl)/1000+0.05)
    paid = reader.get_order('owner', hold['order_id'])
    assert paid['status'] == 'PAID' and paid['tickets'] == []
    consume_event(db, None, {'event_id': str(uuid4()), 'schema_version': 1,
                            'event_type': 'OrderPaid', 'payload': {'order_id': hold['order_id']}})
    ttl = cache.redis.pttl(cache.key('owner', hold['order_id']))
    time.sleep(max(0, ttl)/1000+0.05)
    fulfilled = reader.get_order('owner', hold['order_id'])
    assert fulfilled['status'] == 'FULFILLED' and len(fulfilled['tickets']) == 1
    assert jsonable_encoder(cache.lookup('owner', hold['order_id'])[0]) == jsonable_encoder(fulfilled)


def test_delayed_fill_and_hits_do_not_extend_snapshot_lifetime(system, cache_factory):
    svc, _db, event = system
    hold = svc.reserve('owner', event, ['A'], 'age-bound')
    cache = cache_factory('owner', hold['order_id'], 200)
    _, stamp = cache.lookup('owner', hold['order_id'])
    row = svc.get_order('owner', hold['order_id'])
    time.sleep(0.06)
    cache.put('owner', hold['order_id'], row, stamp)
    key = cache.key('owner', hold['order_id'])
    before = cache.redis.pttl(key)
    assert 0 < before <= 150
    assert cache.lookup('owner', hold['order_id'])[0] is not None
    time.sleep(0.02)
    assert cache.lookup('owner', hold['order_id'])[0] is not None
    assert cache.redis.pttl(key) < before
    time.sleep(max(0, cache.redis.pttl(key))/1000+0.02)
    cache.put('owner', hold['order_id'], row, stamp)
    assert cache.lookup('owner', hold['order_id'])[0] is None


def test_old_concurrent_fill_cannot_replace_newer_snapshot(system, cache_factory):
    svc, db, event = system
    hold = svc.reserve('owner', event, ['A'], 'fill-race')
    cache = cache_factory('owner', hold['order_id'], 200)
    _, old_stamp = cache.lookup('owner', hold['order_id'])
    old = svc.get_order('owner', hold['order_id'])
    svc.callback(payment(svc, hold, actor='owner'))
    consume_event(db, None, {'event_id': str(uuid4()), 'schema_version': 1,
                            'event_type': 'OrderPaid', 'payload': {'order_id': hold['order_id']}})
    _, new_stamp = cache.lookup('owner', hold['order_id'])
    new = svc.get_order('owner', hold['order_id'])
    cache.put('owner', hold['order_id'], new, new_stamp)
    ttl = cache.redis.pttl(cache.key('owner', hold['order_id']))
    cache.put('owner', hold['order_id'], old, old_stamp)
    assert cache.lookup('owner', hold['order_id'])[0]['status'] == 'FULFILLED'
    assert cache.redis.pttl(cache.key('owner', hold['order_id'])) <= ttl
    time.sleep(max(0, ttl)/1000+0.03)
    cache.put('owner', hold['order_id'], old, old_stamp)
    assert cache.lookup('owner', hold['order_id'])[0] is None


def test_invalid_schema_and_future_timestamp_recover_from_postgres(system, cache_factory):
    svc, _db, event = system
    hold = svc.reserve('owner', event, ['A'], 'corrupt-cache')
    cache = cache_factory('owner', hold['order_id'])
    key = cache.key('owner', hold['order_id'])
    cache.redis.set(key, '{corrupt', px=3000)
    reader = Reservations(svc.store, cache)
    assert reader.get_order('owner', hold['order_id'])['status'] == 'PENDING'
    cache.redis.delete(key)
    _, stamp = cache.lookup('owner', hold['order_id'])
    cache.put('owner', hold['order_id'], svc.get_order('owner', hold['order_id']), stamp+5000)
    assert cache.lookup('owner', hold['order_id'])[0] is None
