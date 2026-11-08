import json
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import uuid4

import pytest
from redis.exceptions import TimeoutError

from ticketing.application.reservations import Reservations
from ticketing.config import Settings
from ticketing.domain import Failure
from ticketing.infrastructure.order_status_cache import RedisOrderStatusCache, scalar


def order(actor="owner"):
    return {"id": uuid4(), "actor": actor, "hold_id": uuid4(), "event_id": uuid4(), "total": 100,
            "currency": "THB", "status": "PENDING", "created_at": datetime.now(UTC), "tickets": []}


def envelope(row, start=1000, deadline=4000):
    return json.dumps({"schema_version": 1, "snapshot_start_ms": start,
                       "fresh_until_ms": deadline, "order": row}, default=scalar)


def test_cached_read_uses_no_durable_store_and_returns_same_json_shape():
    row = order()
    redis = Mock()
    redis.eval.return_value = [envelope(row), 2000, 2000, 0]
    store = Mock()
    cache = RedisOrderStatusCache(redis, 3000)
    result = Reservations(store, cache).get_order("owner", row['id'])
    assert result == json.loads(json.dumps(row, default=scalar))
    store.get_order.assert_not_called()
    assert 'owner' not in cache.key('owner', row['id'])
    assert cache.key('owner', row['id']) != cache.key('other', row['id'])


def test_redis_read_failure_falls_back_without_retry_or_fill():
    row, redis, store = order(), Mock(), Mock()
    redis.eval.side_effect = TimeoutError('test outage')
    store.get_order.return_value = row
    assert Reservations(store, RedisOrderStatusCache(redis, 3000)).get_order('owner', row['id']) is row
    assert redis.eval.call_count == 1
    store.get_order.assert_called_once_with('owner', row['id'])


def test_cache_fill_failure_does_not_change_successful_read():
    row, redis, store = order(), Mock(), Mock()
    redis.eval.side_effect = [['', 1000, -2, 0], TimeoutError('fill failed')]
    store.get_order.return_value = row
    assert Reservations(store, RedisOrderStatusCache(redis, 3000)).get_order('owner', row['id']) is row
    assert redis.eval.call_count == 2


def test_failed_authorization_never_fills_cache():
    row, redis, store = order(), Mock(), Mock()
    redis.eval.return_value = ['', 1000, -2, 0]
    store.get_order.side_effect = Failure('ORDER_NOT_FOUND', 404)
    with pytest.raises(Failure, match='ORDER_NOT_FOUND'):
        Reservations(store, RedisOrderStatusCache(redis, 3000)).get_order('other', row['id'])
    assert redis.eval.call_count == 1


@pytest.mark.parametrize('mutate', [
    lambda p: p['order'].update(actor='other'),
    lambda p: p['order'].update(id=str(uuid4())),
    lambda p: p['order'].update(status='UNKNOWN'),
    lambda p: p['order'].update(total=True),
    lambda p: p['order'].update(tickets=[{'id': str(uuid4()), 'seat_id': 'A'}]),
    lambda p: p['order'].update(created_at='2026-10-03T00:00:00'),
    lambda p: p.update(schema_version=True),
    lambda p: p.update(snapshot_start_ms=2500),
    lambda p: p.update(fresh_until_ms=5000),
    lambda p: p.update(fresh_until_ms=1500),
])
def test_invalid_cached_snapshots_are_not_returned(mutate):
    row, redis = order(), Mock()
    payload = json.loads(envelope(row))
    mutate(payload)
    redis.eval.return_value = [json.dumps(payload), 2000, 2000, 0]
    cached, stamp = RedisOrderStatusCache(redis, 3000).lookup('owner', row['id'])
    assert cached is None and stamp == 2000


@pytest.mark.parametrize('ttl', [-1, 0, 2001])
def test_nonexpiring_or_extended_ttl_is_a_miss(ttl):
    row, redis = order(), Mock()
    redis.eval.return_value = [envelope(row), 2000, ttl, 0]
    assert RedisOrderStatusCache(redis, 3000).lookup('owner', row['id'])[0] is None


@pytest.mark.parametrize('raw,oversize', [('not json', 0), ('', 1)])
def test_corrupt_or_oversize_data_never_reaches_caller(raw, oversize):
    redis = Mock()
    redis.eval.return_value = [raw, 2000, 2000, oversize]
    assert RedisOrderStatusCache(redis, 3000).lookup('owner', uuid4())[0] is None


def test_mixed_paid_ticket_snapshot_is_not_cached():
    row, redis = order(), Mock()
    row.update(status='PAID', tickets=[{'id': uuid4(), 'seat_id': 'A'}])
    RedisOrderStatusCache(redis, 3000).put('owner', row['id'], row, 1000)
    redis.eval.assert_not_called()


@pytest.mark.parametrize('value', [0, 1, 3000])
def test_bounded_cache_settings_accept_disabled_and_candidate(value):
    replace(Settings(), order_status_cache_ms=value).validate()


@pytest.mark.parametrize('value', [-1, 3001])
def test_cache_setting_rejects_unbounded_staleness(value):
    with pytest.raises(RuntimeError, match='ORDER_STATUS_CACHE_MS'):
        replace(Settings(), order_status_cache_ms=value).validate()


def test_default_disabled_reader_never_touches_cache():
    row, store = order(), Mock()
    store.get_order.return_value = row
    assert Reservations(store).get_order('owner', row['id']) is row
    store.get_order.assert_called_once()
