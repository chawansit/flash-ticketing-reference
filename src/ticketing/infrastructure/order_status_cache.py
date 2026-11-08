"""Advisory actor-scoped order snapshots; never a booking/payment authority."""

import hashlib
import json
from datetime import datetime
from uuid import UUID

from redis.exceptions import RedisError

from ticketing.observability import ORDER_STATUS_CACHE

MAX_BYTES = 16384
LOOKUP = """
local stamp = redis.call('TIME')
local now = tonumber(stamp[1])*1000 + math.floor(tonumber(stamp[2])/1000)
local raw = redis.call('GET',KEYS[1]) or ''
local ttl = redis.call('PTTL',KEYS[1])
if string.len(raw) > tonumber(ARGV[1]) then return {'',now,ttl,1} end
return {raw,now,ttl,0}
"""
FILL = """
local stamp = redis.call('TIME')
local now = tonumber(stamp[1])*1000 + math.floor(tonumber(stamp[2])/1000)
local start = tonumber(ARGV[2])
local deadline = tonumber(ARGV[3])
local remaining = deadline-now
if now < start or remaining <= 0 or remaining > tonumber(ARGV[4]) then return -1 end
if redis.call('SET',KEYS[1],ARGV[1],'NX','PX',remaining) then return 1 end
return 0
"""
PUBLISH = """
local stamp = redis.call('TIME')
local now = tonumber(stamp[1])*1000 + math.floor(tonumber(stamp[2])/1000)
local start = tonumber(ARGV[2])
local deadline = tonumber(ARGV[3])
local remaining = deadline-now
if now < start or remaining <= 0 or remaining > tonumber(ARGV[4]) then return -1 end
local old = redis.call('GET',KEYS[1])
if old and string.len(old) <= tonumber(ARGV[5]) then
    local ok, payload = pcall(cjson.decode,old)
    local ttl = redis.call('PTTL',KEYS[1])
    if ok and type(payload) == 'table' and payload.schema_version == 1
       and type(payload.snapshot_start_ms) == 'number' and type(payload.fresh_until_ms) == 'number'
       and payload.snapshot_start_ms <= now and payload.snapshot_start_ms >= start
       and payload.fresh_until_ms > now and ttl > 0 and ttl <= payload.fresh_until_ms-now
       and payload.fresh_until_ms-payload.snapshot_start_ms <= tonumber(ARGV[4])
       then return 0 end
end
redis.call('SET',KEYS[1],ARGV[1],'PX',remaining)
return 1
"""
STATUSES = {'PENDING', 'FAILED', 'EXPIRED', 'REFUND_PENDING', 'REFUNDED', 'PAID', 'FULFILLED'}
FIELDS = {'id', 'actor', 'hold_id', 'event_id', 'total', 'currency', 'status', 'created_at', 'tickets'}


def scalar(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    raise TypeError('Unsupported order snapshot value')


def valid_order(row, actor, order_id):
    try:
        if not isinstance(row, dict) or set(row) != FIELDS:
            return False
        if row['actor'] != actor or row['id'] != str(order_id):
            return False
        for name in ('id', 'hold_id', 'event_id'):
            UUID(row[name])
        if type(row['total']) is not int or row['total'] < 0:
            return False
        if not isinstance(row['currency'], str) or len(row['currency']) != 3:
            return False
        if row['status'] not in STATUSES or datetime.fromisoformat(row['created_at']).tzinfo is None:
            return False
        tickets = row['tickets']
        if not isinstance(tickets, list) or len(tickets) > 8:
            return False
        if row['status'] == 'FULFILLED':
            if not tickets:
                return False
        elif tickets:
            return False
        ids, seats = set(), []
        for ticket in tickets:
            if not isinstance(ticket, dict) or set(ticket) != {'id', 'seat_id'}:
                return False
            UUID(ticket['id'])
            seat = ticket['seat_id']
            if not isinstance(seat, str) or not 1 <= len(seat) <= 64:
                return False
            ids.add(ticket['id'])
            seats.append(seat)
        return len(ids) == len(tickets) and seats == sorted(set(seats))
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


class RedisOrderStatusCache:
    def __init__(self, redis, max_age_ms):
        if not 1 <= max_age_ms <= 3000:
            raise ValueError('Order status max age must be 1..3000 ms')
        self.redis, self.max_age_ms = redis, max_age_ms

    @staticmethod
    def key(actor, order_id):
        owner = hashlib.sha256(actor.encode()).hexdigest()
        return f'order-status:v1:{{{order_id}}}:{owner}'

    def lookup(self, actor, order_id):
        try:
            raw, now, ttl, oversize = self.redis.eval(LOOKUP, 1, self.key(actor, order_id), MAX_BYTES)
            now, ttl = int(now), int(ttl)
            if oversize:
                ORDER_STATUS_CACHE.labels('invalid').inc()
                return None, now
            if not raw:
                ORDER_STATUS_CACHE.labels('miss').inc()
                return None, now
            payload = json.loads(raw)
            if (not isinstance(payload, dict) or type(payload.get('schema_version')) is not int
                    or payload['schema_version'] != 1
                    or not valid_order(payload.get('order'), actor, order_id)):
                ORDER_STATUS_CACHE.labels('invalid').inc()
                return None, now
            start, deadline = payload.get('snapshot_start_ms'), payload.get('fresh_until_ms')
            if (type(start) is not int or type(deadline) is not int or start > now
                    or not 0 < deadline-now <= self.max_age_ms
                    or not 0 < deadline-start <= self.max_age_ms
                    or not 0 < ttl <= deadline-now):
                ORDER_STATUS_CACHE.labels('stale').inc()
                return None, now
            ORDER_STATUS_CACHE.labels('hit').inc()
            return payload['order'], None
        except RedisError:
            ORDER_STATUS_CACHE.labels('redis_error').inc()
            return None, None
        except (ValueError, TypeError, KeyError):
            ORDER_STATUS_CACHE.labels('invalid').inc()
            return None, None

    def put(self, actor, order_id, row, snapshot_start_ms):
        try:
            raw = json.dumps({'schema_version': 1, 'snapshot_start_ms': snapshot_start_ms,
                              'fresh_until_ms': snapshot_start_ms+self.max_age_ms,
                              'order': row}, default=scalar, separators=(',', ':'))
            if len(raw.encode()) > MAX_BYTES or not valid_order(json.loads(raw)['order'], actor, order_id):
                ORDER_STATUS_CACHE.labels('fill_skipped').inc()
                return
            result = self.redis.eval(FILL, 1, self.key(actor, order_id), raw,
                                     snapshot_start_ms, snapshot_start_ms+self.max_age_ms, self.max_age_ms)
            ORDER_STATUS_CACHE.labels({1: 'fill', 0: 'fill_raced', -1: 'fill_stale'}[int(result)]).inc()
        except RedisError:
            ORDER_STATUS_CACHE.labels('redis_error').inc()
        except (ValueError, TypeError, KeyError):
            ORDER_STATUS_CACHE.labels('fill_skipped').inc()

    def snapshot_start(self):
        seconds, micros = self.redis.time()
        return int(seconds)*1000 + int(micros)//1000

    def publish(self, actor, order_id, row, snapshot_start_ms):
        """Replace only an older advisory snapshot after a fresh committed read."""
        try:
            if type(snapshot_start_ms) is not int:
                raise ValueError('Invalid snapshot timestamp')
            raw = json.dumps({'schema_version': 1, 'snapshot_start_ms': snapshot_start_ms,
                              'fresh_until_ms': snapshot_start_ms+self.max_age_ms,
                              'order': row}, default=scalar, separators=(',', ':'))
            if len(raw.encode()) > MAX_BYTES or not valid_order(json.loads(raw)['order'], actor, order_id):
                ORDER_STATUS_CACHE.labels('event_fill_skipped').inc()
                return False
            result = self.redis.eval(PUBLISH, 1, self.key(actor, order_id), raw,
                                     snapshot_start_ms, snapshot_start_ms+self.max_age_ms,
                                     self.max_age_ms, MAX_BYTES)
            ORDER_STATUS_CACHE.labels({1: 'event_fill', 0: 'event_fill_raced',
                                       -1: 'event_fill_stale'}[int(result)]).inc()
            return int(result) == 1
        except RedisError:
            ORDER_STATUS_CACHE.labels('redis_error').inc()
        except (ValueError, TypeError, KeyError):
            ORDER_STATUS_CACHE.labels('event_fill_skipped').inc()
        return False
