import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from ticketing.domain import persistent_uuid as uuid4

from redis.exceptions import RedisError, ResponseError

from ticketing.domain import Failure
from ticketing.observability import RESERVATION_INTAKE, RESERVATION_REPLICA_ACKS

ENQUEUE = """
local map = KEYS[1]
local idem = KEYS[2]
local command = KEYS[3]
local stream = KEYS[4]
local expiry = KEYS[5]
local delta = KEYS[6]
local meta = redis.call('HMGET',map,'version','updating','sale_starts_epoch','sale_ends_epoch','currency')
if not meta[1] or meta[2] or not meta[3] or not meta[4] or not meta[5] then return {-1} end
local now = tonumber(redis.call('TIME')[1])
if now < tonumber(meta[3]) or now >= tonumber(meta[4]) then return {-2} end
local previous_hash = redis.call('HGET',idem,'request_hash')
if previous_hash then
  if previous_hash ~= ARGV[1] then return {-3} end
  return {2,redis.call('HGET',idem,'response')}
end
if redis.call('XLEN',stream) >= tonumber(ARGV[2]) then return {-4} end
local max_age = tonumber(ARGV[3])
if max_age > 0 then
  local oldest = redis.call('XRANGE',stream,'-','+','COUNT',1)
  if oldest[1] then
    local separator = string.find(oldest[1][1],'-')
    local oldest_seconds = math.floor(tonumber(string.sub(oldest[1][1],1,separator-1)) / 1000)
    if now - oldest_seconds >= max_age then return {-7} end
  end
end
local seat_ids = cjson.decode(ARGV[4])
local selected = {}
local total = 0
for _,seat_id in ipairs(seat_ids) do
  local field = 'seat:'..seat_id
  local raw = redis.call('HGET',map,field)
  if not raw then return {-5} end
  local seat = cjson.decode(raw)
  local expired = seat.status == 'HELD' and tonumber(seat.reserved_until_epoch or 0) <= now
  if seat.status == 'SOLD' or (seat.status == 'HELD' and not expired) then return {-6} end
  total = total + tonumber(seat.price)
  table.insert(selected,{field=field,seat=seat})
end
local response = cjson.decode(ARGV[5])
response.total = total
response.currency = meta[5]
response.expires_at_epoch = now + tonumber(ARGV[6])
response.expires_at = ARGV[7]
response.persistence_status = 'PENDING'
local response_json = cjson.encode(response)
local version = tonumber(meta[1])
local prior_version = version
local changed = {}
for _,entry in ipairs(selected) do
  local seat = entry.seat
  seat.status = 'HELD'
  seat.hold_id = response.hold_id
  seat.reserved_until = response.expires_at
  seat.reserved_until_epoch = response.expires_at_epoch
  version = version + 1
  seat.version = version
  redis.call('HSET',map,entry.field,cjson.encode(seat))
  table.insert(changed,seat)
end
redis.call('HSET',map,'version',version)
if #changed > 0 then
  local delta_entry = cjson.encode({from_version=prior_version,version=version,seats=changed})
  redis.call('ZADD',delta,version,delta_entry)
  local excess = redis.call('ZCARD',delta) - tonumber(ARGV[10])
  if excess > 0 then redis.call('ZREMRANGEBYRANK',delta,0,excess-1) end
  local map_ttl = redis.call('TTL',map)
  local delta_ttl = redis.call('TTL',delta)
  if map_ttl > 0 and (delta_ttl < 0 or delta_ttl > map_ttl) then
    redis.call('EXPIRE',delta,map_ttl)
  end
end
redis.call('HSET',idem,'request_hash',ARGV[1],'response',response_json)
redis.call('EXPIRE',idem,tonumber(ARGV[8]))
redis.call('HSET',command,'status','PENDING','response',response_json,'payload',ARGV[9],
  'created_at_epoch',now)
redis.call('EXPIRE',command,tonumber(ARGV[8]))
redis.call('ZADD',expiry,response.expires_at_epoch,response.command_id)
redis.call('XADD',stream,'*','command_id',response.command_id,'payload',ARGV[9],'response',response_json,'created_at_epoch',now)
return {1,response_json}
"""

MARK_DURABLE = """
local status = redis.call('HGET',KEYS[1],'status')
if not status then return 0 end
if status == 'FAILED' then return -1 end
if status == 'PENDING' then
  redis.call('HSET',KEYS[1],'status','DURABLE')
end
return 1
"""

MARK_FAILED = """
local command = KEYS[1]
local map = KEYS[2]
local expiry = KEYS[3]
local delta = KEYS[4]
local status = redis.call('HGET',command,'status')
if not status then return 0 end
if status == 'DURABLE' then return -1 end
local response = cjson.decode(redis.call('HGET',command,'response'))
local version = tonumber(redis.call('HGET',map,'version') or 0)
local prior_version = version
local changed = {}
for _,seat_id in ipairs(cjson.decode(ARGV[1])) do
  local field = 'seat:'..seat_id
  local raw = redis.call('HGET',map,field)
  if raw then
    local seat = cjson.decode(raw)
    if seat.hold_id == response.hold_id then
      seat.status = 'AVAILABLE'
      seat.hold_id = nil
      seat.reserved_until = cjson.null
      seat.reserved_until_epoch = cjson.null
      version = version + 1
      seat.version = version
      redis.call('HSET',map,'version',version,field,cjson.encode(seat))
      table.insert(changed,seat)
    end
  end
end
if #changed > 0 then
  local delta_entry = cjson.encode({from_version=prior_version,version=version,seats=changed})
  redis.call('ZADD',delta,version,delta_entry)
  local excess = redis.call('ZCARD',delta) - tonumber(ARGV[3])
  if excess > 0 then redis.call('ZREMRANGEBYRANK',delta,0,excess-1) end
  local map_ttl = redis.call('TTL',map)
  local delta_ttl = redis.call('TTL',delta)
  if map_ttl > 0 and (delta_ttl < 0 or delta_ttl > map_ttl) then
    redis.call('EXPIRE',delta,map_ttl)
  end
end
redis.call('ZREM',expiry,response.command_id)
redis.call('HSET',command,'status','FAILED','error_code',ARGV[2])
return 1
"""


class RedisReservationIntake:
    """Atomic provisional ownership and durable-command intake for one event hash slot."""

    group = "reservation-writers"

    def __init__(
        self,
        cache,
        *,
        hold_seconds=120,
        replica_acks=0,
        wait_ms=100,
        max_backlog=10000,
        max_command_age_seconds=0,
        retention_seconds=86400,
        stream_batch_size=32,
        stream_refresh_seconds=1.0,
        stream_scan_refresh_seconds=60.0,
        stream_scan_steps=4,
    ):
        if stream_batch_size <= 0:
            raise ValueError("Stream batch size must be positive")
        if stream_refresh_seconds < 0 or stream_scan_refresh_seconds < 0:
            raise ValueError("Stream refresh intervals must be nonnegative")
        if stream_scan_steps <= 0:
            raise ValueError("Stream scan steps must be positive")
        self.cache = cache
        self.redis = cache.redis
        self.hold_seconds = hold_seconds
        self.replica_acks = replica_acks
        self.wait_ms = wait_ms
        self.max_backlog = max_backlog
        self.max_command_age_seconds = max_command_age_seconds
        self.retention_seconds = retention_seconds
        self.stream_batch_size = stream_batch_size
        self.stream_refresh_seconds = stream_refresh_seconds
        self.stream_scan_refresh_seconds = stream_scan_refresh_seconds
        self.stream_scan_steps = stream_scan_steps
        self._streams = []
        self._stream_cursor = 0
        self._stream_window_cursor = 0
        self._stream_cycle_complete = True
        self._scan_cursor = 0
        self._last_stream_refresh = 0.0
        self._last_scan_refresh = 0.0
        self._known_groups = set()

    @staticmethod
    def _tag(event):
        return "{" + str(event) + "}"

    @classmethod
    def stream_key(cls, event):
        return f"reservation-stream:{cls._tag(event)}"

    @classmethod
    def command_key(cls, event, command_id):
        return f"reservation-command:{cls._tag(event)}:{command_id}"

    @classmethod
    def expiry_key(cls, event):
        return f"reservation-expiry:{cls._tag(event)}"

    @classmethod
    def idem_key(cls, event, actor, key):
        identity = hashlib.sha256(f"{actor}\0{key}".encode()).hexdigest()
        return f"reservation-idem:{cls._tag(event)}:{identity}"

    def enqueue(self, actor, event_id, seat_ids, key):
        event = str(event_id)
        seats = sorted(set(seat_ids))
        request = {"event_id": event, "seats": seats}
        request_hash = hashlib.sha256(
            json.dumps(request, sort_keys=True, default=str).encode()
        ).hexdigest()
        command_id, hold_id, order_id = str(uuid4()), str(uuid4()), str(uuid4())
        expires = datetime.now(UTC) + timedelta(seconds=self.hold_seconds)
        response = {
            "command_id": command_id,
            "hold_id": hold_id,
            "order_id": order_id,
            "seats": seats,
        }
        payload = {
            "command_id": command_id,
            "actor": actor,
            "idempotency_key": key,
            "request_hash": request_hash,
            "event_id": event,
            "seat_ids": seats,
            "hold_id": hold_id,
            "order_id": order_id,
            "expires_at": expires.isoformat(),
        }
        keys = (
            self.cache.key(event),
            self.idem_key(event, actor, key),
            self.command_key(event, command_id),
            self.stream_key(event),
            self.expiry_key(event),
            self.cache.delta_key(event),
        )
        try:
            arguments = (
                ENQUEUE,
                len(keys),
                *keys,
                request_hash,
                self.max_backlog,
                self.max_command_age_seconds,
                json.dumps(seats),
                json.dumps(response),
                self.hold_seconds,
                expires.isoformat(),
                self.retention_seconds,
                json.dumps(payload),
                self.cache.delta_history_entries,
            )
            acknowledged = None
            if self.replica_acks:
                # Redis WAIT applies to writes issued on the same connection. This pipeline
                # pins EVAL and WAIT to one connection and Redis executes them in order.
                pipe = self.redis.pipeline(transaction=False)
                pipe.eval(*arguments)
                pipe.wait(self.replica_acks, self.wait_ms)
                result, acknowledged = pipe.execute()
            else:
                result = self.redis.eval(*arguments)
            code = int(result[0])
            if code in {1, 2}:
                stored = json.loads(result[1])
                if code == 2:
                    stored = self.status(event, stored["command_id"])
                if acknowledged is not None:
                    RESERVATION_REPLICA_ACKS.observe(acknowledged)
                    if acknowledged < self.replica_acks:
                        RESERVATION_INTAKE.labels("durability_unknown").inc()
                        raise Failure("RESERVATION_DURABILITY_UNKNOWN", 503)
                # Registry is advisory. An accepted command must not become a 503 if
                # this optimization fails; stream scanning is the recovery path.
                try:
                    self.redis.sadd("reservation-stream-registry", self.stream_key(event))
                except RedisError:
                    RESERVATION_INTAKE.labels("registry_error").inc()
                RESERVATION_INTAKE.labels("accepted" if code == 1 else "replay").inc()
                return stored
        except Failure:
            raise
        except RedisError as exc:
            # Once a write has been dispatched, a connection failure cannot prove
            # whether the atomic Lua operation ran. The only safe recovery is an
            # identical replay using the same actor and idempotency key.
            RESERVATION_INTAKE.labels("redis_error").inc()
            RESERVATION_INTAKE.labels("durability_unknown").inc()
            raise Failure("RESERVATION_DURABILITY_UNKNOWN", 503) from exc
        failures = {
            -1: ("SEATMAP_WARMING", 503),
            -2: ("SALE_CLOSED", 409),
            -3: ("IDEMPOTENCY_MISMATCH", 409),
            -4: ("RESERVATION_BACKLOG_FULL", 503),
            -5: ("SEAT_NOT_FOUND", 404),
            -6: ("SEAT_UNAVAILABLE", 409),
            -7: ("RESERVATION_PERSISTENCE_LAGGING", 503),
        }
        name, status = failures.get(code, ("ADMISSION_UNAVAILABLE", 503))
        RESERVATION_INTAKE.labels(name.lower()).inc()
        raise Failure(name, status)

    def status(self, event_id, command_id, actor=None):
        try:
            row = self.redis.hgetall(self.command_key(event_id, command_id))
        except RedisError as exc:
            RESERVATION_INTAKE.labels("redis_error").inc()
            raise Failure("ADMISSION_UNAVAILABLE", 503) from exc
        if not row:
            raise Failure("RESERVATION_COMMAND_NOT_FOUND", 404)
        if actor is not None:
            payload = json.loads(row["payload"])
            if payload["actor"] != actor:
                raise Failure("RESERVATION_COMMAND_NOT_FOUND", 404)
        response = json.loads(row["response"])
        response["persistence_status"] = row["status"]
        if "error_code" in row:
            response["error_code"] = row["error_code"]
        return response

    def _refresh_streams(self, limit=5000):
        """Refresh one fair window without abandoning a partially polled window."""
        now = time.monotonic()
        if self._streams and not self._stream_cycle_complete:
            return
        if now - self._last_stream_refresh < self.stream_refresh_seconds:
            return
        keys = set(self._streams)
        keys.update(self.redis.smembers("reservation-stream-registry"))
        if now - self._last_scan_refresh >= self.stream_scan_refresh_seconds:
            for _ in range(self.stream_scan_steps):
                self._scan_cursor, found = self.redis.scan(
                    self._scan_cursor, match="reservation-stream:*", count=100
                )
                keys.update(found)
                if self._scan_cursor == 0:
                    break
            self._last_scan_refresh = now
        ordered = sorted(keys)
        if ordered:
            size = min(limit, len(ordered))
            start = self._stream_window_cursor % len(ordered)
            candidates = [ordered[(start + offset) % len(ordered)] for offset in range(size)]
            pipe = self.redis.pipeline(transaction=False)
            for stream in candidates:
                pipe.xlen(stream)
            lengths = pipe.execute()
            empty = [stream for stream, length in zip(candidates, lengths, strict=True) if length == 0]
            revived = self._prune_empty_streams(empty) if empty else []
            eligible = set(revived)
            eligible.update(
                stream for stream, length in zip(candidates, lengths, strict=True) if length > 0
            )
            self._streams = [stream for stream in candidates if stream in eligible]
            self._stream_window_cursor = (start + size) % len(ordered)
            self._stream_cursor = 0
            self._stream_cycle_complete = not self._streams
        else:
            self._streams = []
            self._stream_cursor = 0
            self._stream_window_cursor = 0
            self._stream_cycle_complete = True
        self._last_stream_refresh = now

    def _prune_empty_streams(self, empty):
        """Recheck after removal so stale XLEN results cannot erase new registration."""
        try:
            self.redis.srem("reservation-stream-registry", *empty)
        except RedisError:
            RESERVATION_INTAKE.labels("registry_error").inc()

        # Enqueue writes XADD before SADD. Work arriving before this read is restored;
        # work arriving after a zero read registers itself after the earlier SREM.
        # Keep operations separate: event streams and the registry use different slots.
        try:
            pipe = self.redis.pipeline(transaction=False)
            for stream in empty:
                pipe.xlen(stream)
            lengths = pipe.execute()
            revived = [
                stream for stream, length in zip(empty, lengths, strict=True) if length > 0
            ]
        except RedisError:
            RESERVATION_INTAKE.labels("registry_error").inc()
            revived = list(empty)  # A failed read cannot prove a stream is empty.

        if revived:
            try:
                self.redis.sadd("reservation-stream-registry", *revived)
            except RedisError:
                RESERVATION_INTAKE.labels("registry_error").inc()
        return revived

    def streams(self, limit=5000):
        self._refresh_streams(limit)
        return list(self._streams)

    def _next_stream_batch(self):
        if not self._streams:
            return []
        size = min(self.stream_batch_size, len(self._streams))
        start = self._stream_cursor % len(self._streams)
        return [self._streams[(start + offset) % len(self._streams)] for offset in range(size)]

    def _advance_stream_cursor(self, visited):
        """Continue after actual service, not the advisory pending-check window."""
        next_cursor = self._stream_cursor + visited
        self._stream_cursor = next_cursor % len(self._streams)
        if next_cursor >= len(self._streams):
            self._stream_cycle_complete = True

    def ensure_group(self, stream):
        if stream in self._known_groups:
            return
        try:
            self.redis.xgroup_create(stream, self.group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._known_groups.add(stream)

    def reset_connection_state(self):
        """Discard failover-era sockets and rebuild advisory discovery state."""
        self.redis.connection_pool.disconnect()
        self._streams = []
        self._stream_cursor = 0
        self._stream_window_cursor = 0
        self._stream_cycle_complete = True
        self._scan_cursor = 0
        self._last_stream_refresh = 0.0
        self._last_scan_refresh = 0.0
        self._known_groups.clear()

    def messages(self, consumer, count=8, reclaim_idle_ms=30000):
        self._refresh_streams()
        streams = self._next_stream_batch()
        for stream in streams:
            self.ensure_group(stream)

        # Redis applies COUNT independently to each stream in a multi-stream read. Read
        # streams one at a time so ``count`` remains a hard total transaction bound and
        # every message assigned to this consumer is returned to the caller.
        if not streams:
            return
        pending_checks = self.redis.pipeline(transaction=False)
        for stream in streams:
            pending_checks.xpending(stream, self.group)
        pending = pending_checks.execute()

        remaining = count
        for visited, (stream, summary) in enumerate(zip(streams, pending, strict=True), 1):
            # Read-only summaries avoid empty reclaim round trips without assigning
            # extra work. Reclaims still precede new reads and share one total COUNT.
            if summary["pending"] == 0:
                continue
            _next, claimed, _deleted = self.redis.xautoclaim(
                stream, self.group, consumer, reclaim_idle_ms, "0-0", count=remaining
            )
            remaining -= len(claimed)
            if remaining == 0:
                self._advance_stream_cursor(visited)
            for message_id, fields in claimed:
                yield stream, message_id, fields
            if remaining == 0:
                return

        for visited, stream in enumerate(streams, 1):
            batches = self.redis.xreadgroup(
                self.group,
                consumer,
                {stream: ">"},
                count=remaining,
                # Empty/owned streams must not delay later ready streams. The worker
                # backs off after an entirely idle bounded sweep (ADR0113).
            )
            remaining -= sum(len(entries) for _stream, entries in batches)
            if remaining == 0:
                self._advance_stream_cursor(visited)
            for batch_stream, entries in batches:
                for message_id, fields in entries:
                    yield batch_stream, message_id, fields
            if remaining == 0:
                return
        self._advance_stream_cursor(len(streams))

    def mark_durable_many(self, commands):
        commands = list(commands)
        if not 1 <= len(commands) <= 4:
            raise ValueError("Bounded reservation completion batch required")
        pipe = self.redis.pipeline(transaction=False)
        for event_id, command_id in commands:
            pipe.eval(MARK_DURABLE, 1, self.command_key(event_id, command_id))
        return pipe.execute()

    def acknowledge_many(self, messages):
        messages = list(messages)
        if not 1 <= len(messages) <= 4:
            raise ValueError("Bounded reservation acknowledgement batch required")
        pipe = self.redis.pipeline(transaction=False)
        for stream, message_id in messages:
            pipe.xack(stream, self.group, message_id)
            pipe.xdel(stream, message_id)
        result = pipe.execute()
        if len(result) != 2 * len(messages):
            raise RuntimeError("Incomplete reservation acknowledgement response")

    def mark_durable(self, event_id, command_id):
        return self.redis.eval(
            MARK_DURABLE, 1, self.command_key(event_id, command_id)
        )

    def mark_failed(self, event_id, command_id, seat_ids, error_code):
        return self.redis.eval(
            MARK_FAILED,
            4,
            self.command_key(event_id, command_id),
            self.cache.key(event_id),
            self.expiry_key(event_id),
            self.cache.delta_key(event_id),
            json.dumps(seat_ids),
            error_code,
            self.cache.delta_history_entries,
        )

    def acknowledge(self, stream, message_id):
        pipe = self.redis.pipeline(transaction=False)
        pipe.xack(stream, self.group, message_id)
        pipe.xdel(stream, message_id)
        pipe.execute()
