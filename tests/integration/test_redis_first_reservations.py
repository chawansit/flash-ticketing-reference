import json
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest

from ticketing.domain import Failure
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.redis_reservations import RedisReservationIntake
from ticketing.infrastructure.reservations import PostgresReservations
from ticketing.workers import persist_reservation_batch, snapshot

pytestmark = pytest.mark.integration


@pytest.fixture
def redis_first(system):
    _service, db, event_id = system
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("TEST_REDIS_URL not configured")
    cache = RedisSeats(url)
    intake = RedisReservationIntake(cache, hold_seconds=120)
    store = PostgresReservations(db, cache, 120)
    snapshot(db, cache, event_id)
    try:
        yield store, intake, cache, db, event_id
    finally:
        stream = intake.stream_key(event_id)
        keys = list(cache.redis.scan_iter(match=f"*{{{event_id}}}*"))
        if keys:
            cache.redis.delete(*keys)
        cache.redis.srem("reservation-stream-registry", stream)
        cache.redis.close()


def test_one_of_100_concurrent_intakes_wins_and_writer_is_replay_safe(redis_first):
    store, intake, _cache, db, event_id = redis_first
    gate = Barrier(100)

    def attempt(index):
        gate.wait()
        try:
            return intake.enqueue(f"actor-{index}", event_id, ["A"], f"key-{index}")
        except Failure:
            return None

    with ThreadPoolExecutor(max_workers=100) as executor:
        results = list(executor.map(attempt, range(100)))

    accepted = [result for result in results if result is not None]
    assert len(accepted) == 1
    pending = accepted[0]
    assert pending["persistence_status"] == "PENDING"
    assert intake.redis.xlen(intake.stream_key(event_id)) == 1

    stream_row = intake.redis.xrange(intake.stream_key(event_id), count=1)[0][1]
    payload = json.loads(stream_row["payload"])
    response = json.loads(stream_row["response"])
    first = store.persist_reservation_command(payload, response)
    assert store.persist_reservation_command(payload, response) == first

    # The worker can replay the already committed command and safely acknowledge it.
    assert persist_reservation_batch(store, intake, "test-writer", 8)
    status = intake.status(event_id, pending["command_id"])
    assert status["persistence_status"] == "DURABLE"
    assert intake.redis.xlen(intake.stream_key(event_id)) == 0
    replay = intake.enqueue(payload["actor"], event_id, ["A"], payload["idempotency_key"])
    assert replay == status

    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM holds").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM reservation_commands").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM outbox_events").fetchone()["n"] == 1


def test_changed_idempotency_payload_is_rejected(redis_first):
    _store, intake, _cache, _db, event_id = redis_first
    intake.enqueue("actor", event_id, ["A"], "same")
    with pytest.raises(Failure, match="IDEMPOTENCY_MISMATCH"):
        intake.enqueue("actor", event_id, ["B"], "same")


def test_checkout_is_blocked_until_command_is_durable(redis_first):
    store, intake, _cache, _db, event_id = redis_first
    pending = intake.enqueue("actor", event_id, ["C"], "pending")
    with pytest.raises(Failure, match="HOLD_NOT_FOUND"):
        store.checkout("actor", pending["hold_id"], "checkout")

    assert persist_reservation_batch(store, intake, "test-writer", 8)
    assert store.checkout("actor", pending["hold_id"], "checkout")["order_id"] == pending["order_id"]


def test_persistence_conflict_is_compensated_and_snapshot_rebuilt(redis_first):
    store, intake, cache, db, event_id = redis_first
    first = intake.enqueue("winner", event_id, ["A"], "winner")
    assert persist_reservation_batch(store, intake, "test-writer", 8)

    provisional = intake.enqueue("stale-cache", event_id, ["B"], "conflict")
    with db.transaction() as conn:
        conn.execute(
            """UPDATE event_seats SET booked_order_id=%s,hold_id=NULL,reserved_until=NULL,version=version+1
            WHERE event_id=%s AND seat_id='B'""",
            (first["order_id"], event_id),
        )

    assert persist_reservation_batch(store, intake, "test-writer", 8)
    status = intake.status(event_id, provisional["command_id"])
    assert status["persistence_status"] == "FAILED"
    assert status["error_code"] == "SEAT_UNAVAILABLE"
    seats = {seat["seat_id"]: seat for seat in cache.read(str(event_id))["seats"]}
    assert seats["B"]["status"] == "SOLD"
    with db.transaction() as conn:
        assert (
            conn.execute(
                "SELECT count(*) AS n FROM reservation_commands WHERE command_id=%s",
                (provisional["command_id"],),
            ).fetchone()["n"]
            == 0
        )




def test_stream_backlog_is_bounded_before_more_seats_are_mutated(redis_first):
    _store, _intake, cache, _db, event_id = redis_first
    intake = RedisReservationIntake(cache, max_backlog=1)
    intake.enqueue("one", event_id, ["A"], "one")
    with pytest.raises(Failure, match="RESERVATION_BACKLOG_FULL"):
        intake.enqueue("two", event_id, ["B"], "two")
    seats = {seat["seat_id"]: seat for seat in cache.read(str(event_id))["seats"]}
    assert seats["B"]["status"] == "AVAILABLE"


def test_expired_provisional_can_be_reclaimed_without_losing_new_owner(redis_first):
    import time

    store, _intake, cache, db, event_id = redis_first
    intake = RedisReservationIntake(cache, hold_seconds=1)
    old = intake.enqueue("old", event_id, ["A"], "old")
    time.sleep(1.1)
    new = intake.enqueue("new", event_id, ["A"], "new")

    assert persist_reservation_batch(store, intake, "test-writer", 8)
    assert intake.status(event_id, old["command_id"])["persistence_status"] == "FAILED"
    assert intake.status(event_id, new["command_id"])["persistence_status"] == "DURABLE"
    seats = {seat["seat_id"]: seat for seat in cache.read(str(event_id))["seats"]}
    assert seats["A"]["hold_id"] == new["hold_id"]
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM holds").fetchone()["n"] == 1
        assert str(
            conn.execute(
                "SELECT hold_id FROM event_seats WHERE event_id=%s AND seat_id='A'",
                (event_id,),
            ).fetchone()["hold_id"]
        ) == new["hold_id"]


def test_writer_failure_leaves_pending_message_for_reclaim(redis_first, monkeypatch):
    store, intake, _cache, _db, event_id = redis_first
    pending = intake.enqueue("actor", event_id, ["A"], "key")
    original = store.persist_reservation_commands

    def unavailable(*_args):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(store, "persist_reservation_commands", unavailable)
    with pytest.raises(RuntimeError, match="database unavailable"):
        persist_reservation_batch(store, intake, "failed-writer", 8)

    stream = intake.stream_key(event_id)
    assert intake.redis.xpending(stream, intake.group)["pending"] == 1

    monkeypatch.setattr(store, "persist_reservation_commands", original)
    claimed = list(intake.messages("recovery-writer", count=8, reclaim_idle_ms=0))
    assert len(claimed) == 1
    claimed_stream, message_id, fields = claimed[0]
    payload = json.loads(fields["payload"])
    response = json.loads(fields["response"])
    assert original([(payload, response)])[0][0] == "durable"
    intake.mark_durable(event_id, pending["command_id"])
    intake.acknowledge(claimed_stream, message_id)

    assert intake.status(event_id, pending["command_id"])["persistence_status"] == "DURABLE"
    assert intake.redis.xpending(stream, intake.group)["pending"] == 0


def test_status_is_owner_scoped_and_durable_marker_detects_lost_metadata(redis_first):
    _store, intake, cache, _db, event_id = redis_first
    pending = intake.enqueue("owner", event_id, ["A"], "owner-key")

    with pytest.raises(Failure, match="RESERVATION_COMMAND_NOT_FOUND"):
        intake.status(event_id, pending["command_id"], "someone-else")

    assert intake.status(event_id, pending["command_id"], "owner")["persistence_status"] == "PENDING"
    assert intake.mark_failed(event_id, pending["command_id"], ["A"], "TEST_FAILURE") == 1
    assert intake.mark_durable(event_id, pending["command_id"]) == -1

    cache.redis.delete(intake.command_key(event_id, pending["command_id"]))
    assert intake.mark_durable(event_id, pending["command_id"]) == 0


def test_new_messages_are_read_across_event_streams_without_starvation(redis_first):
    _store, intake, cache, db, event_id = redis_first
    second_event = uuid4()
    second_stream = intake.stream_key(second_event)
    try:
        with db.transaction() as conn:
            conn.execute(
                """INSERT INTO events VALUES (%s,'Second','THB',
                clock_timestamp()-interval '1 day',clock_timestamp()+interval '1 day')""",
                (second_event,),
            )
            conn.execute(
                "INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,'A',100)",
                (second_event,),
            )
        snapshot(db, cache, second_event)

        intake.enqueue("first-owner", event_id, ["A"], "first-key")
        intake.enqueue("second-owner", second_event, ["A"], "second-key")
        messages = list(intake.messages("fair-writer", count=8))

        assert {stream for stream, _message_id, _fields in messages} == {
            intake.stream_key(event_id),
            second_stream,
        }
    finally:
        keys = list(cache.redis.scan_iter(match=f"*{{{second_event}}}*"))
        if keys:
            cache.redis.delete(*keys)
        cache.redis.srem("reservation-stream-registry", second_stream)
