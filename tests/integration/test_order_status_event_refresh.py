import os
import time
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from redis import Redis
from redis.exceptions import TimeoutError

from ticketing.application.reservations import Reservations
from ticketing.domain import Failure
from ticketing.infrastructure.order_status_cache import RedisOrderStatusCache
from ticketing.infrastructure.order_status_projector import CommittedOrderStatusProjector
from ticketing.workers import consume_event, publish_batch

pytestmark = pytest.mark.integration


@pytest.fixture(params=[False, True], autouse=True)
def committed_capture_mode(request, monkeypatch):
    implementation = CommittedOrderStatusProjector

    def projector(*args, **kwargs):
        return implementation(*args, **kwargs, capture_committed=request.param)

    monkeypatch.setattr(__import__(__name__, fromlist=[""]), "CommittedOrderStatusProjector", projector)
    return request.param


@pytest.fixture
def event_cache():
    url = os.getenv("TEST_REDIS_URL")
    if not url:
        pytest.skip("TEST_REDIS_URL not configured")
    redis = Redis.from_url(url, decode_responses=True, socket_timeout=.1, socket_connect_timeout=.1)
    keys = []

    def build(actor, identifier, age=1000):
        cache = RedisOrderStatusCache(redis, age)
        keys.extend([cache.key(actor, identifier), cache.key("other", identifier)])
        return cache

    yield build
    if keys:
        redis.delete(*keys)
    redis.close()


def prepare(system, event_cache, age=1000):
    svc, db, event = system
    hold = svc.reserve("owner", event, ["A"], str(uuid4()))
    cache = event_cache("owner", hold["order_id"], age)
    pending = svc.get_order("owner", hold["order_id"])
    stamp = cache.snapshot_start()
    cache.put("owner", hold["order_id"], pending, stamp)
    attempt = svc.initiate_payment("owner", hold["order_id"], str(uuid4()), "SUCCEEDED", 0, 1)
    callback = {"callback_id": str(uuid4()), "payment_id": attempt["payment_id"],
                "order_id": hold["order_id"], "amount": hold["total"], "currency": hold["currency"],
                "outcome": "SUCCEEDED"}
    svc.callback(callback)
    envelope = {"event_id": str(uuid4()), "schema_version": 1, "event_type": "OrderPaid",
                "payload": {"order_id": hold["order_id"], "actor": "other"}}
    # Strictly newer Redis timestamps avoid an intentionally permitted equal-ms race.
    time.sleep(.004)
    return svc, db, hold, cache, pending, stamp, envelope


def drain_outbox(db):
    producer = Mock()
    for _ in range(10):
        if not publish_batch(db, producer, 100):
            break
    else:
        pytest.fail("owned test outbox failed to drain")
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE published_at IS NULL").fetchone()["n"] == 0


def test_committed_ticket_snapshot_replaces_pending_and_avoids_api_database_read(system, event_cache):
    svc, db, hold, cache, _, _, envelope = prepare(system, event_cache)
    projector = CommittedOrderStatusProjector(db, cache)
    original = cache.publish

    def released(*args):
        stats = db.pool.get_stats()
        assert stats["pool_available"] == stats["pool_size"]
        return original(*args)

    cache.publish = released
    consume_event(db, None, envelope, projector)
    row, _ = cache.lookup("owner", hold["order_id"])
    assert row["status"] == "FULFILLED" and len(row["tickets"]) == 1
    store = Mock()
    reader = Reservations(store, cache)
    assert reader.get_order("owner", hold["order_id"]) == row
    store.get_order.assert_not_called()
    assert cache.lookup("other", hold["order_id"])[0] is None
    with pytest.raises(Failure, match="ORDER_NOT_FOUND"):
        Reservations(svc.store, cache).get_order("other", hold["order_id"])
    drain_outbox(db)


def test_post_commit_redis_failure_does_not_replay_financial_effect_and_duplicate_repairs_cache(system, event_cache):
    _, db, hold, cache, _, _, envelope = prepare(system, event_cache)
    projector = CommittedOrderStatusProjector(db, cache)
    with patch.object(cache.redis, "eval", side_effect=TimeoutError("owned publication outage")):
        consume_event(db, None, envelope, projector)
    with db.transaction() as conn:
        assert conn.execute("SELECT status FROM orders WHERE id=%s", (hold["order_id"],)).fetchone()["status"] == "FULFILLED"
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox WHERE event_id=%s", (envelope["event_id"],)).fetchone()["n"] == 1
    assert cache.lookup("owner", hold["order_id"])[0]["status"] == "PENDING"
    consume_event(db, None, envelope, projector)
    assert cache.lookup("owner", hold["order_id"])[0]["status"] == "FULFILLED"
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
    drain_outbox(db)


def test_rolled_back_fulfillment_never_publishes_ticket_snapshot(system, event_cache):
    _, db, hold, cache, _, _, envelope = prepare(system, event_cache)
    projector = CommittedOrderStatusProjector(db, cache)
    with (
        patch("ticketing.workers.event", side_effect=RuntimeError("owned rollback")),
        pytest.raises(RuntimeError, match="owned rollback"),
    ):
        consume_event(db, None, envelope, projector)
    assert cache.lookup("owner", hold["order_id"])[0]["status"] == "PENDING"
    with db.transaction() as conn:
        assert conn.execute("SELECT status FROM orders WHERE id=%s", (hold["order_id"],)).fetchone()["status"] == "PAID"
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox WHERE event_id=%s", (envelope["event_id"],)).fetchone()["n"] == 0


def test_delayed_pending_fill_and_old_publish_cannot_replace_new_ticket_snapshot(system, event_cache):
    _, db, hold, cache, pending, old_stamp, envelope = prepare(system, event_cache, age=200)
    consume_event(db, None, envelope, CommittedOrderStatusProjector(db, cache))
    key = cache.key("owner", hold["order_id"])
    before = cache.redis.pttl(key)
    assert 0 < before <= 200
    cache.put("owner", hold["order_id"], pending, old_stamp)
    cache.publish("owner", hold["order_id"], pending, old_stamp)
    assert cache.lookup("owner", hold["order_id"])[0]["status"] == "FULFILLED"
    assert cache.redis.pttl(key) <= before
    time.sleep(max(0, cache.redis.pttl(key))/1000+.02)
    cache.publish("owner", hold["order_id"], pending, old_stamp)
    assert cache.lookup("owner", hold["order_id"])[0] is None


def test_equal_stamp_does_not_replace_or_extend_snapshot_and_invalid_future_entry_recovers(system, event_cache):
    svc, _, event = system
    hold = svc.reserve("owner", event, ["A"], str(uuid4()))
    cache = event_cache("owner", hold["order_id"])
    row = svc.get_order("owner", hold["order_id"])
    stamp = cache.snapshot_start()
    cache.publish("owner", hold["order_id"], row, stamp)
    key = cache.key("owner", hold["order_id"])
    before = cache.redis.pttl(key)
    cache.publish("owner", hold["order_id"], row, stamp)
    assert cache.redis.pttl(key) <= before
    cache.redis.set(key, '{"schema_version":1,"snapshot_start_ms":999999999999999,"fresh_until_ms":999999999999999}')
    cache.publish("owner", hold["order_id"], row, cache.snapshot_start())
    assert cache.lookup("owner", hold["order_id"])[0]["status"] == "PENDING"


def test_out_of_order_event_uses_current_state_and_tickets_issued_can_repair_after_expiry(system, event_cache):
    _, db, hold, cache, _, _, envelope = prepare(system, event_cache, age=100)
    projector = CommittedOrderStatusProjector(db, cache)
    consume_event(db, None, envelope, projector)
    cache.redis.delete(cache.key("owner", hold["order_id"]))
    issued = {"event_id": str(uuid4()), "schema_version": 1, "event_type": "TicketsIssued",
              "payload": {"order_id": hold["order_id"], "status": "PENDING", "actor": "other"}}
    consume_event(db, None, issued, projector)
    assert cache.lookup("owner", hold["order_id"])[0]["status"] == "FULFILLED"
    ttl = cache.redis.pttl(cache.key("owner", hold["order_id"]))
    time.sleep(max(0, ttl)/1000+.02)
    consume_event(db, None, envelope, projector)
    assert cache.lookup("owner", hold["order_id"])[0]["status"] == "FULFILLED"
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
    drain_outbox(db)


@pytest.mark.parametrize("damage", [None, "deleted", "corrupt", "expired"])
def test_dedup_skips_only_identical_fresh_projection_and_repairs_missing_entries(system, event_cache, damage, committed_capture_mode):
    _, db, hold, cache, _, _, envelope = prepare(system, event_cache, age=200 if damage == "expired" else 1000)
    projector = CommittedOrderStatusProjector(db, cache, deduplicate=True)
    issued = {"event_id": str(uuid4()), "schema_version": 1, "event_type": "TicketsIssued",
              "payload": {"order_id": hold["order_id"], "actor": "other", "status": "PENDING"}}
    key = cache.key("owner", hold["order_id"])
    with (patch.object(db, "transaction", wraps=db.transaction) as transactions,
          patch.object(cache, "publish", wraps=cache.publish) as publish):
        consume_event(db, None, envelope, projector)
        initial = 1 if committed_capture_mode else 2
        assert transactions.call_count == initial
        assert projector._recent
        ttl = cache.redis.pttl(key)
        if damage == "deleted":
            cache.redis.delete(key)
        elif damage == "corrupt":
            cache.redis.set(key, '{"schema_version":99}', px=1000)
        elif damage == "expired":
            time.sleep(ttl/1000+.02)
        consume_event(db, None, issued, projector)
        assert transactions.call_count == initial + (1 if damage is None else 2)
        assert publish.call_count == (1 if damage is None else 2)
        if damage is None:
            assert cache.redis.pttl(key) <= ttl
            consume_event(db, None, issued, projector)
            assert transactions.call_count == initial + 2 and publish.call_count == 1
    assert cache.lookup("owner", hold["order_id"])[0]["status"] == "FULFILLED"
    assert cache.lookup("other", hold["order_id"])[0] is None
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
    drain_outbox(db)


def test_dedup_failed_publication_does_not_suppress_later_repair_or_replay(system, event_cache):
    _, db, hold, cache, _, _, envelope = prepare(system, event_cache)
    projector = CommittedOrderStatusProjector(db, cache, deduplicate=True)
    with patch.object(cache.redis, "eval", side_effect=TimeoutError("owned projection outage")):
        consume_event(db, None, envelope, projector)
    assert not projector._recent
    issued = {"event_id": str(uuid4()), "schema_version": 1, "event_type": "TicketsIssued",
              "payload": {"order_id": hold["order_id"]}}
    consume_event(db, None, issued, projector)
    assert cache.lookup("owner", hold["order_id"])[0]["status"] == "FULFILLED"
    cache.redis.delete(cache.key("owner", hold["order_id"]))
    consume_event(db, None, envelope, projector)
    assert cache.lookup("owner", hold["order_id"])[0]["status"] == "FULFILLED"
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
    drain_outbox(db)


def test_new_delivery_for_fulfilled_order_retains_actual_ticket_id(system, event_cache, committed_capture_mode):
    _, db, hold, cache, _, _, envelope = prepare(system, event_cache)
    projector = CommittedOrderStatusProjector(db, cache, deduplicate=True)
    consume_event(db, None, envelope, projector)
    original = cache.lookup("owner", hold["order_id"])[0]["tickets"]
    cache.redis.delete(cache.key("owner", hold["order_id"]))
    envelope = {**envelope, "event_id": str(uuid4())}
    with patch.object(db, "transaction", wraps=db.transaction) as transactions:
        consume_event(db, None, envelope, projector)
        assert transactions.call_count == (1 if committed_capture_mode else 2)
    assert cache.lookup("owner", hold["order_id"])[0]["tickets"] == original
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
    drain_outbox(db)
