"""ADR0324: real durable grouping, atomic rollback and committed cache publication."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4

import psycopg
import pytest
from test_order_status_event_refresh import event_cache  # noqa: F401

from ticketing import workers
from ticketing.domain import Failure
from ticketing.infrastructure.order_status_projector import CommittedOrderStatusProjector
from ticketing.infrastructure.postgres import Postgres

pytestmark = pytest.mark.integration


def paid_orders(system):
    svc, _, show = system
    events = []
    for seat in ["A", "B", "C"]:
        hold = svc.reserve("owner", show, [seat], str(uuid4()))
        attempt = svc.initiate_payment("owner", hold["order_id"], str(uuid4()), "SUCCEEDED", 0, 1)
        svc.callback({"callback_id": str(uuid4()), "payment_id": attempt["payment_id"],
                      "order_id": hold["order_id"], "amount": hold["total"],
                      "currency": hold["currency"], "outcome": "SUCCEEDED"})
        events.append({"schema_version": 1, "event_id": str(uuid4()), "event_type": "OrderPaid",
                       "payload": {"order_id": hold["order_id"], "actor": "other"}})
    return events


def counts(db):
    with db.transaction() as conn:
        return conn.execute("""SELECT (SELECT count(*) FROM tickets) AS tickets,
            (SELECT count(*) FROM consumer_inbox WHERE consumer='fulfillment') AS inbox,
            (SELECT count(*) FROM orders WHERE status='FULFILLED') AS fulfilled,
            (SELECT count(*) FROM outbox_events WHERE event_type='TicketsIssued') AS issued_events""").fetchone()


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("limit,expected_commits", [(1, 3), (4, 1)])
def test_transaction_batching_and_replay_after_worker_restart(system, explicit, limit, expected_commits):
    events = paid_orders(system); _, base, _ = system
    db = Postgres(base.pool.conninfo, maximum=2, explicit_start=explicit)
    try:
        db.pool.wait(timeout=5)
        with patch.object(db, "transaction", wraps=db.transaction) as tx:
            workers.consume_fulfillment_events(db, None, events, limit=limit)
            assert tx.call_count == expected_commits
        assert counts(db) == {"tickets": 3, "inbox": 3, "fulfilled": 3, "issued_events": 3}
    finally:
        db.close()
    restarted = Postgres(base.pool.conninfo, maximum=2, explicit_start=explicit)
    try:
        workers.consume_fulfillment_events(restarted, None, events, limit=4)
        assert counts(restarted) == {"tickets": 3, "inbox": 3, "fulfilled": 3, "issued_events": 3}
    finally:
        restarted.close()


def test_no_partial_visibility_and_no_cache_work_while_connection_owned(system, event_cache):  # noqa: F811
    events = paid_orders(system); svc, db, _ = system
    cache = event_cache("owner", events[0]["payload"]["order_id"])
    projector = CommittedOrderStatusProjector(db, cache, deduplicate=True, capture_committed=True, notification_dedup=True)
    original = workers._apply_consumer_event
    order_ids = []
    def checked(conn, ignored, envelope, **kwargs):
        order_ids.append(envelope["payload"]["order_id"])
        if len(order_ids) == 2:
            with psycopg.connect(db.pool.conninfo) as observer:
                assert observer.execute("SELECT count(*) FROM tickets").fetchone()[0] == 0
                assert observer.execute("SELECT count(*) FROM consumer_inbox").fetchone()[0] == 0
        return original(conn, ignored, envelope, **kwargs)
    publish = projector.publish_committed
    def released(*args):
        assert db.pool.get_stats()["pool_available"] == db.pool.get_stats()["pool_size"]
        assert counts(db)["tickets"] == 3
        return publish(*args)
    with patch.object(workers, "_apply_consumer_event", side_effect=checked), patch.object(projector, "publish_committed", side_effect=released):
        workers.consume_fulfillment_events(db, None, events, projector, limit=4)
    for e in events:
        identifier = e["payload"]["order_id"]
        row, _ = cache.lookup("owner", identifier)
        assert row["status"] == "FULFILLED" and len(row["tickets"]) == 1
        assert cache.lookup("other", identifier)[0] is None
        with pytest.raises(Failure): svc.get_order("other", identifier)
        cache.redis.delete(cache.key("owner", identifier))


def test_middle_event_failure_rolls_back_whole_group(system, event_cache):  # noqa: F811
    events = paid_orders(system); _, db, _ = system
    cache = event_cache("owner", events[0]["payload"]["order_id"])
    projector = CommittedOrderStatusProjector(db, cache, capture_committed=True)
    original = workers._apply_consumer_event
    def fail(conn, ignored, envelope, **kwargs):
        if envelope == events[1]: raise RuntimeError("injected second event failure")
        return original(conn, ignored, envelope, **kwargs)
    with patch.object(workers, "_apply_consumer_event", side_effect=fail), patch.object(projector, "publish_committed") as publish:
        with pytest.raises(RuntimeError, match="second event"):
            workers.consume_fulfillment_events(db, None, events, projector, limit=4)
        publish.assert_not_called()
    assert counts(db) == {"tickets": 0, "inbox": 0, "fulfilled": 0, "issued_events": 0}
    workers.consume_fulfillment_events(db, None, events, limit=4)
    assert counts(db)["tickets"] == 3


def test_actual_deferred_commit_failure_releases_no_snapshot(system, event_cache):  # noqa: F811
    events = paid_orders(system); _, db, _ = system
    cache = event_cache("owner", events[0]["payload"]["order_id"])
    projector = CommittedOrderStatusProjector(db, cache, capture_committed=True)
    with db.transaction() as conn:
        conn.execute("CREATE FUNCTION reject_issue() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected deferred failure'; END; $$")
        conn.execute("CREATE CONSTRAINT TRIGGER reject_issue AFTER INSERT ON tickets DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_issue()")
    with patch.object(projector, "publish_committed") as publish:
        with pytest.raises(psycopg.errors.RaiseException, match="deferred failure"):
            workers.consume_fulfillment_events(db, None, events, projector, limit=4)
        publish.assert_not_called()
    assert counts(db) == {"tickets": 0, "inbox": 0, "fulfilled": 0, "issued_events": 0}
    with db.transaction() as conn: conn.execute("DROP TRIGGER reject_issue ON tickets")
    workers.consume_fulfillment_events(db, None, events, limit=4)
    assert counts(db)["tickets"] == 3


def test_lost_post_commit_publication_replays_without_duplicate_effects(system, event_cache):  # noqa: F811
    events = paid_orders(system); _, db, _ = system
    cache = event_cache("owner", events[0]["payload"]["order_id"])
    projector = CommittedOrderStatusProjector(db, cache, capture_committed=True)
    with (patch.object(projector, "publish_committed", side_effect=RuntimeError("lost post-commit response")),
          pytest.raises(RuntimeError, match="post-commit")):
        workers.consume_fulfillment_events(db, None, events, projector, limit=4)
    assert counts(db) == {"tickets": 3, "inbox": 3, "fulfilled": 3, "issued_events": 3}
    restarted = CommittedOrderStatusProjector(db, cache, capture_committed=True)
    workers.consume_fulfillment_events(db, None, events, restarted, limit=4)
    assert counts(db) == {"tickets": 3, "inbox": 3, "fulfilled": 3, "issued_events": 3}
    for e in events:
        identifier = e["payload"]["order_id"]
        assert cache.lookup("owner", identifier)[0]["status"] == "FULFILLED"
        cache.redis.delete(cache.key("owner", identifier))


def test_reverse_order_concurrent_replay_has_no_duplicate_tickets_or_deadlock(system):
    events = paid_orders(system); _, db, _ = system
    ready = Barrier(2)
    def consume(reverse):
        ready.wait(timeout=5)
        workers.consume_fulfillment_events(db, None, list(reversed(events)) if reverse else events, limit=4)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(consume, reverse) for reverse in [False, True]]
        for future in futures: future.result(timeout=10)
    assert counts(db) == {"tickets": 3, "inbox": 3, "fulfilled": 3, "issued_events": 3}
