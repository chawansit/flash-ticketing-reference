"""ADR0297: transaction boundaries, actual identities and conservative freshness."""
import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from redis.exceptions import TimeoutError

from ticketing.config import Settings
from ticketing.infrastructure.order_status_cache import scalar
from ticketing.infrastructure.order_status_projector import CommittedOrderStatusProjector
from ticketing.workers import consume_event


class FinancialDatabase:
    def __init__(self, *, replay=False, conflict=False, missing=False, commit_error=False):
        self.order = {"id": uuid4(), "actor": "actual-owner", "hold_id": uuid4(),
                      "event_id": uuid4(), "total": 100, "currency": "THB", "status": "PAID",
                      "created_at": datetime.now(UTC)}
        self.booking, self.ticket = uuid4(), uuid4()
        self.replay, self.conflict, self.missing = replay, conflict, missing
        self.commit_error, self.held, self.committed = commit_error, False, False
        self.statements = []
        self.transactions = 0

    @contextmanager
    def transaction(self):
        self.transactions += 1
        self.held = True
        try:
            yield self
            if self.commit_error:
                raise RuntimeError("commit failed")
            self.committed = True
        finally:
            self.held = False

    def execute(self, sql, params=None):
        self.statements.append(sql)
        result = Mock()
        if sql.startswith("INSERT INTO consumer_inbox"):
            result.fetchone.return_value = None if self.replay else {"event_id": params[0]}
        elif sql.startswith("SELECT * FROM orders"):
            result.fetchone.return_value = dict(self.order)
        elif sql.startswith(("SELECT b.id", "SELECT id FROM bookings")):
            result.fetchall.return_value = [{"id": self.booking, "seat_id": "A",
                "ticket_id": None if self.missing else self.ticket}]
        elif sql.startswith("INSERT INTO tickets"):
            result.fetchone.return_value = None if self.conflict else {"id": self.ticket}
        return result


def setup(**kwargs):
    db = FinancialDatabase(**kwargs)
    cache = Mock(max_age_ms=1000)
    cache.snapshot_start.return_value = 10000
    cache.publish.return_value = True
    projector = CommittedOrderStatusProjector(db, cache, deduplicate=True, capture_committed=True)
    envelope = {"event_id": str(uuid4()), "schema_version": 1, "event_type": "OrderPaid",
                "payload": {"order_id": str(db.order["id"]), "actor": "spoofed-owner"}}
    return db, cache, projector, envelope


@pytest.mark.parametrize("conflict", [False, True])
def test_publishes_actual_ticket_after_commit_without_extra_read(conflict):
    db, cache, projector, envelope = setup(conflict=conflict)
    cache.snapshot_start.side_effect = lambda: 10000 if not db.held else pytest.fail("clock held DB")
    cache.publish.side_effect = lambda *_: True if db.committed and not db.held else pytest.fail("not committed")
    with patch.object(projector, "refresh") as refresh:
        consume_event(db, None, envelope, projector)
    refresh.assert_not_called()
    actor, identifier, snapshot, stamp = cache.publish.call_args.args
    assert actor == "actual-owner" and identifier == db.order["id"] and stamp == 10000
    assert snapshot["tickets"] == [{"id": str(db.ticket), "seat_id": "A"}]
    assert snapshot["status"] == "FULFILLED"
    assert db.transactions == 1 and len(db.statements) == 6
    assert db.statements[3].endswith(" RETURNING id")
    assert projector._recent


def test_commit_failure_never_publishes_or_refreshes():
    db, cache, projector, envelope = setup(commit_error=True)
    with patch.object(projector, "refresh") as refresh, pytest.raises(RuntimeError, match="commit failed"):
        consume_event(db, None, envelope, projector)
    assert not db.committed and not db.held
    cache.publish.assert_not_called()
    refresh.assert_not_called()


@pytest.mark.parametrize("scenario", ["replay", "missing", "clock"])
def test_unavailable_capture_uses_existing_committed_read(scenario):
    db, cache, projector, envelope = setup(
        replay=scenario == "replay", conflict=scenario == "missing", missing=scenario == "missing")
    if scenario == "clock":
        cache.snapshot_start.side_effect = TimeoutError("clock unavailable")
    with patch.object(projector, "refresh") as refresh:
        consume_event(db, None, envelope, projector)
    assert db.committed and not db.held
    refresh.assert_called_once_with(str(db.order["id"]), event_type="OrderPaid")
    cache.publish.assert_not_called()


def test_disabled_path_preserves_original_statements():
    db, cache, projector, envelope = setup()
    projector.capture_committed = False
    with patch.object(projector, "refresh") as refresh:
        consume_event(db, None, envelope, projector)
    cache.snapshot_start.assert_not_called()
    cache.publish.assert_not_called()
    assert db.transactions == 1 and len(db.statements) == 6
    assert db.statements[2] == "SELECT id FROM bookings WHERE order_id=%s"
    assert not db.statements[3].endswith(" RETURNING id")
    refresh.assert_called_once()


def test_post_commit_redis_failure_is_advisory_and_leaves_no_hint():
    db, cache, projector, envelope = setup()
    cache.publish.side_effect = TimeoutError("publication unavailable")
    consume_event(db, None, envelope, projector)
    assert db.committed and not projector._recent
    cache.publish.assert_called_once()


@pytest.mark.parametrize("result", [False, None, 1])
def test_rejected_publication_does_not_establish_dedup_hint(result):
    db, cache, projector, envelope = setup()
    cache.publish.return_value = result
    consume_event(db, None, envelope, projector)
    assert db.committed and not projector._recent


def test_capture_does_not_extend_freshness_or_skip_after_deadline():
    db, cache, projector, envelope = setup()
    clock = [10.0]
    def publish(*_):
        clock[0] = 10.9
        return True
    cache.publish.side_effect = publish
    with patch("ticketing.infrastructure.order_status_projector.time.monotonic", side_effect=lambda: clock[0]):
        consume_event(db, None, envelope, projector)
        hint = projector._recent[db.order["id"]]
        assert hint[2] == 11.0
        clock[0] = 11.0
        assert not projector._can_reuse(db.order["id"])
    cache.lookup.assert_not_called()


def test_invalid_complete_snapshot_falls_back_and_never_publishes():
    db, cache, projector, _ = setup()
    snapshot = json.loads(json.dumps({**db.order, "status": "FULFILLED", "tickets": []}, default=scalar))
    with patch.object(projector, "refresh") as refresh:
        projector.publish_committed(snapshot, 10000, 10)
    refresh.assert_called_once_with(db.order["id"], event_type="OrderPaid")
    cache.publish.assert_not_called()


def test_setting_defaults_off_and_requires_event_refresh():
    assert not Settings().order_status_committed_snapshot
    with pytest.raises(RuntimeError, match="ORDER_STATUS_COMMITTED_SNAPSHOT"):
        replace(Settings(), order_status_committed_snapshot=True).validate()
    replace(Settings(), order_status_committed_snapshot=True, order_status_event_refresh=True,
            order_status_cache_ms=1000).validate()
    with pytest.raises(TypeError, match="boolean"):
        CommittedOrderStatusProjector(Mock(), Mock(), capture_committed=1)
