"""ADR0324: bounded ordering and fail-closed worker configuration."""
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from ticketing import workers
from ticketing.config import Settings


def envelope(order=None, kind="OrderPaid"):
    return {"schema_version": 1, "event_id": str(uuid4()), "event_type": kind,
            "payload": {"order_id": order or str(uuid4())}}


@pytest.mark.parametrize("value", [0, 5, True, 1.5])
def test_invalid_transaction_group_limit_rejected_before_poll(value):
    consumer = SimpleNamespace(poll=lambda **kw: pytest.fail("invalid configuration polled Kafka"))
    with pytest.raises(ValueError):
        workers.consume_iteration(consumer, None, None, 100, lane="fulfillment", fulfillment_batch_size=value)
    with pytest.raises(RuntimeError, match="FULFILLMENT_TRANSACTION_BATCH_SIZE"):
        replace(Settings(), fulfillment_transaction_batch_size=value).validate()


def test_default_one_and_separated_lane_requirement():
    assert Settings().fulfillment_transaction_batch_size == 1
    with pytest.raises(RuntimeError, match="EVENT_CONSUMER_SEPARATION"):
        replace(Settings(), fulfillment_transaction_batch_size=4, event_consumer_separation=False).validate()
    replace(Settings(), fulfillment_transaction_batch_size=4, event_consumer_separation=True).validate()


@pytest.mark.parametrize("lane", ["mixed", "projection"])
def test_batching_rejected_outside_fulfillment_before_retry_or_dead_letter(lane):
    with pytest.raises(ValueError):
        workers.consume_kafka_messages(None, None, [], lane=lane, fulfillment_batch_size=4)


def test_already_fetched_groups_are_bounded_ordered_and_split_repeated_order(monkeypatch):
    events = [envelope() for _ in range(6)]
    events.insert(2, envelope(events[0]["payload"]["order_id"], "TicketsIssued"))
    groups = []
    monkeypatch.setattr(workers, "_consume_fulfillment_group", lambda db, cache, group, projector: groups.append(list(group)))
    workers.consume_fulfillment_events(None, None, events, limit=4)
    assert [len(g) for g in groups] == [2, 4, 1]
    assert [e for g in groups for e in g] == events
    assert all(len({e["payload"]["order_id"] for e in g}) == len(g) for g in groups)


def test_poison_event_is_not_included_in_a_group(monkeypatch):
    groups = []
    monkeypatch.setattr(workers, "_consume_fulfillment_group", lambda *args: groups.append(args))
    with pytest.raises(ValueError):
        workers.consume_fulfillment_events(None, None, [envelope(), {"schema_version": 9}], limit=4)
    assert groups == []


def test_enabled_batches_never_merge_partitions_and_commit_offsets_last(monkeypatch):
    first = workers.TopicPartition("ticketing.events", 0)
    second = workers.TopicPartition("ticketing.events", 1)
    groups, commits = [], []
    def messages():
        return [SimpleNamespace(value=json.dumps(envelope()).encode(), offset=offset) for offset in [7, 8]]
    batches = {first: messages(), second: messages()}
    consumer = SimpleNamespace(poll=lambda **kw: batches, commit=lambda: commits.append(len(groups)),
                               seek=lambda *args: pytest.fail("successful groups rewound"))
    monkeypatch.setattr(workers, "_consume_fulfillment_group", lambda db, cache, group, projector: groups.append(list(group)))
    assert workers.consume_iteration(consumer, None, None, 100, lane="fulfillment", fulfillment_batch_size=4)
    assert [len(g) for g in groups] == [2, 2] and commits == [2]


def test_failed_durable_group_never_advances_offsets(monkeypatch):
    partition = workers.TopicPartition("ticketing.events", 0)
    rewinds, commits = [], []
    batches = {partition: [SimpleNamespace(value=json.dumps(envelope()).encode(), offset=7)]}
    consumer = SimpleNamespace(poll=lambda **kw: batches, commit=lambda: commits.append(True),
                               seek=lambda tp, offset: rewinds.append((tp, offset)))
    def failed(*args, **kwargs): raise RuntimeError("durable handling failed")
    monkeypatch.setattr(workers, "consume_kafka_messages", failed)
    with pytest.raises(RuntimeError, match="durable handling"):
        workers.consume_iteration(consumer, None, None, 100, lane="fulfillment", fulfillment_batch_size=4)
    assert rewinds == [(partition, 7)] and not commits


@pytest.mark.parametrize("event,exception", [({"schema_version": 2}, ValueError),
    ({"schema_version": 1, "event_type": "OrderPaid"}, KeyError)])
def test_default_path_rejects_malformed_envelope_before_database(event, exception):
    db = SimpleNamespace(transaction=lambda: pytest.fail("malformed event acquired SQL"))
    with pytest.raises(exception): workers._consume_event_transaction(db, None, event)
