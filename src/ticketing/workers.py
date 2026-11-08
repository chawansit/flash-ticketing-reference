import argparse
import hashlib
import hmac
import json
import logging
import os
import signal
import socket
import time
import urllib.request
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from uuid import UUID, uuid4

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from prometheus_client import start_http_server
from psycopg.types.json import Jsonb
from redis.exceptions import RedisError

from ticketing.application.reservations import Reservations
from ticketing.config import Settings
from ticketing.domain import persistent_uuid
from ticketing.infrastructure.cache import RedisSeats
from ticketing.infrastructure.order_status_cache import RedisOrderStatusCache
from ticketing.infrastructure.order_status_projector import ORDER_EVENTS, CommittedOrderStatusProjector
from ticketing.infrastructure.payment_confirmation import PostgresPaymentConfirmation
from ticketing.infrastructure.payment_transport import CallbackTransport
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.redis_reservations import RedisReservationIntake
from ticketing.infrastructure.reservations import PostgresReservations, event
from ticketing.observability import (
    CACHE_ROWS,
    EVENT_CONSUMER_BATCH_SIZE,
    OUTBOX_AGE,
    RECONCILE_BACKLOG,
    RECONCILE_EVENTS,
    RECONCILE_FAILURES,
    RECONCILE_OVERDUE,
    RECONCILE_RECOVERED,
    RECONCILE_SECONDS,
    RECONCILE_TRACKED,
    REFRESH_ACK_BATCH_SIZE,
    REFRESH_AGE,
    REFRESH_PENDING,
    RESERVATION_COMMAND_AGE_SECONDS,
    RESERVATION_PERSISTENCE,
    RESERVATION_PERSISTENCE_BATCH_SIZE,
    RESERVATION_PERSISTENCE_FAILURES,
    RESERVATION_PERSISTENCE_PHASE_SECONDS,
    SIMULATOR_BATCH_BARRIER_SECONDS,
    SIMULATOR_DUE_TO_CLAIM_SECONDS,
    WORKER_ERRORS,
    configure_logging,
    consumer_phase,
    measured_consumer_event,
    measured_work,
    observe_consumer_batch_error,
    simulator_phase,
)
from ticketing.release_contract import runtime_value

log = logging.getLogger("ticketing.worker")
running = True


def stop(*_):
    global running
    running = False


@measured_work("reservation_write")
def persist_reservation_batch(store, intake, consumer, limit=1, *, completion_pipeline=False):
    if type(completion_pipeline) is not bool or (completion_pipeline and (type(limit) is not int or not 1 <= limit <= 4)):
        raise ValueError("Boolean completion mode and bounded batch required")
    claim_started = time.perf_counter()
    try:
        messages = list(intake.messages(consumer, count=limit))
    except Exception:
        RESERVATION_PERSISTENCE_PHASE_SECONDS.labels("redis_claim", "error").observe(
            time.perf_counter() - claim_started
        )
        raise
    RESERVATION_PERSISTENCE_PHASE_SECONDS.labels("redis_claim", "ok").observe(
        time.perf_counter() - claim_started
    )
    RESERVATION_PERSISTENCE_BATCH_SIZE.observe(len(messages))
    if not messages:
        return False

    commands = []
    for stream, message_id, fields in messages:
        payload = json.loads(fields["payload"])
        response = json.loads(fields["response"])
        created_at = float(fields.get("created_at_epoch", time.time()))
        command_age = max(0.0, time.time() - created_at)
        RESERVATION_COMMAND_AGE_SECONDS.observe(command_age)
        commands.append(
            {
                "stream": stream,
                "message_id": message_id,
                "payload": payload,
                "response": response,
                "command_age": command_age,
            }
        )

    started = time.perf_counter()
    try:
        outcomes = store.persist_reservation_commands(
            [(command["payload"], command["response"]) for command in commands]
        )
    except Exception:
        RESERVATION_PERSISTENCE_PHASE_SECONDS.labels("postgres_batch", "error").observe(
            time.perf_counter() - started
        )
        RESERVATION_PERSISTENCE.labels("transient_error").inc(len(commands))
        raise
    RESERVATION_PERSISTENCE_PHASE_SECONDS.labels("postgres_batch", "ok").observe(
        time.perf_counter() - started
    )
    if len(outcomes) != len(commands):
        RESERVATION_PERSISTENCE.labels("outcome_mismatch").inc(len(commands))
        raise RuntimeError("reservation persistence batch returned the wrong outcome count")

    if completion_pipeline and all(outcome == "durable" for outcome, _ in outcomes):
        started = time.perf_counter()
        try:
            marked = intake.mark_durable_many([
                (command["payload"]["event_id"], command["payload"]["command_id"])
                for command in commands
            ])
            if len(marked) != len(commands) or any(value != 1 for value in marked):
                RESERVATION_PERSISTENCE.labels("metadata_expired").inc()
                raise RuntimeError("reservation metadata invalid before batch acknowledgement")
        except Exception:
            RESERVATION_PERSISTENCE_PHASE_SECONDS.labels("redis_mark_durable_batch", "error").observe(time.perf_counter() - started)
            RESERVATION_PERSISTENCE.labels("mark_durable_error").inc()
            raise
        RESERVATION_PERSISTENCE_PHASE_SECONDS.labels("redis_mark_durable_batch", "ok").observe(time.perf_counter() - started)
        RESERVATION_PERSISTENCE.labels("durable").inc(len(commands))
        started = time.perf_counter()
        try:
            intake.acknowledge_many([(command["stream"], command["message_id"]) for command in commands])
        except Exception:
            RESERVATION_PERSISTENCE_PHASE_SECONDS.labels("redis_acknowledge_batch", "error").observe(time.perf_counter() - started)
            RESERVATION_PERSISTENCE.labels("acknowledgement_error").inc()
            raise
        RESERVATION_PERSISTENCE_PHASE_SECONDS.labels("redis_acknowledge_batch", "ok").observe(time.perf_counter() - started)
        return True

    failed_events = set()
    for command, (outcome, value) in zip(commands, outcomes, strict=True):
        payload = command["payload"]
        if outcome == "failed":
            error_code = str(value.code or "UNKNOWN")
            RESERVATION_PERSISTENCE.labels("failed").inc()
            RESERVATION_PERSISTENCE_FAILURES.labels(error_code).inc()
            compensate_started = time.perf_counter()
            try:
                compensated = intake.mark_failed(
                    payload["event_id"],
                    payload["command_id"],
                    payload["seat_ids"],
                    error_code,
                )
                if type(compensated) is not int or compensated != 1:
                    raise RuntimeError("reservation failure compensation was not confirmed")
            except Exception:
                RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
                    "redis_compensate", "error"
                ).observe(time.perf_counter() - compensate_started)
                RESERVATION_PERSISTENCE.labels("compensation_error").inc()
                raise
            RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
                "redis_compensate", "ok"
            ).observe(time.perf_counter() - compensate_started)
            failed_events.add(payload["event_id"])
            log.warning(
                "reservation_command_failed",
                extra={"fields": {
                    "event": "reservation_command_failed",
                    "command_id": payload["command_id"],
                    "error_code": error_code,
                    "command_age_seconds": round(command["command_age"], 6),
                }},
            )
            continue

        if outcome != "durable":
            RESERVATION_PERSISTENCE.labels("outcome_invalid").inc()
            raise RuntimeError(f"unknown reservation persistence outcome: {outcome}")
        mark_started = time.perf_counter()
        try:
            marked = intake.mark_durable(payload["event_id"], payload["command_id"])
        except Exception:
            RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
                "redis_mark_durable", "error"
            ).observe(time.perf_counter() - mark_started)
            RESERVATION_PERSISTENCE.labels("mark_durable_error").inc()
            raise
        RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
            "redis_mark_durable", "ok"
        ).observe(time.perf_counter() - mark_started)
        if marked != 1:
            RESERVATION_PERSISTENCE.labels("metadata_expired").inc()
            log.error(
                "reservation_command_metadata_expired",
                extra={"fields": {
                    "event": "reservation_command_metadata_expired",
                    "command_id": payload["command_id"],
                    "command_age_seconds": round(command["command_age"], 6),
                    "mark_result": marked,
                }},
            )
            raise RuntimeError(
                "reservation command metadata expired before durable acknowledgement"
            )
        RESERVATION_PERSISTENCE.labels("durable").inc()

    for event_id in failed_events:
        snapshot(store.db, intake.cache, event_id)

    for command in commands:
        acknowledge_started = time.perf_counter()
        try:
            intake.acknowledge(command["stream"], command["message_id"])
        except Exception:
            RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
                "redis_acknowledge", "error"
            ).observe(time.perf_counter() - acknowledge_started)
            RESERVATION_PERSISTENCE.labels("acknowledgement_error").inc()
            raise
        RESERVATION_PERSISTENCE_PHASE_SECONDS.labels(
            "redis_acknowledge", "ok"
        ).observe(time.perf_counter() - acknowledge_started)
    return True

@measured_work("snapshot")
def snapshot(db, cache, event_id):
    # Event metadata and seats come from one transaction, then one Lua PUT publishes the
    # complete intake/read snapshot atomically.
    with db.transaction() as conn:
        sale = conn.execute(
            "SELECT sale_starts,sale_ends,currency FROM events WHERE id=%s",
            (event_id,),
        ).fetchone()
        rows = conn.execute(
            """SELECT seat_id,price,hold_id,reserved_until,booked_order_id,version
            FROM event_seats WHERE event_id=%s ORDER BY seat_id""",
            (event_id,),
        ).fetchall()
    CACHE_ROWS.labels("full").inc(len(rows))
    version = sum(r["version"] for r in rows)
    seats = [
        {
            "seat_id": r["seat_id"],
            "price": r["price"],
            "status": "SOLD" if r["booked_order_id"] else "HELD" if r["hold_id"] else "AVAILABLE",
            "hold_id": str(r["hold_id"]) if r["hold_id"] else None,
            "reserved_until": r["reserved_until"],
            "reserved_until_epoch": (
                r["reserved_until"].timestamp() if r["reserved_until"] is not None else None
            ),
            "version": version,
            "source_version": r["version"],
        }
        for r in rows
    ]
    cache.put(
        str(event_id),
        version,
        {
            "event_id": str(event_id),
            "version": version,
            "sale_starts_epoch": sale["sale_starts"].timestamp(),
            "sale_ends_epoch": sale["sale_ends"].timestamp(),
            "currency": sale["currency"],
            "seats": seats,
        },
    )

@measured_work("publish_batch")
def publish_batch(db, producer, limit=32, *, observe_age=True):
    token = uuid4()
    with db.transaction() as conn:
        if observe_age:
            age = conn.execute("""SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(occurred_at)) AS age
                FROM outbox_events WHERE published_at IS NULL""").fetchone()["age"]
            OUTBOX_AGE.set(float(age or 0))
        rows = conn.execute(
            """SELECT * FROM outbox_events WHERE published_at IS NULL
            AND (lease_until IS NULL OR lease_until < clock_timestamp())
            ORDER BY occurred_at LIMIT %s FOR UPDATE SKIP LOCKED""",
            (limit,),
        ).fetchall()
        if not rows:
            return False
        conn.execute(
            """UPDATE outbox_events SET lease_until=clock_timestamp()+interval '30 seconds',lease_token=%s
            WHERE id=ANY(%s)""",
            (token, [row["id"] for row in rows]),
        )
    pending, confirmed, errors = [], [], []
    deadline = time.monotonic() + 10
    for row in rows:
        if time.monotonic() >= deadline:
            break
        envelope = {
            "event_id": str(row["id"]),
            "aggregate_id": str(row["aggregate_id"]),
            "event_type": row["event_type"],
            "schema_version": row["schema_version"],
            "occurred_at": row["occurred_at"].isoformat(),
            "payload": row["payload"],
        }
        try:
            future = producer.send("ticketing.events", key=str(row["aggregate_id"]).encode(), value=envelope)
            pending.append((row["id"], future))
        except Exception as exc:  # noqa: BLE001 - re-raised after preserving successful acknowledgements
            errors.append(exc)
            break
    for event_id, future in pending:
        try:
            future.get(timeout=max(0, deadline - time.monotonic()))
            confirmed.append(event_id)
        except Exception as exc:  # noqa: BLE001 - re-raised after preserving successful acknowledgements
            errors.append(exc)
    if confirmed:
        with db.transaction() as conn:
            conn.execute(
                """UPDATE outbox_events SET published_at=clock_timestamp(),lease_until=NULL
                WHERE id=ANY(%s) AND lease_token=%s""",
                (confirmed, token),
            )
    if errors:
        raise errors[0]
    return bool(confirmed)


def publish_one(db, producer):
    return publish_batch(db, producer, limit=1)


def request_refresh(conn, event_id, seat_ids=None):
    conn.execute(
        """INSERT INTO seat_refresh_requests(event_id,seat_ids) VALUES (%s,%s)
        ON CONFLICT(event_id) DO UPDATE SET generation=seat_refresh_requests.generation+1,
        seat_ids=CASE
            WHEN seat_refresh_requests.generation=seat_refresh_requests.completed_generation THEN EXCLUDED.seat_ids
            WHEN seat_refresh_requests.seat_ids IS NULL OR EXCLUDED.seat_ids IS NULL THEN NULL
            ELSE ARRAY(SELECT DISTINCT unnest(seat_refresh_requests.seat_ids || EXCLUDED.seat_ids)) END,
        requested_at=CASE
            WHEN seat_refresh_requests.generation=seat_refresh_requests.completed_generation
            THEN clock_timestamp() ELSE seat_refresh_requests.requested_at END""",
        (event_id, seat_ids),
    )


def changed_snapshot(db, cache, event_id, seat_ids):
    with db.transaction() as conn:
        rows = conn.execute(
            """SELECT seat_id,price,hold_id,reserved_until,booked_order_id,version
            FROM event_seats WHERE event_id=%s AND seat_id=ANY(%s) ORDER BY seat_id""",
            (event_id, seat_ids),
        ).fetchall()
    seats = [seat_state(row) for row in rows]
    CACHE_ROWS.labels("patch").inc(len(seats))
    if not cache.patch(str(event_id), seats):
        snapshot(db, cache, event_id)


def full_snapshot_batch(db, cache, event_ids):
    event_ids = list(dict.fromkeys(UUID(str(event_id)) for event_id in event_ids))
    if not event_ids:
        return set(), []
    if len(event_ids) == 1:
        try:
            snapshot(db, cache, event_ids[0])
            return {event_ids[0]}, []
        except Exception as exc:  # noqa: BLE001 - the durable lease remains retryable
            return set(), [exc]

    with db.transaction() as conn:
        rows = conn.execute(
            """SELECT event.id AS event_id,event.sale_starts,event.sale_ends,event.currency,
                seat.seat_id,seat.price,seat.hold_id,seat.reserved_until,
                seat.booked_order_id,seat.version
            FROM events AS event
            LEFT JOIN event_seats AS seat ON seat.event_id=event.id
            WHERE event.id=ANY(%s)
            ORDER BY event.id,seat.seat_id""",
            (event_ids,),
        ).fetchall()

    grouped = {
        event_id: {"metadata": None, "seats": []}
        for event_id in event_ids
    }
    for row in rows:
        group = grouped[row["event_id"]]
        group["metadata"] = row
        if row["seat_id"] is not None:
            group["seats"].append(seat_state(row))
    CACHE_ROWS.labels("full").inc(sum(len(group["seats"]) for group in grouped.values()))

    updates, errors = [], []
    for event_id in event_ids:
        group = grouped[event_id]
        metadata = group["metadata"]
        if metadata is None:
            errors.append(RuntimeError(f"event {event_id} disappeared during refresh"))
            continue
        seats = group["seats"]
        version = sum(seat["source_version"] for seat in seats)
        updates.append(
            (
                str(event_id),
                version,
                {
                    "event_id": str(event_id),
                    "version": version,
                    "sale_starts_epoch": metadata["sale_starts"].timestamp(),
                    "sale_ends_epoch": metadata["sale_ends"].timestamp(),
                    "currency": metadata["currency"],
                    "seats": seats,
                },
            )
        )
    try:
        outcomes = cache.put_many(updates)
    except Exception as exc:  # noqa: BLE001 - the complete batch remains retryable
        return set(), [*errors, exc]
    if len(outcomes) != len(updates):
        return set(), [*errors, RuntimeError("cache full batch returned the wrong outcome count")]

    completed = set()
    event_by_key = {str(event_id): event_id for event_id in event_ids}
    for update, outcome in zip(updates, outcomes, strict=True):
        if isinstance(outcome, Exception):
            errors.append(outcome)
        elif outcome:
            completed.add(event_by_key[update[0]])
        else:
            errors.append(RuntimeError(f"cache rejected full snapshot for event {update[0]}"))
    return completed, errors


def changed_snapshot_batch(db, cache, requests):
    event_ids = []
    seat_ids = []
    for request in requests:
        for seat_id in request["seat_ids"]:
            event_ids.append(request["event_id"])
            seat_ids.append(seat_id)

    rows = []
    if event_ids:
        with db.transaction() as conn:
            rows = conn.execute(
                """WITH wanted(event_id,seat_id) AS (
                    SELECT * FROM unnest(%s::uuid[],%s::text[])
                )
                SELECT seat.event_id,seat.seat_id,seat.price,seat.hold_id,
                    seat.reserved_until,seat.booked_order_id,seat.version
                FROM event_seats AS seat
                JOIN wanted USING(event_id,seat_id)
                ORDER BY seat.event_id,seat.seat_id""",
                (event_ids, seat_ids),
            ).fetchall()

    grouped = {request["event_id"]: [] for request in requests}
    for row in rows:
        grouped[row["event_id"]].append(seat_state(row))
    CACHE_ROWS.labels("patch").inc(len(rows))

    try:
        outcomes = cache.patch_many(
            (str(request["event_id"]), grouped[request["event_id"]]) for request in requests
        )
    except Exception as exc:  # noqa: BLE001 - the lease remains retryable
        return set(), [exc]
    if len(outcomes) != len(requests):
        return set(), [RuntimeError("cache patch batch returned the wrong outcome count")]

    completed, errors = set(), []
    for request, patched in zip(requests, outcomes, strict=True):
        if patched:
            completed.add(request["event_id"])
            continue
        try:
            snapshot(db, cache, request["event_id"])
            completed.add(request["event_id"])
        except Exception as exc:  # noqa: BLE001 - preserve other successful projections
            errors.append(exc)
    return completed, errors


def seat_state(row):
    return {
        "seat_id": row["seat_id"],
        "price": row["price"],
        "status": "SOLD" if row["booked_order_id"] else "HELD" if row["hold_id"] else "AVAILABLE",
        "reserved_until": row["reserved_until"],
        "reserved_until_epoch": (
            row["reserved_until"].timestamp() if row["reserved_until"] is not None else None
        ),
        "source_version": row["version"],
    }

@measured_work("refresh_batch")
def refresh_batch(db, cache, limit=2, cooldown_ms=250):
    token = uuid4()
    with db.transaction() as conn:
        state = conn.execute(
            """SELECT count(*) AS n,
            EXTRACT(EPOCH FROM clock_timestamp()-min(requested_at)) AS age
            FROM seat_refresh_requests WHERE generation>completed_generation"""
        ).fetchone()
        REFRESH_PENDING.set(state["n"])
        REFRESH_AGE.set(float(state["age"] or 0))
        rows = conn.execute(
            """SELECT * FROM seat_refresh_requests
            WHERE generation>completed_generation AND next_attempt_at<=clock_timestamp()
            AND (lease_until IS NULL OR lease_until<clock_timestamp())
            ORDER BY requested_at LIMIT %s FOR UPDATE SKIP LOCKED""",
            (limit,),
        ).fetchall()
        if not rows:
            return 0
        claimed_at = conn.execute("SELECT clock_timestamp() AS claimed_at").fetchone()["claimed_at"]
        conn.execute(
            """UPDATE seat_refresh_requests
            SET lease_token=%s,lease_until=clock_timestamp()+interval '30 seconds'
            WHERE event_id=ANY(%s)""",
            (token, [row["event_id"] for row in rows]),
        )
    # No SQL locks while writing Redis. Every snapshot is fenced by inventory version.
    completed, errors = [], []
    partial = [row for row in rows if row["seat_ids"] is not None]
    if partial:
        projected, projection_errors = changed_snapshot_batch(db, cache, partial)
        completed.extend(row for row in partial if row["event_id"] in projected)
        errors.extend(projection_errors)
    full = [row for row in rows if row["seat_ids"] is None]
    if full:
        projected, projection_errors = full_snapshot_batch(
            db, cache, [row["event_id"] for row in full]
        )
        completed.extend(row for row in full if row["event_id"] in projected)
        errors.extend(projection_errors)
    if completed:
        REFRESH_ACK_BATCH_SIZE.observe(len(completed))
        acknowledgements = [
            {
                "event_id": str(row["event_id"]),
                "generation": row["generation"],
                "claimed_at": claimed_at.isoformat(),
            }
            for row in completed
        ]
        with db.transaction() as conn:
            conn.execute(
                """WITH acknowledged AS (
                    SELECT * FROM jsonb_to_recordset(%s::jsonb)
                    AS item(event_id uuid,generation bigint,claimed_at timestamptz)
                ), locked AS MATERIALIZED (
                    SELECT request.event_id,acknowledged.generation,acknowledged.claimed_at
                    FROM seat_refresh_requests AS request
                    JOIN acknowledged USING(event_id)
                    WHERE request.lease_token=%s
                    ORDER BY request.event_id FOR UPDATE OF request
                )
                UPDATE seat_refresh_requests AS request
                SET completed_generation=locked.generation,
                    seat_ids=CASE
                        WHEN request.generation=locked.generation THEN NULL
                        ELSE request.seat_ids END,
                    lease_until=NULL,lease_token=NULL,
                    next_attempt_at=clock_timestamp()+(%s * interval '1 millisecond'),
                    requested_at=CASE
                        WHEN request.generation>locked.generation
                        THEN locked.claimed_at ELSE request.requested_at END
                FROM locked
                WHERE request.event_id=locked.event_id
                AND request.lease_token=%s""",
                (Jsonb(acknowledgements), token, cooldown_ms, token),
            )
    if errors:
        raise errors[0]
    return len(completed)


@measured_work("refresh_one")
def refresh_one(db, cache, cooldown_ms=250):
    return bool(refresh_batch(db, cache, limit=1, cooldown_ms=cooldown_ms))


@measured_work("expire_batch")
def expire_batch(service, limit=8):
    return service.expire_batch(limit)


# --- Bounded proactive reconciliation -------------------------------------------------
# The schema models scheduled inventory as `events` only. Proactive reconciliation is
# therefore defined purely by the sale window that exists today: an event is active from
# `sale_starts - window` until `sale_ends + window`. No show-end, screen or location field
# is invented. Events outside the window are still reconciled reactively through
# seat_refresh_requests when a SeatsChanged notification arrives.
ACTIVE_WINDOW = """e.sale_starts <= statement_timestamp() + (%s * interval '1 second')
    AND e.sale_ends > statement_timestamp() - (%s * interval '1 second')"""
BACKOFF_SHIFT_CAP = 5  # Retry delay tops out at base * 2**5.
BACKLOG_COUNT_CAP = 10000  # Backlog gauge saturates rather than scanning unbounded rows.
TRACKED_COUNT_CAP = 50000


def schedule_window(settings):
    return (settings.reconcile_window_seconds, settings.reconcile_window_seconds)


@measured_work("reconcile_schedule")
def maintain_schedule(db, settings):
    """Bounded seed, prune and metric sampling. Never runs inside a claim or Redis write."""
    window = schedule_window(settings)
    with db.transaction() as conn:
        seeded = conn.execute(
            f"""INSERT INTO event_reconciliation(event_id)
            SELECT e.id FROM events e
            WHERE {ACTIVE_WINDOW} AND NOT EXISTS (
                SELECT 1 FROM event_reconciliation r WHERE r.event_id=e.id)
            ORDER BY e.sale_starts LIMIT %s
            ON CONFLICT DO NOTHING""",
            (*window, settings.reconcile_seed_batch),
        ).rowcount
        # Leaving the active window drops the advisory row. It is derived state and is
        # re-seeded if the window reopens; a leased row is never removed underneath a worker.
        pruned = conn.execute(
            f"""DELETE FROM event_reconciliation WHERE event_id = ANY(ARRAY(
                SELECT r.event_id FROM event_reconciliation r JOIN events e ON e.id=r.event_id
                WHERE NOT ({ACTIVE_WINDOW})
                AND (r.lease_until IS NULL OR r.lease_until < clock_timestamp())
                ORDER BY r.event_id LIMIT %s FOR UPDATE OF r SKIP LOCKED))
                AND (lease_until IS NULL OR lease_until < clock_timestamp())""",
            (*window, settings.reconcile_seed_batch),
        ).rowcount
        row = conn.execute(
            f"""SELECT
            (SELECT count(*) FROM (SELECT 1 FROM event_reconciliation
                WHERE next_due_at<=clock_timestamp() LIMIT {BACKLOG_COUNT_CAP}) due) AS backlog,
            (SELECT count(*) FROM (SELECT 1 FROM event_reconciliation
                LIMIT {TRACKED_COUNT_CAP}) all_rows) AS tracked,
            (SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(next_due_at))
                FROM event_reconciliation) AS overdue"""
        ).fetchone()
    RECONCILE_BACKLOG.set(row["backlog"])
    RECONCILE_TRACKED.set(row["tracked"])
    RECONCILE_OVERDUE.set(max(0.0, float(row["overdue"] or 0)))
    return seeded, pruned


def claim_due_events(db, settings):
    """Claim a bounded batch under one short transaction. Returns (token, event ids)."""
    token = uuid4()
    window = schedule_window(settings)
    with db.transaction() as conn:
        rows = conn.execute(
            f"""SELECT r.event_id, r.lease_until FROM event_reconciliation r
            JOIN events e ON e.id=r.event_id
            WHERE r.next_due_at<=clock_timestamp()
            AND (r.lease_until IS NULL OR r.lease_until<clock_timestamp())
            AND {ACTIVE_WINDOW}
            ORDER BY r.next_due_at, r.event_id
            LIMIT %s FOR UPDATE OF r SKIP LOCKED""",
            (*window, settings.reconcile_batch_size),
        ).fetchall()
        if not rows:
            return token, []
        recovered = sum(1 for row in rows if row["lease_until"] is not None)
        conn.execute(
            """UPDATE event_reconciliation
            SET lease_token=%s, lease_until=clock_timestamp()+(%s * interval '1 second'),
            claimed_at=clock_timestamp() WHERE event_id=ANY(%s)""",
            (token, settings.reconcile_lease_seconds, [row["event_id"] for row in rows]),
        )
    if recovered:
        RECONCILE_RECOVERED.inc(recovered)
    return token, [row["event_id"] for row in rows]


def complete_reconciliation(db, settings, event_id, token):
    with db.transaction() as conn:
        result = conn.execute(
            """WITH completion AS MATERIALIZED (
                SELECT clock_timestamp() AS completed_at
            )
            UPDATE event_reconciliation SET last_reconciled_at=completion.completed_at,
            next_due_at=completion.completed_at+(%s * interval '1 second'),
            consecutive_failures=0, lease_until=NULL, lease_token=NULL
            FROM completion
            WHERE event_id=%s AND lease_token=%s""",
            (settings.reconcile_interval_seconds, event_id, token),
        )
        return result.rowcount == 1


def defer_reconciliation(db, settings, event_id, token):
    """Bounded exponential backoff. A stale token cannot move another worker's deadline."""
    cap = settings.reconcile_backoff_ms * 2**BACKOFF_SHIFT_CAP
    with db.transaction() as conn:
        conn.execute(
            f"""UPDATE event_reconciliation SET consecutive_failures=consecutive_failures+1,
            next_due_at=clock_timestamp() + (LEAST(
                %s * power(2, LEAST(consecutive_failures, {BACKOFF_SHIFT_CAP})), %s)
                * interval '1 millisecond'),
            lease_until=NULL, lease_token=NULL
            WHERE event_id=%s AND lease_token=%s""",
            (settings.reconcile_backoff_ms, cap, event_id, token),
        )


@measured_work("reconcile_batch")
def reconcile_batch(db, cache, settings, deadline=None):
    """One bounded batch. No SQL lock is held while `snapshot` writes Redis, and one slow
    or failing event can neither abort the batch nor block another event's progress."""
    token, event_ids = claim_due_events(db, settings)
    processed = 0
    for index, event_id in enumerate(event_ids):
        if deadline is not None and time.monotonic() >= deadline:
            with db.transaction() as conn:
                conn.execute(
                    """UPDATE event_reconciliation SET lease_until=NULL,lease_token=NULL
                    WHERE event_id=ANY(%s) AND lease_token=%s""",
                    (event_ids[index:], token),
                )
            break
        processed += 1
        started = time.monotonic()
        try:
            snapshot(db, cache, event_id)
        except Exception:
            RECONCILE_SECONDS.labels("error").observe(time.monotonic() - started)
            RECONCILE_EVENTS.labels("error").inc()
            RECONCILE_FAILURES.labels("snapshot").inc()
            log.exception("reconciliation_failed")
            try:
                defer_reconciliation(db, settings, event_id, token)
            except Exception:
                RECONCILE_FAILURES.labels("defer").inc()
                log.exception("reconciliation_defer_failed")
            continue
        try:
            acknowledged = complete_reconciliation(db, settings, event_id, token)
        except Exception:
            RECONCILE_FAILURES.labels("acknowledge").inc()
            log.exception("reconciliation_acknowledge_failed")
            RECONCILE_EVENTS.labels("ack_error").inc()
            continue
        if not acknowledged:
            RECONCILE_EVENTS.labels("stale").inc()
            continue
        RECONCILE_SECONDS.labels("ok").observe(time.monotonic() - started)
        RECONCILE_EVENTS.labels("ok").inc()
    return processed


def reconcile_pass(db, cache, settings, deadline):
    """Claim batches until the wall-clock budget is spent. Overrun is bounded by one started snapshot plus database bookkeeping,
    so hold expiry and dirty-seat processing keep their turn in the maintenance loop."""
    done = 0
    while time.monotonic() < deadline:
        claimed = reconcile_batch(db, cache, settings, deadline)
        done += claimed
        if not claimed:
            break
    return done


@measured_work("consume_event")
def consume_event(db, cache, envelope, order_status_projector=None):
    capture = None
    if (order_status_projector is not None and order_status_projector.capture_committed is True
            and envelope["event_type"] == "OrderPaid"):
        data = envelope.get("payload")
        capture = order_status_projector.begin_capture(data.get("order_id") if isinstance(data, dict) else None)
    snapshot = (_consume_event_transaction(db, cache, envelope, capture_status=True)
                if capture is not None else _consume_event_transaction(db, cache, envelope))
    if order_status_projector is not None and envelope["event_type"] in ORDER_EVENTS:
        if snapshot is not None and capture is not None:
            order_status_projector.publish_committed(snapshot, *capture)
        else:
            data = envelope.get("payload")
            order_status_projector.refresh(
                data.get("order_id") if isinstance(data, dict) else None, event_type=envelope["event_type"]
            )


@measured_consumer_event
def _consume_event_transaction(db, cache, envelope, *, capture_status=False):
    # Keep malformed events off the database on the default single-event path.
    if envelope["schema_version"] != 1:
        raise ValueError("Unsupported event schema")
    _kind, _data = envelope["event_type"], envelope["payload"]
    with db.transaction() as conn:
        return _apply_consumer_event(conn, cache, envelope, capture_status=capture_status)


def _apply_consumer_event(conn, cache, envelope, *, capture_status=False):
    if envelope["schema_version"] != 1:
        raise ValueError("Unsupported event schema")
    kind, data = envelope["event_type"], envelope["payload"]
    inserted = conn.execute(
        """INSERT INTO consumer_inbox VALUES ('fulfillment',%s)
        ON CONFLICT DO NOTHING RETURNING event_id""",
        (envelope["event_id"],),
    ).fetchone()
    if not inserted:
        return
    if kind == "SeatsChanged":
        if "seats" in data:
            request_refresh(conn, data["event_id"], data["seats"])
        else:
            request_refresh(conn, data["event_id"])
    elif kind == "OrderPaid":
        order = conn.execute(
            "SELECT * FROM orders WHERE id=%s FOR UPDATE", (data["order_id"],)
        ).fetchone()
        if order["status"] not in {"PAID", "FULFILLED"}:
            raise ValueError("OrderPaid has no paid order")
        booking_query = (
            "SELECT b.id,b.seat_id,t.id AS ticket_id FROM bookings b "
            "LEFT JOIN tickets t ON t.booking_id=b.id WHERE b.order_id=%s ORDER BY b.seat_id"
            if capture_status else "SELECT id FROM bookings WHERE order_id=%s")
        bookings = conn.execute(booking_query, (order["id"],)).fetchall()
        captured_tickets = []
        for booking in bookings:
            result = conn.execute(
                "INSERT INTO tickets(id,booking_id) VALUES (%s,%s) ON CONFLICT DO NOTHING"
                + (" RETURNING id" if capture_status else ""),
                (persistent_uuid(), booking["id"]),
            )
            if capture_status:
                inserted = result.fetchone()
                identifier = inserted["id"] if inserted else booking["ticket_id"]
                if identifier is not None:
                    captured_tickets.append({"id": identifier, "seat_id": booking["seat_id"]})
        conn.execute("UPDATE orders SET status='FULFILLED' WHERE id=%s", (order["id"],))
        event(conn, order["id"], "TicketsIssued", {"order_id": str(order["id"])})
        if capture_status and len(captured_tickets) == len(bookings) and captured_tickets:
            # Return exits the context first; a commit failure cannot release this snapshot.
            return {**order, "status": "FULFILLED", "tickets": captured_tickets}
    elif kind == "RefundRequested":
        # Simulator's financial effect is this durable row. A real gateway requires
        # a separate idempotent external dispatch/reconciliation adapter.
        conn.execute("SELECT id FROM orders WHERE id=%s FOR UPDATE", (data["order_id"],))
        conn.execute(
            "UPDATE refund_requests SET status='REFUNDED' WHERE payment_id=%s", (data["payment_id"],)
        )
        conn.execute(
            "UPDATE orders SET status='REFUNDED' WHERE id=%s AND status='REFUND_PENDING'",
            (data["order_id"],),
        )
    elif kind == "TicketsIssued":
        log.info("development_notification", extra={"fields": data})
    else:
        raise ValueError("Unknown event type")


@measured_consumer_event
def _consume_group_event(conn, cache, envelope, *, capture_status=False):
    return _apply_consumer_event(conn, cache, envelope, capture_status=capture_status)


def _consume_fulfillment_group(db, cache, envelopes, order_status_projector):
    if len(envelopes) == 1:
        consume_event(db, cache, envelopes[0], order_status_projector)
        return
    captures = []
    for envelope in envelopes:
        capture = None
        if (order_status_projector is not None and order_status_projector.capture_committed is True
                and envelope["event_type"] == "OrderPaid"):
            capture = order_status_projector.begin_capture(envelope["payload"]["order_id"])
        captures.append(capture)
    order_ids = sorted({UUID(str(e["payload"]["order_id"])) for e in envelopes
                        if e["event_type"] in {"OrderPaid", "RefundRequested"}})
    snapshots = []
    with consumer_phase("fulfillment_batch"), db.transaction() as conn:
        if order_ids:
            conn.execute("SELECT id FROM orders WHERE id=ANY(%s::uuid[]) ORDER BY id FOR UPDATE", (order_ids,)).fetchall()
        for envelope, capture in zip(envelopes, captures, strict=True):
            snapshots.append(_consume_group_event(conn, cache, envelope, capture_status=capture is not None))
    # The entire group committed and released its connection before cache work.
    if order_status_projector is not None:
        for envelope, snapshot, capture in zip(envelopes, snapshots, captures, strict=True):
            if snapshot is not None and capture is not None:
                order_status_projector.publish_committed(snapshot, *capture)
            else:
                order_status_projector.refresh(envelope["payload"]["order_id"], event_type=envelope["event_type"])


def consume_fulfillment_events(db, cache, envelopes, order_status_projector=None, *, limit=1):
    if type(limit) is not int or not 1 <= limit <= 4:
        raise ValueError("Fulfillment transaction batch must be an integer between 1 and 4")
    group, orders = [], set()
    for envelope in envelopes:
        if (not isinstance(envelope, dict) or envelope.get("schema_version") != 1
                or envelope.get("event_type") not in BUSINESS_EVENTS
                or not isinstance(envelope.get("payload"), dict)):
            raise ValueError("Validated fulfillment event required")
        order_id = UUID(str(envelope["payload"].get("order_id")))
        if group and (len(group) == limit or order_id in orders):
            _consume_fulfillment_group(db, cache, group, order_status_projector)
            group, orders = [], set()
        group.append(envelope)
        orders.add(order_id)
    if group:
        _consume_fulfillment_group(db, cache, group, order_status_projector)


@measured_work("consume_refresh_batch")
def consume_refresh_batch(db, envelopes):
    if not envelopes:
        return 0
    for envelope in envelopes:
        if envelope["schema_version"] != 1:
            raise ValueError("Unsupported event schema")
        if envelope["event_type"] != "SeatsChanged":
            raise ValueError("Only SeatsChanged events can be coalesced")

    with db.transaction(pipeline=True) as conn:
        inserted = conn.execute(
            """INSERT INTO consumer_inbox(consumer,event_id)
            SELECT 'fulfillment',event_id FROM unnest(%s::uuid[]) AS event_id
            ON CONFLICT DO NOTHING RETURNING event_id""",
            ([envelope["event_id"] for envelope in envelopes],),
        ).fetchall()
        inserted_ids = {str(row["event_id"]) for row in inserted}
        refreshes = {}
        handled_ids = set()
        for envelope in envelopes:
            envelope_id = envelope["event_id"]
            if envelope_id not in inserted_ids or envelope_id in handled_ids:
                continue
            handled_ids.add(envelope_id)
            data = envelope["payload"]
            target = data["event_id"]
            seats = data.get("seats")
            if target not in refreshes:
                refreshes[target] = None if seats is None else set(seats)
            elif refreshes[target] is not None:
                if seats is None:
                    refreshes[target] = None
                else:
                    refreshes[target].update(seats)
        # Mixed batches can touch several shows from different Kafka partitions.
        # A stable lock order prevents opposite input orders from deadlocking.
        if refreshes:
            requests = [(UUID(str(event_id)), seats) for event_id, seats in refreshes.items()]
            if len({event_id for event_id, _ in requests}) != len(requests):
                # Preserve inherited behavior for raw aliases of the same UUID.
                for event_id in sorted(refreshes, key=str):
                    seats = refreshes[event_id]
                    request_refresh(conn, event_id, None if seats is None else sorted(seats))
            else:
                rows = [{"event_id": str(event_id), "seat_ids": None if seats is None else sorted(seats)}
                        for event_id, seats in requests]
                conn.execute('INSERT INTO seat_refresh_requests(event_id,seat_ids)\n        SELECT item.event_id,item.seat_ids FROM jsonb_to_recordset(%s::jsonb)\n        AS item(event_id uuid,seat_ids text[]) ORDER BY item.event_id\n        ON CONFLICT(event_id) DO UPDATE SET generation=seat_refresh_requests.generation+1,\n        seat_ids=CASE\n            WHEN seat_refresh_requests.generation=seat_refresh_requests.completed_generation THEN EXCLUDED.seat_ids\n            WHEN seat_refresh_requests.seat_ids IS NULL OR EXCLUDED.seat_ids IS NULL THEN NULL\n            ELSE ARRAY(SELECT DISTINCT unnest(seat_refresh_requests.seat_ids || EXCLUDED.seat_ids)) END,\n        requested_at=CASE\n            WHEN seat_refresh_requests.generation=seat_refresh_requests.completed_generation\n            THEN clock_timestamp() ELSE seat_refresh_requests.requested_at END', (Jsonb(rows),))
    return len(inserted_ids)


def consume_events(db, cache, envelopes, order_status_projector=None):
    pending_refresh = []

    def flush_refresh():
        if pending_refresh:
            with consumer_phase("event_SeatsChanged"):
                consume_refresh_batch(db, pending_refresh)
            pending_refresh.clear()

    for envelope in envelopes:
        if envelope.get("event_type") == "SeatsChanged":
            pending_refresh.append(envelope)
            continue
        # Refresh intents project the latest committed inventory. Defer them
        # within this bounded partition batch; business event order is unchanged.
        if order_status_projector is None:
            consume_event(db, cache, envelope)
        else:
            consume_event(db, cache, envelope, order_status_projector=order_status_projector)
    flush_refresh()


EVENT_TOPIC = "ticketing.events"
FULFILLMENT_GROUP = "ticketing-fulfillment-v1"
PROJECTION_GROUP = "ticketing-seat-projection-v1"
BUSINESS_EVENTS = frozenset({"OrderPaid", "TicketsIssued", "RefundRequested"})
CONSUMER_LANES = frozenset({"mixed", "fulfillment", "projection"})


def event_consumer_identity(settings, role):
    if role == "projection-consumer":
        if not settings.event_consumer_separation:
            raise RuntimeError("Projection consumer requires EVENT_CONSUMER_SEPARATION=1")
        return PROJECTION_GROUP, "projection"
    if role != "consumer":
        raise ValueError("Not an event consumer role")
    return FULFILLMENT_GROUP, "fulfillment" if settings.event_consumer_separation else "mixed"


def create_event_consumer(settings, role):
    group, _ = event_consumer_identity(settings, role)
    return KafkaConsumer(
        EVENT_TOPIC,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=settings.consumer_batch_size,
        fetch_max_wait_ms=settings.consumer_batch_wait_ms,
        # Six owned partitions may exceed the client's default 1 MB frame cap.
        # Kafka can return the first batch above the fetch target; keep headroom
        # for bounded per-partition batches without requesting large responses.
        **({"fetch_max_bytes": 1024 * 1024, "receive_message_max_bytes": 8 * 1024 * 1024}
           if role == "projection-consumer" else {}),
    )


def consume_lane_events(db, cache, envelopes, order_status_projector=None, *, lane="mixed", fulfillment_batch_size=1):
    if lane not in CONSUMER_LANES:
        raise ValueError("Unknown consumer lane")
    if lane != "mixed":
        selected = []
        for envelope in envelopes:
            if not isinstance(envelope, dict) or envelope.get("schema_version") != 1:
                raise ValueError("Unsupported event schema")
            kind = envelope.get("event_type")
            if kind not in BUSINESS_EVENTS and kind != "SeatsChanged":
                raise ValueError("Unknown event type")
            if (lane == "projection" and kind == "SeatsChanged") or (
                lane == "fulfillment" and kind in BUSINESS_EVENTS
            ):
                selected.append(envelope)
        envelopes = selected
    if not envelopes:
        return
    if type(fulfillment_batch_size) is not int or not 1 <= fulfillment_batch_size <= 4:
        raise ValueError("Fulfillment transaction batch must be an integer between 1 and 4")
    if fulfillment_batch_size > 1:
        if lane != "fulfillment":
            raise ValueError("Fulfillment transaction batching requires the fulfillment lane")
        consume_fulfillment_events(db, cache, envelopes, order_status_projector, limit=fulfillment_batch_size)
        return
    # Keep mixed-mode calling conventions and the accepted batching implementation.
    if order_status_projector is None:
        consume_events(db, cache, envelopes)
    else:
        consume_events(db, cache, envelopes, order_status_projector=order_status_projector)


def dead_letter_message(db, message):
    with db.transaction() as conn:
        conn.execute(
            """INSERT INTO dead_letters(event_id,payload,error)
            VALUES (%s,%s,%s)""",
            (
                uuid4(),
                Jsonb({"raw": message.value.decode(errors="replace")}),
                "Processing failed after 5 attempts; inspect worker logs",
            ),
        )


def consume_kafka_messages(db, cache, messages, order_status_projector=None, *, lane="mixed", fulfillment_batch_size=1):
    if lane not in CONSUMER_LANES:
        raise ValueError("Unknown consumer lane")
    if (type(fulfillment_batch_size) is not int or not 1 <= fulfillment_batch_size <= 4
            or (fulfillment_batch_size > 1 and lane != "fulfillment")):
        raise ValueError("Valid fulfillment-only transaction batch required")
    EVENT_CONSUMER_BATCH_SIZE.observe(len(messages))
    batch_kwargs = {"fulfillment_batch_size": fulfillment_batch_size} if fulfillment_batch_size != 1 else {}
    for attempt in range(5):
        try:
            envelopes = [json.loads(message.value) for message in messages]
            consume_lane_events(db, cache, envelopes, order_status_projector, lane=lane, **batch_kwargs)
            return
        except Exception as exc:
            observe_consumer_batch_error(exc)
            if attempt < 4:
                time.sleep(0.2 * 2**attempt)
                continue
            log.exception("consumer_batch_failed")

    # Isolate a poison message after the bounded batch retries. Already committed
    # inbox rows make replay of valid records inexpensive and safe.
    for message in messages:
        try:
            envelope = json.loads(message.value)
            if lane != "mixed":
                consume_lane_events(db, cache, [envelope], order_status_projector, lane=lane, **batch_kwargs)
            elif order_status_projector is None:
                consume_event(db, cache, envelope)
            else:
                consume_event(db, cache, envelope, order_status_projector=order_status_projector)
        except Exception:
            dead_letter_message(db, message)
            log.exception("dead_letter")

def consume_iteration(consumer, db, cache, batch_size, order_status_projector=None, *, lane="mixed", fulfillment_batch_size=1):
    """Advance only this group after its selected events have durable outcomes."""
    if lane not in CONSUMER_LANES:
        raise ValueError("Unknown consumer lane")
    if (type(fulfillment_batch_size) is not int or not 1 <= fulfillment_batch_size <= 4
            or (fulfillment_batch_size > 1 and lane != "fulfillment")):
        raise ValueError("Valid fulfillment-only transaction batch required")
    handler_kwargs = {}
    if fulfillment_batch_size != 1:
        handler_kwargs["fulfillment_batch_size"] = fulfillment_batch_size
    if order_status_projector is not None:
        handler_kwargs["order_status_projector"] = order_status_projector
    if lane != "mixed":
        handler_kwargs["lane"] = lane
    with consumer_phase("poll"):
        batches = consumer.poll(timeout_ms=500, max_records=batch_size)
    starts = {
        TopicPartition(tp.topic, tp.partition): messages[0].offset
        for tp, messages in batches.items() if messages
    }
    try:
        for tp, messages in batches.items():
            if messages:
                with consumer_phase("partition", tp.partition):
                    consume_kafka_messages(db, cache, messages, **handler_kwargs)
    except Exception:
        for tp, offset in starts.items():
            with consumer_phase("rewind", tp.partition):
                consumer.seek(tp, offset)
        raise
    if starts:
        with consumer_phase("commit"):
            consumer.commit()
        return True
    return False


@measured_work("simulate_one")
def simulate_one(db, settings, transport=None):
    token = uuid4()
    with simulator_phase("claim"), db.transaction(pipeline=True) as conn:
        row = conn.execute("""WITH candidate AS (
            SELECT p.id,o.total,o.currency,clock_timestamp() AS claim_observed_at
            FROM payment_attempts p JOIN orders o ON o.id=p.order_id
            WHERE p.deliveries<p.target_deliveries AND p.due_at<=statement_timestamp()
            AND (p.lease_until IS NULL OR p.lease_until<clock_timestamp())
            ORDER BY p.due_at LIMIT 1 FOR UPDATE OF p SKIP LOCKED
            ) UPDATE payment_attempts p
            SET lease_until=clock_timestamp()+interval '15 seconds',lease_token=%s
            FROM candidate c WHERE p.id=c.id
            RETURNING p.*,c.total,c.currency,c.claim_observed_at""", (token,)).fetchone()
        if not row:
            return False
    return deliver_claimed_callback(db, settings, row, token, transport)


def deliver_claimed_callback(db, settings, row, token, transport=None):
    SIMULATOR_DUE_TO_CLAIM_SECONDS.observe(
        max(0, (row["claim_observed_at"] - row["due_at"]).total_seconds())
    )
    # Same delivery ID repeated intentionally; tests also exercise different IDs for one payment.
    payload = {
        "callback_id": str(row["id"]),
        "payment_id": str(row["id"]),
        "order_id": str(row["order_id"]),
        "amount": row["total"],
        "currency": row["currency"],
        "outcome": row["outcome"],
    }
    raw = json.dumps(payload).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(
        settings.webhook_secret.encode(), timestamp.encode() + b"." + raw, hashlib.sha256
    ).hexdigest()
    request = urllib.request.Request(
        runtime_value("API_URL", "http://localhost:8000") + "/v1/webhooks/payments",
        data=raw,
        headers={
            "Content-Type": "application/json",
            "X-Payment-Timestamp": timestamp,
            "X-Payment-Signature": signature,
        },
    )
    with simulator_phase("delivery"):
        if transport is None:
            with urllib.request.urlopen(request, timeout=5) as response:
                response.read()
        else:
            transport.post(raw, dict(request.header_items()))
    with simulator_phase("ack"), db.transaction(pipeline=True) as conn:
        conn.execute(
            """UPDATE payment_attempts SET deliveries=deliveries+1,lease_until=NULL
            WHERE id=%s AND lease_token=%s""",
            (row["id"], token),
        )
    return True


@measured_work("simulate_one")
def simulate_claimed_one(db, settings, row, token, transport=None):
    """Preserve observed delivery progress without double-counting legacy work."""
    return deliver_claimed_callback(db, settings, row, token, transport)


def claim_due_callbacks(db, limit):
    """Claim only free executor slots; commit fenced leases before HTTP work."""
    if type(limit) is not int or not 1 <= limit <= 16:
        raise ValueError("Callback claim batch requires1..16 free slots")
    token = uuid4()
    with simulator_phase("claim"), db.transaction(pipeline=True) as conn:
        rows = conn.execute("""WITH candidate AS (
            SELECT p.id,o.total,o.currency,clock_timestamp() AS claim_observed_at
            FROM payment_attempts p JOIN orders o ON o.id=p.order_id
            WHERE p.deliveries<p.target_deliveries AND p.due_at<=statement_timestamp()
            AND (p.lease_until IS NULL OR p.lease_until<clock_timestamp())
            ORDER BY p.due_at LIMIT %s FOR UPDATE OF p SKIP LOCKED
            ) UPDATE payment_attempts p
            SET lease_until=clock_timestamp()+interval '15 seconds',lease_token=%s
            FROM candidate c WHERE p.id=c.id
            RETURNING p.*,c.total,c.currency,c.claim_observed_at""", (limit, token)).fetchall()
    return rows, token


def simulate_refill_claim_batch(db, settings, executor, should_run=None, transport=None):
    """Bound claims by free slots; refill without a delivery-group barrier."""
    maximum = settings.simulator_concurrency
    if type(maximum) is not int or not 1 <= maximum <= 16:
        raise ValueError("Batch claim preserves at most16 independent delivery slots")
    should_run = should_run or (lambda: running)
    pending = set()
    next_poll = 0.0

    def complete(future):
        try:
            return future.result()
        except Exception:
            WORKER_ERRORS.labels("simulator").inc()
            log.exception("callback_dispatch_failed")
            return False

    try:
        while should_run():
            free = maximum - len(pending)
            now = time.monotonic()
            if free and now >= next_poll:
                try:
                    rows, token = claim_due_callbacks(db, free)
                except Exception:
                    WORKER_ERRORS.labels("simulator").inc()
                    log.exception("callback_claim_failed")
                    rows = []
                next_poll = time.monotonic() + (.1 if len(rows) < free else 0)
                for row in rows:
                    pending.add(executor.submit(simulate_claimed_one, db, settings, row, token, transport))
            if not pending:
                time.sleep(max(0, min(.1, next_poll-time.monotonic())))
                continue
            done, _ = wait(pending, timeout=.1, return_when=FIRST_COMPLETED)
            for future in done:
                pending.remove(future)
                complete(future)
    finally:
        for future in as_completed(pending):
            complete(future)


def simulate_batch(db, settings, executor, transport=None):
    futures = [executor.submit(simulate_one, db, settings, transport) for _ in range(settings.simulator_concurrency)]
    work = False
    completed_at = []
    for future in as_completed(futures):
        completed_at.append(time.monotonic())
        try:
            work = future.result() or work
        except Exception:
            WORKER_ERRORS.labels("simulator").inc()
            log.exception("callback_dispatch_failed")
    batch_end = time.monotonic()
    SIMULATOR_BATCH_BARRIER_SECONDS.inc(sum(batch_end - stamp for stamp in completed_at))
    return work



def simulate_refill(db, settings, executor, should_run=None, transport=None):
    """Keep bounded slots occupied independently; drain outstanding work on stop."""
    if getattr(settings, "simulator_batch_claim", False):
        return simulate_refill_claim_batch(db, settings, executor, should_run, transport)
    should_run = should_run or (lambda: running)
    ready_at = [0.0] * settings.simulator_concurrency
    pending = {}

    def complete(future):
        try:
            return bool(future.result())
        except Exception:
            WORKER_ERRORS.labels("simulator").inc()
            log.exception("callback_dispatch_failed")
            return False

    try:
        while should_run():
            now = time.monotonic()
            occupied = set(pending.values())
            for slot, eligible_at in enumerate(ready_at):
                if not should_run():
                    break
                if slot not in occupied and eligible_at <= now:
                    pending[executor.submit(simulate_one, db, settings, transport)] = slot
            if not pending:
                delay = max(0, min(.1, min(ready_at) - time.monotonic()))
                time.sleep(delay)
                continue
            done, _ = wait(pending, timeout=.1, return_when=FIRST_COMPLETED)
            for future in done:
                slot = pending.pop(future)
                worked = complete(future)
                ready_at[slot] = time.monotonic() + (0 if worked else .1)
    finally:
        for future in pending:
            complete(future)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "role",
        choices=["publisher", "consumer", "projection-consumer", "reservation-writer", "maintenance", "refresh", "expiry", "reconciler", "simulator", "confirmation"],
    )
    role = parser.parse_args().role
    configure_logging()
    settings = Settings()
    settings.validate()
    consumer_lane = "mixed"
    if role in {"consumer", "projection-consumer"}:
        consumer_group, consumer_lane = event_consumer_identity(settings, role)
    if role == "confirmation" and not settings.payment_confirmation_async:
        raise RuntimeError("Confirmation worker requires PAYMENT_CONFIRMATION_ASYNC=1")
    if role == "simulator" and settings.environment != "development":
        raise RuntimeError("Simulator is development only")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start_http_server(settings.worker_port)
    pool_options = {"explicit_start": True} if settings.db_explicit_transaction_start else {}
    if role == "reservation-writer":
        pool_options["idle_retention"] = True
    db, cache = Postgres(settings.database_url, settings.pool_max, **pool_options), RedisSeats(settings.redis_url, seatmap_ttl_seconds=settings.seatmap_ttl_seconds)
    store = PostgresReservations(
        db, cache, settings.hold_seconds,
        persist_write_pipeline=role == "reservation-writer" and settings.reservation_write_pipeline,
        persist_fast_batch_pipeline=role == "reservation-writer" and settings.reservation_fast_batch_pipeline,
    )
    service = Reservations(store)
    order_status_projector = (
        CommittedOrderStatusProjector(
            db, RedisOrderStatusCache(cache.redis, settings.order_status_cache_ms),
            deduplicate=settings.order_status_event_refresh_dedup,
            capture_committed=settings.order_status_committed_snapshot,
            notification_dedup=settings.order_status_notification_dedup,
        )
        if role == "consumer" and settings.order_status_event_refresh else None
    )
    intake = RedisReservationIntake(
        cache,
        hold_seconds=settings.hold_seconds,
        replica_acks=settings.redis_reservation_replica_acks,
        wait_ms=settings.redis_reservation_wait_ms,
        max_backlog=settings.redis_reservation_max_backlog,
        max_command_age_seconds=settings.redis_reservation_max_command_age_seconds,
    )
    reservation_consumer = f"{socket.gethostname()}-{os.getpid()}"
    producer = consumer = executor = callback_transport = None
    try:
        if role == "confirmation":
            PostgresPaymentConfirmation(db, settings).run(store, lambda: running)
            return
        if role == "simulator":
            callback_transport = CallbackTransport(
                runtime_value("API_URL", "http://localhost:8000") + "/v1/webhooks/payments",
                settings.simulator_concurrency,
            )
            executor = ThreadPoolExecutor(max_workers=settings.simulator_concurrency)
        if role == "publisher":
            producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap,
                acks="all",
                value_serializer=lambda v: json.dumps(v).encode(),
                retries=3,
                max_block_ms=10000,
                request_timeout_ms=10000,
            )
        if role in {"consumer", "projection-consumer"}:
            consumer = create_event_consumer(settings, role)
            log.info("event_consumer_started", extra={"fields": {
                "role": role, "group": consumer_group, "lane": consumer_lane,
            }})
        if role == "simulator" and settings.simulator_dispatch_mode == "refill":
            simulate_refill(db, settings, executor, transport=callback_transport)
            return
        next_warm = 0
        next_outbox_age_sample = 0.0
        while running:
            try:
                work = False
                if role == "publisher":
                    now = time.monotonic()
                    observe_age = now >= next_outbox_age_sample
                    if observe_age:
                        next_outbox_age_sample = now + 0.5
                    work = publish_batch(db, producer, settings.publisher_batch_size, observe_age=observe_age)
                elif role == "reservation-writer":
                    work = persist_reservation_batch(
                        store, intake, reservation_consumer, settings.reservation_writer_batch_size,
                        completion_pipeline=settings.reservation_completion_pipeline,
                    )
                elif role == "simulator":
                    work = simulate_batch(db, settings, executor, callback_transport)
                elif role == "maintenance":
                    work = (
                        refresh_batch(db, cache, settings.refresh_batch_size, settings.refresh_cooldown_ms) > 0
                    ) or work
                    work = expire_batch(service, settings.expiry_batch_size) > 0 or work
                elif role == "refresh":
                    work = (
                        refresh_batch(db, cache, settings.refresh_batch_size, settings.refresh_cooldown_ms) > 0
                    ) or work
                elif role == "expiry":
                    work = expire_batch(service, settings.expiry_batch_size) > 0 or work
                elif role == "reconciler":
                    if time.monotonic() >= next_warm:
                        # Seeding, pruning and metric sampling are bounded and infrequent;
                        # a failure here must not stop reconciliation already scheduled.
                        try:
                            maintain_schedule(db, settings)
                        except Exception:
                            RECONCILE_FAILURES.labels("schedule").inc()
                            log.exception("reconciliation_schedule_failed")
                        next_warm = time.monotonic() + 1
                    work = (
                        reconcile_pass(
                            db, cache, settings, time.monotonic() + settings.reconcile_budget_ms / 1000
                        )
                        > 0
                        or work
                    )
                else:
                    work = consume_iteration(
                        consumer, db, cache, settings.consumer_batch_size,
                        order_status_projector=order_status_projector, lane=consumer_lane,
                        **({"fulfillment_batch_size": settings.fulfillment_transaction_batch_size}
                           if consumer_lane == "fulfillment" and settings.fulfillment_transaction_batch_size > 1 else {})
                    )
                if not work:
                    time.sleep(0.1)
            except Exception as exc:
                WORKER_ERRORS.labels(role).inc()
                if role == "reservation-writer" and isinstance(exc, RedisError):
                    intake.reset_connection_state()
                log.exception("worker_iteration_failed", extra={"fields": {"role": role}})
                time.sleep(1)
    finally:
        if executor:
            executor.shutdown(wait=True)
        if callback_transport:
            callback_transport.close()
        if producer:
            producer.close(timeout=5)
        if consumer:
            consumer.close()
        db.close()
        cache.redis.close()


if __name__ == "__main__":
    main()
