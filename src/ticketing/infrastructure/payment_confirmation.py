"""Durable receipt boundary; PostgreSQL owns both admission and financial completion."""
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from psycopg import OperationalError
from psycopg.types.json import Jsonb
from psycopg_pool import PoolTimeout, TooManyRequests

from ticketing.domain import Failure
from ticketing.infrastructure.reservations import digest
from ticketing.observability import (
    PAYMENT_CONFIRMATION_AGE,
    PAYMENT_CONFIRMATION_OLDEST,
    PAYMENT_CONFIRMATION_PENDING,
    PAYMENT_CONFIRMATION_PHASE,
    PAYMENT_CONFIRMATION_REVIEW,
    PAYMENT_CONFIRMATIONS,
    PAYMENT_RECEIPTS,
)

log = logging.getLogger("ticketing.confirmation")


class LeaseLost(Exception):
    """A transaction must roll back rather than apply another worker's receipt."""


def transient(exc):
    state = getattr(exc, "sqlstate", "") or ""
    return (isinstance(exc, (OperationalError, PoolTimeout, TooManyRequests)) or
            state in {"55P03", "40001", "40P01", "57014"} or state.startswith("08"))


def retry_delay_ms(attempt, base, random_fraction=None):
    fraction = random.random() if random_fraction is None else random_fraction
    capped = min(30000, base * 2 ** min(max(attempt - 1, 0), 20))
    return max(1, int(capped * (.5 + .5 * fraction)))


class PostgresPaymentConfirmation:
    def __init__(self, db, settings):
        self.db, self.settings = db, settings
        self.provider = settings.payment_callback_provider

    def receive(self, payload):
        # Only authenticated, schema-validated ingress may call this port.
        with PAYMENT_CONFIRMATION_PHASE.labels("receipt").time(), self.db.transaction() as conn:
            conn.execute("INSERT INTO payment_receipt_capacity(provider) VALUES (%s) ON CONFLICT DO NOTHING",
                         (self.provider,))
            row = conn.execute(
                """INSERT INTO payment_webhook_receipts(provider,callback_id,payload_hash,payload)
                VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING callback_id""",
                (self.provider, payload["callback_id"], digest(payload), Jsonb(payload)),
            ).fetchone()
            if row:
                capacity = conn.execute(
                    """UPDATE payment_receipt_capacity SET outstanding=outstanding+1
                    WHERE provider=%s AND outstanding<%s RETURNING outstanding""",
                    (self.provider, self.settings.confirmation_max_pending),
                ).fetchone()
                if not capacity:
                    PAYMENT_RECEIPTS.labels("overload").inc()
                    raise Failure("CONFIRMATION_BACKLOG_FULL", 503)
                outcome = "received"
            else:
                previous = conn.execute(
                    "SELECT payload_hash FROM payment_webhook_receipts WHERE provider=%s AND callback_id=%s",
                    (self.provider, payload["callback_id"]),
                ).fetchone()
                if previous["payload_hash"] != digest(payload):
                    PAYMENT_RECEIPTS.labels("mismatch").inc()
                    raise Failure("CALLBACK_MISMATCH")
                outcome = "replay"
        PAYMENT_RECEIPTS.labels(outcome).inc()
        return {"status": "received", "receipt_id": payload["callback_id"]}

    def claim(self):
        token = uuid4()
        with PAYMENT_CONFIRMATION_PHASE.labels("claim").time(), self.db.transaction() as conn:
            row = conn.execute(
                """SELECT * FROM payment_webhook_receipts WHERE provider=%s
                AND status IN ('RECEIVED','RETRY','PROCESSING')
                AND next_attempt_at<=clock_timestamp()
                AND (lease_until IS NULL OR lease_until<clock_timestamp())
                ORDER BY next_attempt_at,received_at LIMIT 1 FOR UPDATE SKIP LOCKED""",
                (self.provider,),
            ).fetchone()
            if not row:
                return None
            return conn.execute(
                """UPDATE payment_webhook_receipts SET status='PROCESSING',attempts=attempts+1,
                lease_token=%s,lease_until=clock_timestamp()+(%s*interval '1 second')
                WHERE provider=%s AND callback_id=%s RETURNING *""",
                (token, self.settings.confirmation_lease_seconds, self.provider, row["callback_id"]),
            ).fetchone()

    def apply(self, receipt, store):
        with PAYMENT_CONFIRMATION_PHASE.labels("financial").time(), self.db.transaction() as conn:
            row = conn.execute(
                """SELECT *,clock_timestamp() AS observed_at FROM payment_webhook_receipts
                WHERE provider=%s AND callback_id=%s AND status='PROCESSING'
                AND lease_token=%s AND lease_until>clock_timestamp() FOR UPDATE NOWAIT""",
                (self.provider, receipt["callback_id"], receipt["lease_token"]),
            ).fetchone()
            if not row:
                raise LeaseLost()
            if digest(row["payload"]) != row["payload_hash"]:
                raise Failure("RECEIPT_CORRUPT")
            result = store.apply_callback(conn, row["payload"])
            completed = conn.execute(
                """UPDATE payment_webhook_receipts SET status='COMPLETED',result=%s,
                completed_at=clock_timestamp(),lease_until=NULL,lease_token=NULL,error_code=NULL
                WHERE provider=%s AND callback_id=%s AND lease_token=%s
                AND lease_until>clock_timestamp() RETURNING completed_at""",
                (Jsonb(result), self.provider, row["callback_id"], receipt["lease_token"]),
            ).fetchone()
            if not completed:
                raise LeaseLost()
            capacity = conn.execute(
                """UPDATE payment_receipt_capacity SET outstanding=outstanding-1
                WHERE provider=%s AND outstanding>0 RETURNING outstanding""", (self.provider,),
            ).fetchone()
            if not capacity:
                raise RuntimeError("Receipt capacity invariant failed")
            age = max(0, (completed["completed_at"] - row["received_at"]).total_seconds())
        PAYMENT_CONFIRMATIONS.labels("completed").inc()
        PAYMENT_CONFIRMATION_AGE.observe(age)
        return result

    def defer(self, receipt, exc):
        retry = transient(exc) and receipt["attempts"] < self.settings.confirmation_max_attempts
        state = "RETRY" if retry else "REVIEW"
        # Bounded error taxonomy; never persist/log callback payload or exception text.
        code = getattr(exc, "code", None) if isinstance(exc, Failure) else None
        code = code or (getattr(exc, "sqlstate", None) if transient(exc) else "PROCESSING_FAILED") or "DB_UNAVAILABLE"
        delay = retry_delay_ms(receipt["attempts"], self.settings.confirmation_retry_ms)
        with self.db.transaction() as conn:
            row = conn.execute(
                """UPDATE payment_webhook_receipts SET status=%s,error_code=%s,
                next_attempt_at=clock_timestamp()+(%s*interval '1 millisecond'),lease_until=NULL,lease_token=NULL
                WHERE provider=%s AND callback_id=%s AND status='PROCESSING'
                AND lease_token=%s AND lease_until>clock_timestamp() RETURNING callback_id""",
                (state, code, delay, self.provider, receipt["callback_id"], receipt["lease_token"]),
            ).fetchone()
        if row:
            PAYMENT_CONFIRMATIONS.labels(state.lower()).inc()
            log.warning("payment_confirmation_deferred", extra={"fields": {"state": state, "code": code}})
        return bool(row)

    def process_one(self, store):
        receipt = self.claim()
        if receipt is None:
            return False
        try:
            if receipt["attempts"] > self.settings.confirmation_max_attempts:
                raise Failure("CONFIRMATION_ATTEMPTS_EXHAUSTED")
            self.apply(receipt, store)
        except LeaseLost:
            PAYMENT_CONFIRMATIONS.labels("lease_lost").inc()
        except Exception as exc:  # noqa: BLE001 - persist unresolved work rather than lose a receipt
            self.defer(receipt, exc)
        return True

    def sample(self):
        with self.db.transaction() as conn:
            row = conn.execute(
                """SELECT count(*) AS pending,count(*) FILTER (WHERE status='REVIEW') AS review,
                coalesce(extract(epoch FROM clock_timestamp()-min(received_at)),0) AS oldest
                FROM payment_webhook_receipts WHERE provider=%s AND status<>'COMPLETED'""",
                (self.provider,),
            ).fetchone()
        PAYMENT_CONFIRMATION_PENDING.set(row["pending"])
        PAYMENT_CONFIRMATION_REVIEW.set(row["review"])
        PAYMENT_CONFIRMATION_OLDEST.set(max(0,float(row["oldest"])))
        return row

    def run(self, store, should_run):
        def slot():
            while should_run():
                try:
                    worked = self.process_one(store)
                except Exception:  # noqa: BLE001 - durable lease permits recovery
                    # Failed retry persistence leaves the durable lease recoverable.
                    PAYMENT_CONFIRMATIONS.labels("worker_error").inc()
                    worked = False
                if not worked:
                    time.sleep(.1)
        with ThreadPoolExecutor(max_workers=self.settings.confirmation_concurrency) as executor:
            futures = [executor.submit(slot) for _ in range(self.settings.confirmation_concurrency)]
            next_sample = 0.0
            while should_run():
                if time.monotonic() >= next_sample:
                    try:
                        self.sample()
                    except Exception:  # noqa: BLE001 - metrics must not stop processing
                        PAYMENT_CONFIRMATIONS.labels("metrics_error").inc()
                    next_sample = time.monotonic() + 1
                time.sleep(.1)
            for future in futures:
                future.result()
