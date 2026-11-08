import json
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from functools import wraps
from time import perf_counter as monotonic

from prometheus_client import Counter, Gauge, Histogram

ORDER_STATUS_CACHE = Counter(
    "ticketing_order_status_cache_total", "Advisory order snapshot outcomes", ["outcome"]
)

REQUESTS = Counter("ticketing_http_requests_total", "HTTP requests", ["route", "method", "status"])
LATENCY = Histogram("ticketing_http_seconds", "HTTP latency", ["route"])
OUTCOMES = Counter("ticketing_outcomes_total", "Business outcomes", ["operation", "outcome"])
WORKER_ERRORS = Counter("ticketing_worker_errors_total", "Worker errors", ["role"])
RESERVATION_INTAKE = Counter(
    "ticketing_reservation_intake_total",
    "Redis-first reservation intake outcomes",
    ["outcome"],
)
RESERVATION_PERSISTENCE = Counter(
    "ticketing_reservation_persistence_total",
    "Redis-first PostgreSQL writer outcomes",
    ["outcome"],
)
RESERVATION_PERSISTENCE_FAILURES = Counter(
    "ticketing_reservation_persistence_failures_total",
    "Deterministic Redis-first persistence failures by bounded domain code",
    ["code"],
)
RESERVATION_PERSISTENCE_PHASE_SECONDS = Histogram(
    "ticketing_reservation_persistence_phase_seconds",
    "Reservation-writer phase wall time",
    ["phase", "outcome"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
RESERVATION_PERSISTENCE_BATCH_SIZE = Histogram(
    "ticketing_reservation_persistence_batch_size",
    "Commands handled by one reservation-writer poll",
    buckets=(0, 1, 2, 4, 8, 16, 32, 64, 100),
)
SIMULATOR_PHASE_SECONDS = Histogram(
    "ticketing_simulator_phase_seconds", "Development callback dispatch phase wall time",
    ["phase", "outcome"], buckets=(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10),
)
SIMULATOR_DUE_TO_CLAIM_SECONDS = Histogram(
    "ticketing_simulator_due_to_claim_seconds", "DB-clock due-to-claim statement age",
    buckets=(.001, .01, .1, .25, .5, 1, 2, 5, 10, 15, 30, 60, 120),
)
SIMULATOR_BATCH_BARRIER_SECONDS = Counter(
    "ticketing_simulator_batch_barrier_seconds", "Sum of completed slot seconds parked behind a batch",
)


@contextmanager
def simulator_phase(phase):
    phase = phase if phase in {"claim", "delivery", "ack"} else "other"
    started, outcome = monotonic(), "ok"
    try:
        yield
    except Exception:
        outcome = "error"
        raise
    finally:
        SIMULATOR_PHASE_SECONDS.labels(phase, outcome).observe(monotonic() - started)


EVENT_CONSUMER_BATCH_SIZE = Histogram(
    "ticketing_event_consumer_batch_size",
    "Kafka records handled by one bounded consumer batch",
    buckets=(1, 2, 4, 8, 16, 32, 64, 100),
)
REFRESH_ACK_BATCH_SIZE = Histogram(
    "ticketing_refresh_ack_batch_size",
    "Successful refresh projections acknowledged by one PostgreSQL statement",
    buckets=(1, 2, 4, 8, 16, 32, 64, 100),
)
RESERVATION_COMMAND_AGE_SECONDS = Histogram(
    "ticketing_reservation_command_age_seconds",
    "Age of a Redis-first command when a writer handles it",
    buckets=(
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 15, 30, 60,
        90, 120, 180, 300,
    ),
)
RESERVATION_REPLICA_ACKS = Histogram(
    "ticketing_reservation_replica_acknowledgements",
    "Redis replicas acknowledging provisional reservation intake",
    buckets=(0, 1, 2, 3, 5),
)
OUTBOX_AGE = Gauge("ticketing_outbox_oldest_seconds", "Oldest unpublished event age")
REFRESH_PENDING = Gauge("ticketing_cache_refresh_pending", "Events awaiting cache refresh")
REFRESH_AGE = Gauge("ticketing_cache_refresh_oldest_seconds", "Oldest dirty cache request age")
DB_SECONDS = Histogram("ticketing_db_transaction_seconds", "Database transaction duration")
DB_ERRORS = Counter("ticketing_db_errors_total", "Database errors", ["type"])
DB_UNAVAILABLE = Counter(
    "ticketing_db_unavailable_total", "Handled database-unavailable responses", ["cause"]
)
REQUEST_ID = ContextVar("request_id", default=None)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        result = {
            "time": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        result.update(getattr(record, "fields", {}))
        if record.exc_info:
            result["exception"] = self.formatException(record.exc_info)
        return json.dumps(result, default=str)


class RequestLogFilter(logging.Filter):
    """Keep failures/diagnostics while optionally reducing duplicate success logs."""

    def filter(self, record):
        fields = getattr(record, "fields", None)
        return not (
            record.levelno < logging.WARNING and record.msg == "request"
            and isinstance(fields, dict) and type(fields.get("status")) is int
            and 200 <= fields["status"] < 400
            and not any(fields.get(key) for key in ("error_code", "db_failure_type", "close_source"))
        )


def configure_logging():
    from ticketing.release_contract import runtime_value

    success_logs = runtime_value("HTTP_SUCCESS_LOGS", "1")
    if success_logs not in {"0", "1"}:
        raise ValueError("HTTP_SUCCESS_LOGS must explicitly be0or1")
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    if success_logs == "0":
        handler.addFilter(RequestLogFilter())
    # Application failures and uvicorn.error remain; access records duplicate
    # the request metrics and the application's independently preserved errors.
    logging.getLogger("uvicorn.access").disabled = success_logs == "0"
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


# Fixed labels only: no SQL text, identifiers or request payloads.
DB_QUERY_SECONDS = Histogram(
    "ticketing_db_query_seconds", "Client execute time including network/pooler", ["command"]
)
DB_TRANSACTION_BODY_SECONDS = Histogram(
    "ticketing_db_transaction_body_seconds", "Transaction body execution time excluding commit/rollback and pool return"
)
DB_COMMIT_SECONDS = Histogram("ticketing_db_commit_seconds", "Time spent committing an API transaction")
DB_ROLLBACK_SECONDS = Histogram("ticketing_db_rollback_seconds", "Time spent rolling back API transactions")
DB_POOL_SECONDS = Histogram(
    "ticketing_db_pool_acquire_seconds", "Pool acquisition including failures", ["outcome"]
)
DB_ACQUISITION_FAILURES = Counter(
    "ticketing_db_acquisition_failures_total", "Connection checkout failures by fixed admission reason",
    ["role", "reason"],
)
DB_POOL_RETURN_SECONDS = Histogram(
    "ticketing_db_pool_return_seconds", "Time spent returning a DB connection to the pool"
)
DB_CONNECTION_HOLD_SECONDS = Histogram(
    "ticketing_db_connection_hold_seconds",
    "Time from successful pool checkout through return of the connection",
)
DB_POOL_ACQUIRING = Gauge("ticketing_db_pool_acquiring", "Threads acquiring a connection")
DB_POOL_IN_USE = Gauge("ticketing_db_pool_in_use", "Connections checked out")
DB_POOL_STATE = Gauge("ticketing_db_pool_state", "Last sampled pool state", ["state"])
EVENT_LOOP_LAG_SECONDS = Histogram(
    "ticketing_event_loop_lag_seconds",
    "Delay beyond the API event-loop probe schedule",
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.25, 0.5, 1),
)
EVENT_LOOP_LAG_CURRENT_SECONDS = Gauge(
    "ticketing_event_loop_lag_current_seconds",
    "Most recently observed API event-loop scheduling delay",
)

WORK_SECONDS = Counter(
    "ticketing_worker_busy_seconds_total", "Operation wall time including I/O", ["operation"]
)
WORK_ACTIVE = Gauge("ticketing_worker_active", "Concurrent worker operations", ["operation"])
WORK_CALLS = Counter("ticketing_worker_operations_total", "Completed worker calls", ["operation", "outcome"])
CACHE_ROWS = Counter("ticketing_cache_rows_total", "Rows sent to cache", ["mode"])

HTTP_CONNECTION_AGE_SECONDS = Histogram(
    "ticketing_http_connection_age_seconds",
    "Approximate server-visible connection age when request completed",
    buckets=(0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60),
)
HTTP_CONNECTION_CLOSE_TOTAL = Counter(
    "ticketing_http_connection_close_total",
    "Connection close or reconnect reason observed by request middleware",
    ["source"],
)
# Reconciliation scheduling. Labels are fixed vocabularies: never event or seat identifiers.
RECONCILE_BACKLOG = Gauge(
    "ticketing_reconciliation_backlog", "Active events due for reconciliation, counted up to a cap"
)
RECONCILE_OVERDUE = Gauge(
    "ticketing_reconciliation_overdue_seconds", "Age of the oldest reconciliation deadline already passed"
)
RECONCILE_TRACKED = Gauge(
    "ticketing_reconciliation_tracked_events", "Events currently held in the reconciliation schedule"
)
RECONCILE_EVENTS = Counter(
    "ticketing_reconciliation_events_total", "Scheduled reconciliations by outcome", ["outcome"]
)
RECONCILE_FAILURES = Counter(
    "ticketing_reconciliation_failures_total", "Reconciliation failures by stage", ["stage"]
)
RECONCILE_SECONDS = Histogram(
    "ticketing_reconciliation_seconds",
    "Per-event reconciliation wall time including Redis I/O",
    ["outcome"],
)
RECONCILE_RECOVERED = Counter(
    "ticketing_reconciliation_recovered_leases_total", "Expired reconciliation leases reclaimed"
)
CONSUMER_BATCH_FAILURES = Counter(
    "ticketing_consumer_batch_failures_total", "Consumer batch failed attempts including recovery", ["sqlstate"]
)


CONSUMER_FAILURE_CLASSES = Counter(
    "ticketing_consumer_failure_classes_total", "Bounded consumer exception categories", ["category"]
)


def observe_consumer_batch_error(exc):
    category = type(exc).__name__
    allowed_classes = {"ValueError", "KeyError", "TypeError", "OperationalError", "InterfaceError",
                       "PoolTimeout", "TimeoutError", "ConnectionError", "KafkaError", "KafkaTimeoutError",
                       "UnknownTopicOrPartitionError", "NoBrokersAvailable"}
    CONSUMER_FAILURE_CLASSES.labels(category if category in allowed_classes else "other").inc()
    state = getattr(exc, "sqlstate", None)
    allowed = {"40P01", "40001", "55P03", "57014", "08006", "53300"}
    CONSUMER_BATCH_FAILURES.labels(state if state in allowed else "other").inc()


CONSUMER_PHASE_SECONDS = Histogram(
    "ticketing_consumer_phase_seconds", "Consumer phase wall time including I/O",
    ["phase", "partition", "outcome"],
    buckets=(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10),
)


@contextmanager
def consumer_phase(phase, partition="all"):
    allowed = {"poll", "partition", "commit", "rewind", "event_SeatsChanged",
               "event_OrderPaid", "event_TicketsIssued", "event_RefundRequested"}
    phase = phase if phase in allowed else "event_other"
    label = str(partition)
    label = label if label == "all" or label.isdigit() and 0 <= int(label) <= 31 else "other"
    started, outcome = monotonic(), "ok"
    try:
        yield
    except Exception:
        outcome = "error"
        raise
    finally:
        CONSUMER_PHASE_SECONDS.labels(phase, label, outcome).observe(monotonic() - started)


def measured_consumer_event(fn):
    @wraps(fn)
    def wrapped(db, cache, envelope, **kwargs):
        with consumer_phase("event_" + str(envelope.get("event_type", "other"))):
            return fn(db, cache, envelope, **kwargs)
    return wrapped


def measured_work(operation):
    def decorate(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            start, outcome = monotonic(), "ok"
            WORK_ACTIVE.labels(operation).inc()
            try:
                return fn(*args, **kwargs)
            except Exception:
                outcome = "error"
                raise
            finally:
                WORK_ACTIVE.labels(operation).dec()
                WORK_SECONDS.labels(operation).inc(monotonic() - start)
                WORK_CALLS.labels(operation, outcome).inc()

        return wrapped

    return decorate


HOLD_TRACE = ContextVar("hold_trace", default=None)
HOLD_INFLIGHT = Gauge("ticketing_hold_inflight", "Admitted holds in this API process")
HOLD_LIMIT = Gauge("ticketing_hold_limit", "Configured hold limit in this API process")
HOLD_ADMISSION = Counter("ticketing_hold_admission_total", "Hold admission decisions", ["outcome"])
HOLD_OCCUPANCY = Histogram(
    "ticketing_hold_arrival_occupancy", "In-flight holds observed at each hold arrival",
    buckets=(0, 1, 2, 4, 6, 8, 12, 16, 32, 64),
)
HOLD_PHASE = Histogram(
    "ticketing_hold_phase_seconds", "Hold stage wall time including I/O", ["phase", "outcome"],
    buckets=(.001, .005, .01, .025, .05, .1, .2, .3, .5, 1, 2),
)


def observe_hold_phase(phase, seconds, outcome="ok"):
    HOLD_PHASE.labels(phase, outcome).observe(seconds)
    trace = HOLD_TRACE.get()
    if trace is not None:
        trace[phase] = trace.get(phase, 0) + seconds * 1000


@contextmanager
def hold_phase(phase):
    start, outcome = monotonic(), "ok"
    try:
        yield
    except BaseException:
        outcome = "error"
        raise
    finally:
        observe_hold_phase(phase, monotonic() - start, outcome)


class TimedHoldResource:
    """Measure entry/exit while preserving the wrapped context manager's semantics."""

    def __init__(self, phase, resource):
        self.phase, self.resource = phase, resource

    def __enter__(self):
        with hold_phase(self.phase + "_enter"):
            return self.resource.__enter__()

    def __exit__(self, *exc):
        with hold_phase(self.phase + "_exit"):
            return self.resource.__exit__(*exc)


SEAT_DELTA_OUTCOMES = Counter(
    "ticketing_seat_delta_outcomes_total",
    "Versioned seat-map delta outcomes",
    ["outcome"],
)
SEAT_DELTA_RESETS = Counter(
    "ticketing_seat_delta_resets_total",
    "Seat-map delta reset fallbacks by bounded cause",
    ["reason"],
)
SEAT_DELTA_PHASE_SECONDS = Histogram(
    "ticketing_seat_delta_phase_seconds",
    "Seat-map delta read wall time by bounded phase",
    ["phase"],
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1),
)
SEAT_DELTA_RAW_ENTRIES = Histogram(
    "ticketing_seat_delta_raw_entries",
    "Retained history entries returned by one Redis delta read",
    buckets=(0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512),
)
SEAT_DELTA_RESULT_SEATS = Histogram(
    "ticketing_seat_delta_result_seats",
    "Distinct seat states returned by one delta response",
    buckets=(0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 300),
)
BROWSE_BODY_OUTCOMES = Counter(
    "ticketing_browse_body_total", "Redis-validated browse representation outcomes", ["outcome"]
)
BROWSE_BODY_BYTES = Gauge("ticketing_browse_body_bytes", "Retained serialized browse payload bytes")
BROWSE_BODY_ENTRIES = Gauge("ticketing_browse_body_entries", "Retained serialized browse representations")


PAYMENT_RECEIPTS = Counter(
    "ticketing_payment_receipts_total", "Durable callback receipt outcomes", ["outcome"]
)
PAYMENT_CONFIRMATIONS = Counter(
    "ticketing_payment_confirmations_total", "Financial confirmation outcomes", ["outcome"]
)
PAYMENT_CONFIRMATION_PHASE = Histogram(
    "ticketing_payment_confirmation_phase_seconds", "Receipt and confirmation phase duration",
    ["phase"], buckets=(.001,.005,.01,.025,.05,.1,.25,.5,1,2,5,10,30),
)
PAYMENT_CONFIRMATION_AGE = Histogram(
    "ticketing_payment_confirmation_age_seconds", "Receipt age when financial confirmation commits",
    buckets=(.01,.05,.1,.25,.5,1,2,5,10,30,60,120),
)
PAYMENT_CONFIRMATION_PENDING = Gauge(
    "ticketing_payment_confirmation_pending", "Unresolved durable receipts including review"
)
PAYMENT_CONFIRMATION_REVIEW = Gauge(
    "ticketing_payment_confirmation_review", "Receipts requiring explicit operator reconciliation"
)
PAYMENT_CONFIRMATION_OLDEST = Gauge(
    "ticketing_payment_confirmation_oldest_seconds", "Oldest unresolved durable receipt age"
)
