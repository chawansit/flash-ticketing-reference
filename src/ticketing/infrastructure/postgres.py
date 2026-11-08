import logging
from contextlib import ExitStack, contextmanager, nullcontext
from threading import Lock
from time import perf_counter as monotonic

from psycopg import Cursor
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout, TooManyRequests

from ticketing.infrastructure.slot_diagnostics import (
    ACTIVE_LEASE,
    SlotDiagnostics,
    phase,
    query_phase,
    safely,
)
from ticketing.observability import (
    DB_ACQUISITION_FAILURES,
    DB_COMMIT_SECONDS,
    DB_CONNECTION_HOLD_SECONDS,
    DB_ERRORS,
    DB_POOL_ACQUIRING,
    DB_POOL_IN_USE,
    DB_POOL_RETURN_SECONDS,
    DB_POOL_SECONDS,
    DB_POOL_STATE,
    DB_QUERY_SECONDS,
    DB_ROLLBACK_SECONDS,
    DB_SECONDS,
    DB_TRANSACTION_BODY_SECONDS,
    REQUEST_ID,
)

logger = logging.getLogger(__name__)
SLOW_DB_PHASE_SECONDS = 0.1


def record_slow_db_phase(phase: str, duration: float, outcome: str = "ok") -> None:
    if duration >= SLOW_DB_PHASE_SECONDS:
        logger.warning(
            "slow_db_phase",
            extra={
                "fields": {
                    "event": "slow_db_phase",
                    "phase": phase,
                    "duration_ms": round(duration * 1000, 3),
                    "outcome": outcome,
                }
            },
        )


class MeasuredCursor(Cursor):
    def execute(self, query, params=None, **kwargs):
        command = (
            query.lstrip().split(None, 1)[0].upper() if isinstance(query, str) and query.strip() else "OTHER"
        )
        if command not in {"SELECT", "INSERT", "UPDATE", "DELETE", "SET", "BEGIN", "COMMIT", "ROLLBACK"}:
            command = "OTHER"
        started = monotonic()
        try:
            with query_phase(command):
                return super().execute(query, params, **kwargs)
        finally:
            DB_QUERY_SECONDS.labels(command).observe(monotonic() - started)


class Postgres:
    def __init__(self, url: str, maximum: int = 12, wait_ms: int = 150, maximum_waiting: int | None = None, *, explicit_start=False, idle_retention=False):
        if type(explicit_start) is not bool:
            raise TypeError("Explicit transaction start selection must be boolean")
        if type(idle_retention) is not bool:
            raise TypeError("Idle retention selection must be boolean")
        self.pool = ConnectionPool(
            url,
            open=True,
            min_size=0 if idle_retention else 1,
            max_size=maximum,
            timeout=wait_ms / 1000,
            max_waiting=maximum if maximum_waiting is None else maximum_waiting,
            kwargs={"row_factory": dict_row, "prepare_threshold": None, "cursor_factory": MeasuredCursor, "autocommit": explicit_start},
            **({"max_idle": 5.0} if idle_retention else {}),
        )

    @contextmanager
    def transaction(self, *, pipeline=False):
        def _record_error(exc):
            state = getattr(exc, "sqlstate", None)
            if state or type(exc).__name__ in {"PoolTimeout", "TooManyRequests"}:
                DB_ERRORS.labels(state or type(exc).__name__).inc()

        error_recorded = {"value": False}
        try:
            with self.connection() as conn:
                started = monotonic()
                body_started = monotonic()
                body_done = None
                transaction_started = False
                try:
                    with conn.pipeline() if pipeline else nullcontext():
                        phase("setup")
                        # The implicit driver begins before the setup SELECT.
                        # Autocommit connections still need an explicit boundary.
                        if getattr(conn, "autocommit", True):
                            conn.execute("BEGIN")
                        transaction_started = True
                        conn.execute(
                            "SELECT set_config('lock_timeout', '75ms', true), "
                            "set_config('statement_timeout', '1500ms', true), "
                            "set_config('idle_in_transaction_session_timeout', '3s', true)"
                        )
                        phase("body")
                        yield conn

                    body_done = monotonic()
                    DB_TRANSACTION_BODY_SECONDS.observe(body_done - body_started)

                    phase("commit")
                    commit_started = monotonic()
                    commit_outcome = "error"
                    try:
                        conn.commit()
                        commit_outcome = "ok"
                    finally:
                        commit_duration = monotonic() - commit_started
                        DB_COMMIT_SECONDS.observe(commit_duration)
                        record_slow_db_phase("commit", commit_duration, commit_outcome)
                except Exception as exc:
                    error_recorded["value"] = True
                    body_done = body_done or monotonic()
                    DB_TRANSACTION_BODY_SECONDS.observe(body_done - body_started)

                    phase("rollback")
                    rollback_started = monotonic()
                    try:
                        if transaction_started:
                            conn.rollback()
                    finally:
                        DB_ROLLBACK_SECONDS.observe(monotonic() - rollback_started)

                    _record_error(exc)
                    raise
                finally:
                    if body_done is None:
                        body_done = monotonic()
                        DB_TRANSACTION_BODY_SECONDS.observe(body_done - body_started)
                    DB_SECONDS.observe(monotonic() - started)
        except Exception as exc:
            if not error_recorded["value"]:
                _record_error(exc)
            raise

    def readonly_rows(self, query, params=None):
        """ADR0399: acknowledge one read-only pipeline, including COMMIT, before fetching."""
        try:
            with self.connection() as conn:
                started = monotonic()
                try:
                    if not conn.autocommit or conn.info.transaction_status != 0:
                        raise ValueError("Queued read requires an idle explicit-start connection")
                    with conn.pipeline():
                        phase("setup")
                        conn.execute("BEGIN READ ONLY")
                        conn.execute(
                            "SELECT set_config('lock_timeout', '75ms', true), "
                            "set_config('statement_timeout', '1500ms', true), "
                            "set_config('idle_in_transaction_session_timeout', '3s', true)"
                        )
                        phase("body")
                        result = conn.execute(query, params)
                        phase("commit")
                        committed = conn.execute("COMMIT")
                    if committed.statusmessage != "COMMIT":
                        raise RuntimeError("Read-only transaction commit was not acknowledged")
                    return result.fetchall()
                except Exception:
                    phase("rollback")
                    rollback_started = monotonic()
                    try:
                        conn.rollback()
                    finally:
                        DB_ROLLBACK_SECONDS.observe(monotonic() - rollback_started)
                    raise
                finally:
                    DB_SECONDS.observe(monotonic() - started)
        except Exception as exc:
            state = getattr(exc, "sqlstate", None)
            if state or type(exc).__name__ in {"PoolTimeout", "TooManyRequests"}:
                DB_ERRORS.labels(state or type(exc).__name__).inc()
            raise

    @contextmanager
    def connection(self):
        start, outcome = monotonic(), "error"
        conn = None
        acquired_at = None
        diagnostics = getattr(self, "_slot_diagnostics", None)
        lease = context = None
        DB_POOL_ACQUIRING.inc()
        try:
            conn = self.pool.getconn()
            acquired_at = monotonic()
            if diagnostics is not None:
                lease = safely(diagnostics, "checkout", self._diagnostic_role, conn)
                context = ACTIVE_LEASE.set((diagnostics, lease))
            outcome = "ok"
            DB_POOL_SECONDS.labels(outcome).observe(acquired_at - start)
            DB_POOL_ACQUIRING.dec()
            self.sample_pool()
            DB_POOL_IN_USE.inc()
            try:
                with conn:
                    try:
                        yield conn
                    finally:
                        phase("context_exit")
            finally:
                phase("pool_return")
                return_started = monotonic()
                return_outcome = "error"
                try:
                    if conn is not None:
                        self.pool.putconn(conn)
                    return_outcome = "ok"
                finally:
                    if diagnostics is not None:
                        safely(diagnostics, "returned", lease, return_outcome == "ok")
                        ACTIVE_LEASE.reset(context)
                    return_duration = monotonic() - return_started
                    DB_POOL_RETURN_SECONDS.observe(return_duration)
                    record_slow_db_phase("pool_return", return_duration, return_outcome)
                DB_CONNECTION_HOLD_SECONDS.observe(monotonic() - acquired_at)
                if conn is not None:
                    DB_POOL_IN_USE.dec()
                self.sample_pool()
        except (PoolTimeout, TooManyRequests) as exc:
            evidence = getattr(exc, "acquisition_failure", None)
            if diagnostics is not None:
                if evidence is None:
                    reason = "native_timeout" if isinstance(exc, PoolTimeout) else "native_limit"
                    evidence = {"role": self._diagnostic_role, "reason": reason}
                    DB_ACQUISITION_FAILURES.labels(self._diagnostic_role, reason).inc()
                captured = safely(diagnostics, "failure", evidence)
                if captured is not None:
                    evidence = {**evidence, "slot_ownership": captured}
                    exc.acquisition_failure = evidence
            if evidence is not None:
                try:
                    logger.warning("db_acquisition_failure", extra={"fields": {
                        "event": "db_acquisition_failure", "request_id": REQUEST_ID.get(), **evidence,
                    }})
                except Exception:  # noqa: BLE001, S110 - logs must not mask the original DB failure.
                    pass
            raise
        finally:
            if outcome == "error":
                DB_POOL_SECONDS.labels(outcome).observe(monotonic() - start)
                # Keep acquisition telemetry consistent when checkout fails.
                DB_POOL_ACQUIRING.dec()
                self.sample_pool()
            else:
                # On success, return path already decremented DB_POOL_ACQUIRING.
                pass

    def sample_pool(self, *, force=False):
        if getattr(self, "pool_state_on_scrape", False) and not force:
            return
        roles = getattr(self, "_metric_pool_roles", None)
        waiter_limits = getattr(self, "_metric_waiter_limits", {})
        pools = roles if roles is not None else {"general": self.pool}
        states = {role: pool.get_stats() for role, pool in pools.items()}
        for name in ("pool_size", "pool_available", "requests_waiting", "pool_max"):
            DB_POOL_STATE.labels(name).set(sum(stats.get(name, 0) for stats in states.values()))
            if roles is not None:
                for role in ("general", "payment"):
                    DB_POOL_STATE.labels(f"{role}_{name}").set(states.get(role, {}).get(name, 0))
        if roles is not None:
            shared = getattr(self, "_shared_acquisition_budget", None)
            DB_POOL_STATE.labels("pool_max_waiting").set(shared.maximum if shared else sum(waiter_limits.values()))
            budget_state = shared.snapshot() if shared else {"used": 0, "acquiring": 0, "retained": 0}
            DB_POOL_STATE.labels("shared_acquisition_limit").set(shared.maximum if shared else 0)
            for name in ("used", "acquiring", "retained"):
                DB_POOL_STATE.labels(f"shared_acquisition_{name}").set(budget_state[name])
            for role in ("general", "payment"):
                DB_POOL_STATE.labels(f"{role}_max_waiting").set(waiter_limits.get(role, 0))

    def close(self):
        self.pool.close()



class SharedAcquisitionBudget:
    """Bound live checkout attempts and conservatively retained timeout slots."""

    def __init__(self, maximum, limits, pools, reclaim_partial_timeouts=False):
        self.reclaim_partial_timeouts = reclaim_partial_timeouts
        self.maximum = maximum
        self.limits = limits
        self.pools = pools
        self._lock = Lock()
        self._counts = {role: 0 for role in limits}
        self._retained = {role: 0 for role in limits}
        self._rejected = {role: 0 for role in limits}

    def _refresh(self):
        # Public FIFO length bounds remaining expired positions. Never subtract
        # active checkouts: they may hold admission but not yet be queued.
        for role, pool in self.pools.items():
            if self._retained[role]:
                waiting = pool.get_stats().get("requests_waiting", 0)
                retained = min(self._retained[role], waiting) if self.reclaim_partial_timeouts else (
                    self._retained[role] if waiting else 0
                )
                self._counts[role] -= self._retained[role] - retained
                self._retained[role] = retained

    def acquire(self, role):
        with self._lock:
            self._refresh()
            if sum(self._counts.values()) >= self.maximum or self._counts[role] >= self.limits[role]:
                self._rejected[role] += 1
                reason = "global_limit" if sum(self._counts.values()) >= self.maximum else "role_limit"
                exc = TooManyRequests("API shared acquisition budget exhausted")
                exc.acquisition_failure = self._failure_snapshot_unlocked(role, reason, "guard_rejection")
                raise exc
            self._counts[role] += 1

    def _failure_snapshot_unlocked(self, role, reason, capture):
        # This runs only on failure. Do not serialize connection info or errors.
        try:
            stats = self.pools[role].get_stats()
            native = {key: stats.get(key, 0) for key in (
                "pool_size", "pool_available", "requests_waiting", "pool_max"
            )}
        except Exception:  # noqa: BLE001 - diagnostic snapshot failure must preserve the driver error.
            native = None
        return {
            "role": role,
            "reason": reason,
            "capture": capture,
            "native_pool": native,
            "guard": {
                "maximum": self.maximum,
                "used": sum(self._counts.values()),
                "acquiring": sum(self._counts.values()) - sum(self._retained.values()),
                "retained": sum(self._retained.values()),
                "counts": dict(self._counts),
                "retained_by_role": dict(self._retained),
                "limits": dict(self.limits),
                "callback_reserved": 0,
                "partial_timeout_reclaim": self.reclaim_partial_timeouts,
            },
        }


    def failure_snapshot(self, role, reason):
        with self._lock:
            return self._failure_snapshot_unlocked(role, reason, "native_failure_before_release")


    def release(self, role, timed_out=False):
        with self._lock:
            if timed_out and self.pools[role].get_stats().get("requests_waiting", 0):
                self._retained[role] += 1
            else:
                self._counts[role] -= 1
            self._refresh()

    def snapshot(self):
        with self._lock:
            self._refresh()
            return {
                "used": sum(self._counts.values()),
                "retained": sum(self._retained.values()),
                "acquiring": sum(self._counts.values()) - sum(self._retained.values()),
                "counts": dict(self._counts),
                "rejected": dict(self._rejected),
            }


class AcquisitionLimitedPool:
    """Apply one shared guard to the public checkout surface of a native pool."""

    def __init__(self, pool, budget, role):
        self._native = pool
        self._budget = budget
        self._role = role

    def __getattr__(self, name):
        return getattr(self._native, name)

    def getconn(self, timeout=None):
        started = monotonic()
        timeout = self._native.timeout if timeout is None else timeout
        try:
            self._budget.acquire(self._role)
        except TooManyRequests as exc:
            self._record_failure(exc, started)
            raise
        timed_out = False
        try:
            return self._native.getconn(timeout=timeout - (monotonic() - started))
        except (PoolTimeout, TooManyRequests) as exc:
            timed_out = isinstance(exc, PoolTimeout)
            self._record_failure(exc, started)
            raise
        finally:
            self._budget.release(self._role, timed_out)

    def _record_failure(self, exc, started):
        try:
            evidence = getattr(exc, "acquisition_failure", None)
            if evidence is None:
                reason = "native_timeout" if isinstance(exc, PoolTimeout) else "native_limit"
                evidence = self._budget.failure_snapshot(self._role, reason)
            evidence = {**evidence, "elapsed_ms": round((monotonic() - started) * 1000, 3)}
            exc.acquisition_failure = evidence
            DB_ACQUISITION_FAILURES.labels(self._role, evidence["reason"]).inc()
        except Exception:  # noqa: BLE001 - diagnostic failure cannot prevent finally-release.
            return


    def putconn(self, conn):
        try:
            return self._native.putconn(conn)
        finally:
            self._budget.snapshot()

    @contextmanager
    def connection(self, timeout=None):
        conn = self.getconn(timeout)
        try:
            with conn:
                yield conn
        finally:
            self.putconn(conn)

    def get_stats(self):
        stats = self._native.get_stats()
        rejected = self._budget.snapshot()["rejected"][self._role]
        if rejected:
            stats["requests_num"] = stats.get("requests_num", 0) + rejected
            stats["requests_errors"] = stats.get("requests_errors", 0) + rejected
        return stats

    def close(self, *args, **kwargs):
        try:
            return self._native.close(*args, **kwargs)
        finally:
            self._budget.snapshot()


def api_pool_budgets(maximum: int, maximum_waiting: int | None, payment_maximum: int = 0, shared_waiting: bool = False):
    """Partition existing API ceilings; no zero (unlimited) enabled queue."""
    waiting = maximum if maximum_waiting is None else maximum_waiting
    if maximum < 1 or waiting < 1 or not 0 <= payment_maximum < maximum:
        raise ValueError("Invalid API pool budget")
    if shared_waiting and (not payment_maximum or waiting < maximum):
        raise ValueError("Shared acquisition budget requires a partition and waiter budget at least total connections")
    if not payment_maximum:
        return {"general": {"maximum": maximum, "maximum_waiting": waiting}, "payment": None}
    if waiting < 2:
        raise ValueError("Payment partition requires at least two total waiter slots")
    if shared_waiting:
        return {
            "general": {"maximum": maximum - payment_maximum, "maximum_waiting": waiting - payment_maximum},
            "payment": {"maximum": payment_maximum, "maximum_waiting": waiting - (maximum - payment_maximum)},
        }
    payment_waiting = max(1, waiting * payment_maximum // maximum)
    return {
        "general": {"maximum": maximum - payment_maximum, "maximum_waiting": waiting - payment_waiting},
        "payment": {"maximum": payment_maximum, "maximum_waiting": payment_waiting},
    }


def create_api_databases(url, maximum, wait_ms, maximum_waiting, payment_maximum=0, shared_waiting=False, reclaim_partial_timeouts=False, failure_diagnostics=False, explicit_start=False):
    """Return general/payment adapters, sharing one pool when disabled."""
    if reclaim_partial_timeouts and not shared_waiting:
        raise ValueError("Partial timeout reclamation requires shared acquisition admission")
    if type(explicit_start) is not bool:
        raise TypeError("Explicit transaction start selection must be boolean")
    start_options = {"explicit_start": True} if explicit_start else {}
    budgets = api_pool_budgets(maximum, maximum_waiting, payment_maximum, shared_waiting)
    with ExitStack() as resources:
        general = Postgres(url, wait_ms=wait_ms, **start_options, **budgets["general"])
        resources.callback(general.close)
        payment = general
        if budgets["payment"] is not None:
            payment = Postgres(url, wait_ms=wait_ms, **start_options, **budgets["payment"])
            resources.callback(payment.close)
        shared = None
        if shared_waiting:
            native = {"general": general.pool, "payment": payment.pool}
            shared = SharedAcquisitionBudget(
                maximum if maximum_waiting is None else maximum_waiting,
                {role: budget["maximum_waiting"] for role, budget in budgets.items()}, native, reclaim_partial_timeouts,
            )
            general.pool = AcquisitionLimitedPool(general.pool, shared, "general")
            payment.pool = AcquisitionLimitedPool(payment.pool, shared, "payment")
        general._shared_acquisition_budget = payment._shared_acquisition_budget = shared
        roles = {"general": general.pool}
        if payment is not general:
            roles["payment"] = payment.pool
        general._metric_pool_roles = payment._metric_pool_roles = roles
        limits = {role: budget["maximum_waiting"] for role, budget in budgets.items() if budget is not None}
        general._metric_waiter_limits = payment._metric_waiter_limits = limits
        if failure_diagnostics:
            diagnostics = SlotDiagnostics(maximum)
            general._slot_diagnostics = payment._slot_diagnostics = diagnostics
            general._diagnostic_role = "general"
            if payment is not general:
                payment._diagnostic_role = "payment"
        resources.pop_all()
        return general, payment
