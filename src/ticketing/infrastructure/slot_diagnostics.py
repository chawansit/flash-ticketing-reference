"""Bounded, nonfinancial checkout evidence; no SQL or customer identifiers."""
import json
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Lock
from time import monotonic, time

OPERATION = ContextVar("db_operation_category", default="other")
ACTIVE_LEASE = ContextVar("active_db_lease", default=None)
PREFIX = b"# ticketing_db_failure_diagnostics "
MAX_PAYLOAD = 64 * 1024
OPERATIONS = {"payment_request", "payment_callback", "order_status", "order_create", "hold", "availability", "other"}
PHASES = {"connection_context", "setup", "body", "commit", "rollback", "context_exit", "pool_return"}
COMMANDS = {"SELECT", "INSERT", "UPDATE", "DELETE", "SET", "BEGIN", "COMMIT", "ROLLBACK", "OTHER"}


def operation_category(method, path):
    parts = path.strip("/").split("/")
    if method == "POST" and parts == ["v1", "webhooks", "payments"]:
        return "payment_callback"
    if parts[:2] == ["v1", "orders"]:
        if method == "POST" and len(parts) == 4 and parts[3] == "payments":
            return "payment_request"
        if method == "GET" and len(parts) == 3:
            return "order_status"
        if method == "POST" and len(parts) == 2:
            return "order_create"
    if parts[:2] == ["v1", "holds"]:
        return "hold"
    if method == "GET" and parts[:2] == ["v1", "events"]:
        return "availability"
    return "other"


class SlotDiagnostics:
    def __init__(self, maximum):
        if type(maximum) is not int or not 1 <= maximum <= 128:
            raise ValueError("Bounded diagnostic connection budget required")
        self.maximum = maximum
        self.lock = Lock()
        self.active = {}
        self.records = deque(maxlen=16)
        self.sequence = self.lease_sequence = self.overwritten = self.errors = 0

    def checkout(self, role, connection=None):
        with self.lock:
            # A pool can hand a returned connection to another thread before
            # putconn() finishes. Retire only that proven same-object handoff;
            # the older lease must never erase the new owner on late return.
            prior = next((key for key, value in self.active.items()
                          if connection is not None and value["connection"] is connection), None)
            if prior is not None:
                if self.active[prior]["phase"] != "pool_return":
                    self.errors += 1
                    return None
                self.active.pop(prior)
            if len(self.active) >= self.maximum or role not in {"general", "payment"}:
                self.errors += 1
                return None
            self.lease_sequence += 1
            lease = self.lease_sequence
            at = monotonic()
            operation = OPERATION.get()
            self.active[lease] = {"role": role, "operation": operation if operation in OPERATIONS else "other",
                                  "started": at, "phase": "connection_context", "phase_started": at, "connection": connection}
            return lease

    def phase(self, lease, phase):
        with self.lock:
            item = self.active.get(lease)
            if item is None:
                return None
            prior = item["phase"], item["phase_started"]
            if phase not in PHASES and phase not in {"query_" + c for c in COMMANDS}:
                self.errors += 1
                return prior
            item["phase"], item["phase_started"] = phase, monotonic()
            return prior

    def restore_phase(self, lease, prior):
        with self.lock:
            if lease in self.active and prior is not None:
                self.active[lease]["phase"], self.active[lease]["phase_started"] = prior

    def returned(self, lease, success):
        with self.lock:
            if success:
                self.active.pop(lease, None)
            else:
                self.errors += 1  # Preserve uncertain occupancy until process teardown.

    def failure(self, evidence):
        with self.lock:
            at = monotonic()
            holders = [{"lease": key, "role": value["role"], "operation": value["operation"],
                        "phase": value["phase"], "age_ms": round(max(0, at - value["started"]) * 1000, 3),
                        "phase_age_ms": round(max(0, at - value["phase_started"]) * 1000, 3)}
                       for key, value in sorted(self.active.items())]
            self.sequence += 1
            if len(self.records) == self.records.maxlen:
                self.overwritten += 1
            role = evidence.get("role")
            reason = evidence.get("reason")
            record = {"sequence": self.sequence, "captured_at_unix_seconds": time(),
                      "role": role if role in {"general", "payment", "callback"} else "unknown",
                      "reason": reason if reason in {"native_timeout", "native_limit", "global_limit", "role_limit"}
                      else "unknown", "holders": holders}
            self.records.append(record)
            return dict(record)

    def comment(self):
        with self.lock:
            value = {"schema_version": 1, "failure_total": self.sequence, "overwritten_total": self.overwritten,
                     "diagnostic_errors": self.errors, "complete": self.overwritten == self.errors == 0,
                     "records": list(self.records)}
            raw = json.dumps(value, separators=(",", ":")).encode()
            if len(raw) + len(PREFIX) + 1 > MAX_PAYLOAD:
                value.update(complete=False, payload_ceiling_reached=True, records=[])
                raw = json.dumps(value, separators=(",", ":")).encode()
            return PREFIX + raw + b"\n"


def safely(tracker, method, *args):
    try:
        return getattr(tracker, method)(*args)
    except Exception:  # noqa: BLE001 - Diagnostics must never replace a financial outcome.
        try:
            with tracker.lock:
                tracker.errors += 1
        except Exception:  # noqa: BLE001, S110 - Even broken diagnostics cannot mask financial errors.
            pass
        return None


def phase(value):
    active = ACTIVE_LEASE.get()
    if active is not None:
        safely(active[0], "phase", active[1], value)


@contextmanager
def query_phase(command):
    active = ACTIVE_LEASE.get()
    prior = safely(active[0], "phase", active[1], "query_" + command) if active is not None else None
    try:
        yield
    finally:
        if active is not None:
            safely(active[0], "restore_phase", active[1], prior)
