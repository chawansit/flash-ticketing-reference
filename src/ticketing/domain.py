import os
from secrets import randbits
from time import time_ns
from uuid import UUID, uuid4
from dataclasses import dataclass


@dataclass
class Failure(Exception):
    code: str
    status: int = 409


def payment_decision(order_status: str, valid_hold: bool, outcome: str) -> str:
    """A financial success is never discarded, even after a failed/expired checkout."""
    if order_status in {"PAID", "FULFILLED"}:
        return "UNCHANGED"
    if outcome == "FAILED":
        return "FAILED" if order_status == "PENDING" else "UNCHANGED"
    return "BOOK" if order_status == "PENDING" and valid_hold else "REFUND"


def persistent_uuid():
    """Opaque durable IDs; timestamps never drive authorization, sale clocks or TTL."""
    if os.getenv("PERSISTENT_UUID_V7", "0") != "1":
        return uuid4()
    timestamp = time_ns() // 1_000_000
    if not 0 <= timestamp < (1 << 48):
        raise RuntimeError("Persistent UUID timestamp outside48-bit Unix milliseconds")
    random = randbits(74)
    return UUID(int=(timestamp << 80) | (7 << 76) | ((random >> 62) << 64)
                | (2 << 62) | (random & ((1 << 62) - 1)))
