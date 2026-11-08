from ticketing.application.ports import OrderStatusCache, ReservationStore
from ticketing.domain import Failure


class Reservations:
    """Use cases depend on an atomic persistence port, never a database driver."""

    def __init__(self, store: ReservationStore, order_cache: OrderStatusCache | None = None):
        self.store, self.order_cache = store, order_cache

    def reserve(self, actor, event_id, seat_ids, key):
        if not 1 <= len(seat_ids) <= 8 or len(set(seat_ids)) != len(seat_ids):
            raise Failure("INVALID_SEATS", 422)
        if any(not seat or len(seat) > 64 for seat in seat_ids):
            raise Failure("INVALID_SEATS", 422)
        return self.store.reserve(actor, event_id, sorted(seat_ids), key)

    def checkout(self, actor, hold_id, key):
        return self.store.checkout(actor, hold_id, key)

    def get_order(self, actor, order_id):
        if self.order_cache is None:
            return self.store.get_order(actor, order_id)
        cached, snapshot_start_ms = self.order_cache.lookup(actor, order_id)
        if cached is not None:
            return cached
        row = self.store.get_order(actor, order_id)
        if snapshot_start_ms is not None:
            self.order_cache.put(actor, order_id, row, snapshot_start_ms)
        return row

    def get_payment_operation(self, actor, order_id, key):
        return self.store.get_payment_operation(actor, order_id, key)

    def get_hold(self, actor, hold_id):
        return self.store.get_hold(actor, hold_id)

    def release(self, actor, hold_id):
        return self.store.release(actor, hold_id)

    def expire_one(self):
        return self.store.expire_one()

    def expire_batch(self, limit):
        return self.store.expire_batch(limit)

    def initiate_payment(self, actor, order_id, key, outcome, delay_seconds, duplicates):
        if (
            outcome not in {"SUCCEEDED", "FAILED"}
            or not 0 <= delay_seconds <= 600
            or not 1 <= duplicates <= 10
        ):
            raise Failure("INVALID_PAYMENT", 422)
        return self.store.initiate_payment(actor, order_id, key, outcome, delay_seconds, duplicates)

    def callback(self, payload):
        return self.store.callback(payload)
