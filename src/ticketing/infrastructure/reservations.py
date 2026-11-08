import hashlib
import json
from contextlib import nullcontext
from datetime import datetime, timedelta
from ticketing.domain import persistent_uuid as uuid4
from psycopg.pq import PipelineStatus

from psycopg.types.json import Jsonb

from ticketing.application.ports import Database, SeatCache
from ticketing.domain import Failure, payment_decision
from ticketing.observability import OUTCOMES, REQUEST_ID, TimedHoldResource, hold_phase


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def event(conn, aggregate, kind, payload):
    conn.execute(
        "INSERT INTO outbox_events(id,aggregate_id,event_type,payload) VALUES (%s,%s,%s,%s)",
        (uuid4(), aggregate, kind, Jsonb({**payload, "correlation_id": REQUEST_ID.get()})),
    )


def idem(conn, actor, operation, key, request):
    hashed = digest(request)
    row = conn.execute(
        """INSERT INTO idempotency_records(actor,operation,key,request_hash)
        VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING key""",
        (actor, operation, key, hashed),
    ).fetchone()
    if row:
        return None
    previous = conn.execute(
        """SELECT * FROM idempotency_records
        WHERE actor=%s AND operation=%s AND key=%s""",
        (actor, operation, key),
    ).fetchone()
    if previous["request_hash"] != hashed:
        OUTCOMES.labels("idempotency", "mismatch").inc()
        raise Failure("IDEMPOTENCY_MISMATCH")
    OUTCOMES.labels("idempotency", "replay").inc()
    return previous["response"]


def remember(conn, actor, operation, key, response):
    conn.execute(
        """UPDATE idempotency_records SET response=%s
        WHERE actor=%s AND operation=%s AND key=%s""",
        (Jsonb(response), actor, operation, key),
    )
    return response


class PostgresReservations:
    """Sole seat-write owner. Lock order: order -> payment -> hold -> sorted seats.

    New reservations lock only seats; they never lock previous owners' orders/holds.
    No gateway, Redis, or Kafka calls occur while database locks are held.
    """

    def __init__(self, db: Database, cache: SeatCache, hold_seconds=120, *, persist_write_pipeline=False, persist_fast_batch_pipeline=False):
        self.db, self.cache, self.hold_seconds = db, cache, hold_seconds
        self.persist_write_pipeline = persist_write_pipeline
        if type(persist_fast_batch_pipeline) is not bool:
            raise TypeError("Fast batch pipeline selection must be boolean")
        self.persist_fast_batch_pipeline = persist_fast_batch_pipeline

    def reserve(self, actor, event_id, seat_ids, key):
        seats = sorted(set(seat_ids))
        if len(seats) != len(seat_ids) or not 1 <= len(seats) <= 8:
            raise Failure("INVALID_SEATS", 422)
        request = {"event_id": str(event_id), "seats": seats}
        # Admission precedes every DB access. A concurrent replay can return SEAT_BUSY;
        # replaying the same key after the original request completes returns its result.
        with (
            TimedHoldResource("redis", self.cache.shield(str(event_id), seats)),
            TimedHoldResource("db", self.db.transaction()) as conn,
            hold_phase("database_body"),
        ):
            replay = idem(conn, actor, "hold", key, request)
            if replay is not None:
                return replay
            sale = conn.execute("SELECT * FROM events WHERE id=%s", (event_id,)).fetchone()
            if not sale:
                raise Failure("EVENT_NOT_FOUND", 404)
            rows = conn.execute(
                """SELECT * FROM event_seats WHERE event_id=%s
                    AND seat_id=ANY(%s) ORDER BY seat_id FOR UPDATE NOWAIT""",
                (event_id, seats),
            ).fetchall()
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            if not sale["sale_starts"] <= now < sale["sale_ends"]:
                raise Failure("SALE_CLOSED")
            if len(rows) != len(seats):
                raise Failure("SEAT_NOT_FOUND", 404)
            if any(r["booked_order_id"] or (r["reserved_until"] and r["reserved_until"] > now) for r in rows):
                raise Failure("SEAT_UNAVAILABLE")
            hold, order, expires = uuid4(), uuid4(), now + timedelta(seconds=self.hold_seconds)
            conn.execute("INSERT INTO holds VALUES (%s,%s,%s,%s,'ACTIVE')", (hold, actor, event_id, expires))
            total = sum(r["price"] for r in rows)
            conn.execute(
                """INSERT INTO orders(id,actor,hold_id,event_id,total,currency,status)
                    VALUES (%s,%s,%s,%s,%s,%s,'PENDING')""",
                (order, actor, hold, event_id, total, sale["currency"]),
            )
            conn.execute(
                """UPDATE event_seats SET hold_id=%s,reserved_until=%s,version=version+1
                    WHERE event_id=%s AND seat_id=ANY(%s)""",
                (hold, expires, event_id, seats),
            )
            for row in rows:
                conn.execute(
                    "INSERT INTO order_items VALUES (%s,%s,%s,%s)",
                    (order, event_id, row["seat_id"], row["price"]),
                )
            event(conn, order, "SeatsChanged", {"event_id": str(event_id), "seats": seats})
            return remember(
                conn,
                actor,
                "hold",
                key,
                {
                    "hold_id": str(hold),
                    "order_id": str(order),
                    "expires_at": expires.isoformat(),
                    "total": total,
                    "currency": sale["currency"],
                    "seats": seats,
                },
            )

    def persist_reservation_command(self, payload, response):
        """Persist one Redis-first command exactly once."""
        outcome, value = self.persist_reservation_commands([(payload, response)])[0]
        if outcome == "failed":
            raise value
        return value

    def persist_reservation_commands(self, commands):
        """Persist a bounded command batch with savepoint-isolated domain failures."""
        commands = list(commands)
        if not 1 <= len(commands) <= 8:
            raise ValueError("Reservation persistence batch must contain between 1 and 8 commands")
        if len(commands) == 1:
            try:
                with self.db.transaction() as conn:
                    durable = self._persist_reservation_command(conn, *commands[0])
            except Failure as exc:
                return [("failed", exc)]
            return [("durable", durable)]

        if self.persist_write_pipeline and 2 <= len(commands) <= 4:
            try:
                transaction = (self.db.transaction(pipeline=True)
                               if self.persist_fast_batch_pipeline else self.db.transaction())
                with transaction as conn:
                    outcomes = [("durable", self._persist_reservation_command(conn, payload, response))
                                for payload, response in commands]
                return outcomes
            except Failure:
                # The transaction context must confirm rollback before isolation.
                # SQL, commit and rollback failures propagate without fallback.
                pass

        outcomes = []
        with self.db.transaction() as conn:
            for payload, response in commands:
                try:
                    with conn.transaction():
                        durable = self._persist_reservation_command(conn, payload, response)
                except Failure as exc:
                    outcomes.append(("failed", exc))
                else:
                    outcomes.append(("durable", durable))
        return outcomes

    def _persist_reservation_command(self, conn, payload, response):
        """Validate and write one command inside its caller-owned transaction."""
        request = {"event_id": payload["event_id"], "seats": sorted(payload["seat_ids"])}
        if digest(request) != payload["request_hash"]:
            raise Failure("RESERVATION_COMMAND_MISMATCH", 422)
        previous_command = conn.execute(
            "SELECT request_hash,response FROM reservation_commands WHERE command_id=%s",
            (payload["command_id"],),
        ).fetchone()
        if previous_command:
            if previous_command["request_hash"] != payload["request_hash"]:
                raise Failure("RESERVATION_COMMAND_MISMATCH", 409)
            return previous_command["response"]

        replay = idem(
            conn,
            payload["actor"],
            "hold",
            payload["idempotency_key"],
            request,
        )
        if replay is not None:
            if (
                replay["hold_id"] != payload["hold_id"]
                or replay["order_id"] != payload["order_id"]
            ):
                raise Failure("RESERVATION_COMMAND_MISMATCH", 409)
            return replay

        rows = conn.execute(
            """SELECT * FROM event_seats WHERE event_id=%s
            AND seat_id=ANY(%s) ORDER BY seat_id FOR UPDATE NOWAIT""",
            (payload["event_id"], sorted(payload["seat_ids"])),
        ).fetchall()
        sale = conn.execute(
            "SELECT *, clock_timestamp() AS now FROM events WHERE id=%s", (payload["event_id"],)
        ).fetchone()
        if not sale:
            raise Failure("EVENT_NOT_FOUND", 404)
        now = sale["now"]
        if not sale["sale_starts"] <= now < sale["sale_ends"]:
            raise Failure("SALE_CLOSED")
        if len(rows) != len(payload["seat_ids"]):
            raise Failure("SEAT_NOT_FOUND", 404)
        expires_at = datetime.fromisoformat(payload["expires_at"])
        if expires_at <= now:
            raise Failure("HOLD_EXPIRED")
        if any(
            row["booked_order_id"]
            or (
                row["reserved_until"]
                and row["reserved_until"] > now
                and str(row["hold_id"]) != payload["hold_id"]
            )
            for row in rows
        ):
            raise Failure("SEAT_UNAVAILABLE")

        total = sum(row["price"] for row in rows)
        durable = {
            **response,
            "total": total,
            "currency": sale["currency"],
            "expires_at": expires_at.isoformat(),
            "persistence_status": "DURABLE",
        }
        outer_pipeline = (self.persist_fast_batch_pipeline
                          and conn.info.pipeline_status != PipelineStatus.OFF)
        with conn.pipeline() if self.persist_write_pipeline and not outer_pipeline else nullcontext():
            conn.execute(
                "INSERT INTO holds VALUES (%s,%s,%s,%s,'ACTIVE')",
                (
                    payload["hold_id"],
                    payload["actor"],
                    payload["event_id"],
                    payload["expires_at"],
                ),
            )
            conn.execute(
                """INSERT INTO orders(id,actor,hold_id,event_id,total,currency,status)
            VALUES (%s,%s,%s,%s,%s,%s,'PENDING')""",
                (
                    payload["order_id"],
                    payload["actor"],
                    payload["hold_id"],
                    payload["event_id"],
                    total,
                    sale["currency"],
                ),
            )
            conn.execute(
                """UPDATE event_seats SET hold_id=%s,reserved_until=%s,version=version+1
            WHERE event_id=%s AND seat_id=ANY(%s)""",
                (
                    payload["hold_id"],
                    payload["expires_at"],
                    payload["event_id"],
                    sorted(payload["seat_ids"]),
                ),
            )
            for row in rows:
                conn.execute(
                    "INSERT INTO order_items VALUES (%s,%s,%s,%s)",
                    (
                        payload["order_id"],
                        payload["event_id"],
                        row["seat_id"],
                        row["price"],
                    ),
                )
            event(
                conn,
                payload["order_id"],
                "SeatsChanged",
                {"event_id": payload["event_id"], "seats": sorted(payload["seat_ids"])},
            )
            remember(
                conn,
                payload["actor"],
                "hold",
                payload["idempotency_key"],
                durable,
            )
            conn.execute(
                """INSERT INTO reservation_commands
            (command_id,actor,idempotency_key,request_hash,event_id,hold_id,order_id,status,response)
            VALUES (%s,%s,%s,%s,%s,%s,%s,'DURABLE',%s)""",
                (
                    payload["command_id"],
                    payload["actor"],
                    payload["idempotency_key"],
                    payload["request_hash"],
                    payload["event_id"],
                    payload["hold_id"],
                    payload["order_id"],
                    Jsonb(durable),
                ),
            )
        return durable

    def checkout(self, actor, hold_id, key):
        with self.db.transaction() as conn:
            replay = idem(conn, actor, "order", key, {"hold_id": str(hold_id)})
            if replay is not None:
                return replay
            row = conn.execute(
                "SELECT * FROM orders WHERE hold_id=%s AND actor=%s FOR UPDATE NOWAIT", (hold_id, actor)
            ).fetchone()
            if not row:
                raise Failure("HOLD_NOT_FOUND", 404)
            hold = conn.execute("SELECT * FROM holds WHERE id=%s", (hold_id,)).fetchone()
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            if row["status"] == "PENDING" and hold["expires_at"] <= now:
                raise Failure("HOLD_EXPIRED")
            return remember(
                conn,
                actor,
                "order",
                key,
                {
                    "order_id": str(row["id"]),
                    "status": row["status"],
                    "total": row["total"],
                    "currency": row["currency"],
                },
            )

    def get_order(self, actor, order_id):
        query = """SELECT o.*, t.id AS _ticket_id, b.seat_id AS _ticket_seat_id
                FROM orders o
                LEFT JOIN (bookings b JOIN tickets t ON t.booking_id=b.id) ON b.order_id=o.id
                WHERE o.id=%s AND o.actor=%s ORDER BY b.seat_id"""
        if getattr(self.db, "order_status_read_queued_commit", False):
            return self._order_result(self.db.readonly_rows(query, (order_id, actor)))
        transaction = (self.db.transaction(pipeline=True)
                       if getattr(self.db, "order_status_read_pipeline", False) else self.db.transaction())
        with transaction as conn:
            return self._order_result(conn.execute(query, (order_id, actor)).fetchall())

    @staticmethod
    def _order_result(rows):
        if not rows:
            raise Failure("ORDER_NOT_FOUND", 404)
        tickets = [
            {"id": row["_ticket_id"], "seat_id": row["_ticket_seat_id"]}
            for row in rows
            if row["_ticket_id"] is not None
        ]
        row = rows[0]
        row.pop("_ticket_id")
        row.pop("_ticket_seat_id")
        row["tickets"] = tickets
        return row

    def get_payment_operation(self, actor, order_id, key):
        """Authoritative observation only; NOT_STARTED never authorizes a new identity."""
        with self.db.transaction() as conn:
            row = conn.execute(
                """SELECT o.id AS order_id, o.status AS order_status, h.expires_at,
                clock_timestamp() AS observed_at, p.id AS payment_id, p.status AS payment_status
                FROM orders o JOIN holds h ON h.id=o.hold_id
                LEFT JOIN idempotency_records i ON i.actor=o.actor AND i.operation='payment'
                    AND i.key=%s AND i.response->>'order_id'=o.id::text
                LEFT JOIN payment_attempts p ON p.id=(i.response->>'payment_id')::uuid AND p.order_id=o.id
                WHERE o.id=%s AND o.actor=%s""", (key, order_id, actor),
            ).fetchone()
            if row is None:
                raise Failure("ORDER_NOT_FOUND", 404)
            return {
                "order_id": str(row["order_id"]),
                "payment_id": str(row["payment_id"]) if row["payment_id"] is not None else None,
                "state": row["payment_status"] if row["payment_id"] is not None else "NOT_STARTED",
                "order_status": row["order_status"],
                "expires_at": row["expires_at"], "observed_at": row["observed_at"],
                "can_initiate": row["order_status"] == "PENDING" and row["expires_at"] > row["observed_at"],
            }

    def get_hold(self, actor, hold_id):
        with self.db.transaction() as conn:
            row = conn.execute("SELECT * FROM holds WHERE id=%s AND actor=%s", (hold_id, actor)).fetchone()
            if not row:
                raise Failure("HOLD_NOT_FOUND", 404)
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            if row["status"] == "ACTIVE" and row["expires_at"] <= now:
                row["status"] = "EXPIRED"
            return row

    def release(self, actor, hold_id):
        with self.db.transaction() as conn:
            order = conn.execute(
                "SELECT * FROM orders WHERE hold_id=%s AND actor=%s FOR UPDATE NOWAIT", (hold_id, actor)
            ).fetchone()
            if not order:
                raise Failure("HOLD_NOT_FOUND", 404)
            if order["status"] in {"PAID", "FULFILLED"}:
                raise Failure("ALREADY_BOOKED")
            self._release(conn, order, "RELEASED")
        return {"status": "RELEASED"}

    def _release(self, conn, order, state):
        conn.execute("SELECT id FROM holds WHERE id=%s FOR UPDATE NOWAIT", (order["hold_id"],))
        conn.execute(
            """SELECT seat_id FROM event_seats WHERE hold_id=%s
            ORDER BY seat_id FOR UPDATE NOWAIT""",
            (order["hold_id"],),
        ).fetchall()
        released = conn.execute(
            """UPDATE event_seats SET hold_id=NULL,reserved_until=NULL,version=version+1
            WHERE hold_id=%s AND booked_order_id IS NULL RETURNING seat_id""",
            (order["hold_id"],),
        ).fetchall()
        conn.execute("UPDATE holds SET status=%s WHERE id=%s AND status='ACTIVE'", (state, order["hold_id"]))
        conn.execute("UPDATE orders SET status='EXPIRED' WHERE id=%s AND status='PENDING'", (order["id"],))
        event(
            conn,
            order["id"],
            "SeatsChanged",
            {"event_id": str(order["event_id"]), "seats": [r["seat_id"] for r in released]},
        )

    def expire_batch(self, limit=8):
        if not 1 <= limit <= 100:
            raise ValueError("Expiry batch limit must be between 1 and 100")
        with self.db.transaction() as conn:
            orders = conn.execute(
                """SELECT o.* FROM orders o JOIN holds h ON h.id=o.hold_id
                WHERE o.status='PENDING' AND h.status='ACTIVE' AND h.expires_at <= clock_timestamp()
                ORDER BY h.expires_at LIMIT %s FOR UPDATE OF o SKIP LOCKED""",
                (limit,),
            ).fetchall()
            for order in orders:
                self._release(conn, order, "EXPIRED")
            return len(orders)

    def expire_one(self):
        return bool(self.expire_batch(1))

    def initiate_payment(self, actor, order_id, key, outcome, delay_seconds, duplicates):
        request = {
            "order_id": str(order_id),
            "outcome": outcome,
            "delay_seconds": delay_seconds,
            "duplicates": duplicates,
        }
        transaction = (self.db.transaction(pipeline=True)
                       if getattr(self.db, "payment_write_pipeline", False) else self.db.transaction())
        with transaction as conn:
            replay = idem(conn, actor, "payment", key, request)
            if replay is not None:
                return replay
            order = conn.execute(
                "SELECT * FROM orders WHERE id=%s AND actor=%s FOR UPDATE NOWAIT", (order_id, actor)
            ).fetchone()
            if not order:
                raise Failure("ORDER_NOT_FOUND", 404)
            # Use a fresh snapshot after the order lock; an initial lock/join can
            # miss a payment committed while that statement acquired its order.
            context = conn.execute(
                """SELECT p.id AS payment_id, h.expires_at, clock_timestamp() AS now
                FROM holds h LEFT JOIN payment_attempts p ON p.order_id=%s
                WHERE h.id=%s""",
                (order_id, order["hold_id"]),
            ).fetchone()
            if context["payment_id"] is not None:
                result = {"payment_id": str(context["payment_id"]), "order_id": str(order_id)}
            else:
                now = context["now"]
                if order["status"] != "PENDING" or context["expires_at"] <= now:
                    raise Failure("ORDER_NOT_PAYABLE")
                payment_id = uuid4()
                conn.execute(
                    """INSERT INTO payment_attempts
                    (id,order_id,status,outcome,due_at,target_deliveries)
                    VALUES (%s,%s,'PENDING',%s,%s,%s)""",
                    (payment_id, order_id, outcome, now + timedelta(seconds=delay_seconds), duplicates),
                )
                result = {"payment_id": str(payment_id), "order_id": str(order_id)}
            return remember(conn, actor, "payment", key, result)

    def callback(self, payload):
        with self.db.transaction() as conn:
            return self.apply_callback(conn, payload)

    def apply_callback(self, conn, payload):
        """Apply financial effects in the caller-owned transaction; never commit here."""
        order = conn.execute(
            """SELECT o.* FROM orders o
                JOIN payment_attempts p ON p.order_id=o.id
                WHERE p.id=%s FOR UPDATE OF o NOWAIT""",
            (payload["payment_id"],),
        ).fetchone()
        if not order:
            raise Failure("PAYMENT_NOT_FOUND", 404)
        payment = conn.execute(
            "SELECT * FROM payment_attempts WHERE id=%s FOR UPDATE NOWAIT", (payload["payment_id"],)
        ).fetchone()
        if (
            str(order["id"]) != payload["order_id"]
            or order["total"] != payload["amount"]
            or order["currency"] != payload["currency"]
        ):
            raise Failure("PAYMENT_MISMATCH", 422)
        previous = conn.execute(
            "SELECT * FROM payment_callbacks WHERE id=%s", (payload["callback_id"],)
        ).fetchone()
        if previous:
            if previous["payload_hash"] != digest(payload):
                raise Failure("CALLBACK_MISMATCH")
            return {"status": "duplicate"}
        conn.execute(
            "INSERT INTO payment_callbacks(id,payment_id,payload_hash) VALUES (%s,%s,%s)",
            (payload["callback_id"], payload["payment_id"], digest(payload)),
        )
        if payment["status"] == "SUCCEEDED":
            return {"status": "duplicate"}
        hold = conn.execute(
            "SELECT * FROM holds WHERE id=%s FOR UPDATE NOWAIT", (order["hold_id"],)
        ).fetchone()
        seats = conn.execute(
            """SELECT s.* FROM event_seats s JOIN order_items i
                ON i.event_id=s.event_id AND i.seat_id=s.seat_id
                WHERE i.order_id=%s ORDER BY s.seat_id FOR UPDATE OF s NOWAIT""",
            (order["id"],),
        ).fetchall()
        now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
        valid = (
            bool(seats)
            and hold["status"] == "ACTIVE"
            and hold["expires_at"] > now
            and all(
                r["hold_id"] == hold["id"] and r["booked_order_id"] is None and r["reserved_until"] > now
                for r in seats
            )
        )
        decision = payment_decision(order["status"], valid, payload["outcome"])
        pipeline = decision == "BOOK" and getattr(self.db, "payment_callback_write_pipeline", False)
        with conn.pipeline() if pipeline else nullcontext():
            conn.execute(
                "UPDATE payment_attempts SET status=%s WHERE id=%s", (payload["outcome"], payment["id"])
            )
            if decision == "BOOK":
                for seat in seats:
                    conn.execute(
                        "INSERT INTO bookings VALUES (%s,%s,%s,%s)",
                        (uuid4(), order["event_id"], seat["seat_id"], order["id"]),
                    )
                conn.execute(
                    """UPDATE event_seats SET booked_order_id=%s,hold_id=NULL,
                        reserved_until=NULL,version=version+1 WHERE hold_id=%s""",
                    (order["id"], hold["id"]),
                )
                conn.execute("UPDATE holds SET status='CONSUMED' WHERE id=%s", (hold["id"],))
                conn.execute("UPDATE orders SET status='PAID' WHERE id=%s", (order["id"],))
                event(conn, order["id"], "OrderPaid", {"order_id": str(order["id"])})
            elif decision in {"REFUND", "FAILED"}:
                self._release(conn, order, "EXPIRED" if decision == "REFUND" else "RELEASED")
                conn.execute(
                    "UPDATE orders SET status=%s WHERE id=%s",
                    ("REFUND_PENDING" if decision == "REFUND" else "FAILED", order["id"]),
                )
                if decision == "REFUND":
                    conn.execute(
                        "INSERT INTO refund_requests VALUES (%s,%s,'PENDING') ON CONFLICT DO NOTHING",
                        (payment["id"], order["id"]),
                    )
                    event(
                        conn,
                        order["id"],
                        "RefundRequested",
                        {"payment_id": str(payment["id"]), "order_id": str(order["id"])},
                    )
            event(
                conn,
                order["id"],
                "SeatsChanged",
                {"event_id": str(order["event_id"]), "seats": [r["seat_id"] for r in seats]},
            )
        return {"status": decision.lower()}





class RedisFirstReservations:
    """Route only hold creation through Redis; durable commerce remains PostgreSQL-backed."""

    def __init__(self, durable, intake):
        self.durable = durable
        self.intake = intake

    def reserve(self, actor, event_id, seat_ids, key):
        return self.intake.enqueue(actor, event_id, seat_ids, key)

    def command_status(self, event_id, command_id):
        return self.intake.status(event_id, command_id)

    def __getattr__(self, name):
        return getattr(self.durable, name)
