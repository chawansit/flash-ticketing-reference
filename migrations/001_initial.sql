CREATE TABLE IF NOT EXISTS events (
 id uuid PRIMARY KEY, title text NOT NULL, currency text NOT NULL CHECK(length(currency)=3),
 sale_starts timestamptz NOT NULL, sale_ends timestamptz NOT NULL,
 CHECK(sale_ends > sale_starts)
);
CREATE TABLE IF NOT EXISTS holds (
 id uuid PRIMARY KEY, actor text NOT NULL, event_id uuid NOT NULL REFERENCES events,
 expires_at timestamptz NOT NULL, status text NOT NULL CHECK(status IN ('ACTIVE','RELEASED','EXPIRED','CONSUMED'))
);
CREATE TABLE IF NOT EXISTS orders (
 id uuid PRIMARY KEY, actor text NOT NULL, hold_id uuid NOT NULL UNIQUE REFERENCES holds,
 event_id uuid NOT NULL REFERENCES events, total bigint NOT NULL CHECK(total>=0), currency text NOT NULL,
 status text NOT NULL CHECK(status IN ('PENDING','FAILED','EXPIRED','REFUND_PENDING','REFUNDED','PAID','FULFILLED')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS event_seats (
 event_id uuid NOT NULL REFERENCES events, seat_id text NOT NULL,
 price bigint NOT NULL CHECK(price>=0), hold_id uuid REFERENCES holds,
 reserved_until timestamptz, booked_order_id uuid REFERENCES orders,
 version bigint NOT NULL DEFAULT 0,
 PRIMARY KEY(event_id,seat_id),
 CHECK((hold_id IS NULL) = (reserved_until IS NULL)),
 CHECK(booked_order_id IS NULL OR hold_id IS NULL)
);
CREATE TABLE IF NOT EXISTS order_items (
 order_id uuid NOT NULL REFERENCES orders, event_id uuid NOT NULL, seat_id text NOT NULL,
 price bigint NOT NULL, PRIMARY KEY(order_id,seat_id),
 FOREIGN KEY(event_id,seat_id) REFERENCES event_seats
);
CREATE TABLE IF NOT EXISTS bookings (
 id uuid PRIMARY KEY, event_id uuid NOT NULL, seat_id text NOT NULL,
 order_id uuid NOT NULL REFERENCES orders, UNIQUE(event_id,seat_id),
 FOREIGN KEY(event_id,seat_id) REFERENCES event_seats
);
CREATE TABLE IF NOT EXISTS idempotency_records (
 actor text NOT NULL, operation text NOT NULL, key text NOT NULL, request_hash text NOT NULL,
 response jsonb, PRIMARY KEY(actor,operation,key)
);
CREATE TABLE IF NOT EXISTS payment_attempts (
 id uuid PRIMARY KEY, order_id uuid NOT NULL UNIQUE REFERENCES orders,
 status text NOT NULL CHECK(status IN ('PENDING','FAILED','SUCCEEDED')),
 outcome text NOT NULL CHECK(outcome IN ('SUCCEEDED','FAILED')),
 due_at timestamptz NOT NULL, deliveries integer NOT NULL DEFAULT 0,
 target_deliveries integer NOT NULL CHECK(target_deliveries BETWEEN 1 AND 10),
 lease_until timestamptz, lease_token uuid
);
CREATE TABLE IF NOT EXISTS payment_callbacks (
 id uuid PRIMARY KEY, payment_id uuid NOT NULL REFERENCES payment_attempts,
 payload_hash text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS refund_requests (
 payment_id uuid PRIMARY KEY REFERENCES payment_attempts, order_id uuid NOT NULL REFERENCES orders,
 status text NOT NULL CHECK(status IN ('PENDING','REFUNDED'))
);
CREATE TABLE IF NOT EXISTS outbox_events (
 id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL,
 schema_version integer NOT NULL DEFAULT 1, payload jsonb NOT NULL,
 occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(), published_at timestamptz,
 lease_until timestamptz, lease_token uuid
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox_events(occurred_at) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS holds_expiry ON holds(expires_at) WHERE status='ACTIVE';
CREATE TABLE IF NOT EXISTS consumer_inbox (
 consumer text NOT NULL, event_id uuid NOT NULL, PRIMARY KEY(consumer,event_id)
);
CREATE TABLE IF NOT EXISTS tickets (
 id uuid PRIMARY KEY, booking_id uuid NOT NULL UNIQUE REFERENCES bookings,
 issued_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS dead_letters (
 event_id uuid PRIMARY KEY, payload jsonb NOT NULL, error text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
