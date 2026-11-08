CREATE TABLE IF NOT EXISTS reservation_commands (
 command_id uuid PRIMARY KEY,
 actor text NOT NULL,
 idempotency_key text NOT NULL,
 request_hash text NOT NULL,
 event_id uuid NOT NULL REFERENCES events,
 hold_id uuid NOT NULL UNIQUE,
 order_id uuid NOT NULL UNIQUE,
 status text NOT NULL CHECK(status IN ('DURABLE')),
 response jsonb NOT NULL,
 persisted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS reservation_commands_event
 ON reservation_commands(event_id, persisted_at);
