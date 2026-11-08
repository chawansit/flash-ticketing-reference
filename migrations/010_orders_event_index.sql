-- ADR0224: event-scoped order lookup; keep mutable status out of the index.
CREATE INDEX IF NOT EXISTS orders_event_id ON orders(event_id);
