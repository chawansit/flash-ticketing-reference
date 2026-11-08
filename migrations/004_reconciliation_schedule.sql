-- Additive. Proactive reconciliation schedule for the maintenance worker.
-- Advisory scheduling state only: never an authority for seats, holds or orders.
-- Rows are derived from events and may be deleted or truncated without data loss.
CREATE TABLE IF NOT EXISTS event_reconciliation (
 event_id uuid PRIMARY KEY REFERENCES events(id),
 next_due_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 last_reconciled_at timestamptz,
 consecutive_failures integer NOT NULL DEFAULT 0 CHECK(consecutive_failures >= 0),
 lease_until timestamptz,
 lease_token uuid,
 claimed_at timestamptz,
 CHECK((lease_until IS NULL) = (lease_token IS NULL))
);
-- Oldest-due ordering and the cheap min(next_due_at) overdue probe.
CREATE INDEX IF NOT EXISTS event_reconciliation_due ON event_reconciliation(next_due_at, event_id);
-- Active-window selection for seeding, claiming and pruning.
CREATE INDEX IF NOT EXISTS events_sale_window ON events(sale_ends, sale_starts);
