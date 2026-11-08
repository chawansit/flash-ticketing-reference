CREATE TABLE IF NOT EXISTS seat_refresh_requests (
 event_id uuid PRIMARY KEY REFERENCES events(id),
 generation bigint NOT NULL DEFAULT 1 CHECK(generation > 0),
 completed_generation bigint NOT NULL DEFAULT 0 CHECK(completed_generation >= 0),
 requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_until timestamptz,
 lease_token uuid,
 CHECK(completed_generation <= generation)
);
CREATE INDEX IF NOT EXISTS seat_refresh_pending
 ON seat_refresh_requests(next_attempt_at, requested_at)
 WHERE generation > completed_generation;

