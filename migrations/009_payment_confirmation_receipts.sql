-- Durable verified webhook admission; no foreign keys to financially unvalidated IDs.
CREATE TABLE IF NOT EXISTS payment_receipt_capacity (
 provider text PRIMARY KEY,
 outstanding bigint NOT NULL DEFAULT 0 CHECK(outstanding>=0)
);
CREATE TABLE IF NOT EXISTS payment_webhook_receipts (
 provider text NOT NULL REFERENCES payment_receipt_capacity,
 callback_id uuid NOT NULL,
 payload_hash text NOT NULL,
 payload jsonb NOT NULL,
 received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 status text NOT NULL DEFAULT 'RECEIVED'
   CHECK(status IN ('RECEIVED','PROCESSING','RETRY','COMPLETED','REVIEW')),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_until timestamptz, lease_token uuid,
 completed_at timestamptz, result jsonb, error_code text,
 PRIMARY KEY(provider,callback_id),
 CHECK((lease_until IS NULL)=(lease_token IS NULL)),
 CHECK((status='PROCESSING')=(lease_token IS NOT NULL)),
 CHECK((status='COMPLETED')=(completed_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS payment_receipt_due
 ON payment_webhook_receipts(provider,next_attempt_at,received_at)
 WHERE status IN ('RECEIVED','RETRY','PROCESSING');
