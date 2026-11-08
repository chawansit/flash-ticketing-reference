-- Keep completed history out of the claim scan. SUCCEEDED payments can still
-- have duplicate callback deliveries outstanding; do not filter by status.
CREATE INDEX IF NOT EXISTS payment_attempts_dispatch_due
ON payment_attempts(due_at) WHERE deliveries < target_deliveries;
