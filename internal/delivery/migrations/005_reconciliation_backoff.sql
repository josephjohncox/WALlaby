ALTER TABLE delivery_attempts
  ADD COLUMN IF NOT EXISTS reconciliation_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE delivery_attempts
  ADD COLUMN IF NOT EXISTS last_reconciled_at TIMESTAMPTZ;
ALTER TABLE delivery_attempts DROP CONSTRAINT IF EXISTS delivery_attempts_reconciliation_attempts_nonnegative;
ALTER TABLE delivery_attempts ADD CONSTRAINT delivery_attempts_reconciliation_attempts_nonnegative
  CHECK (reconciliation_attempts >= 0);

DROP INDEX IF EXISTS delivery_attempts_retry_idx;
CREATE INDEX delivery_attempts_retry_idx
  ON delivery_attempts (flow_incarnation_id,destination_revision_id,next_attempt_at)
  WHERE attempt_state IN ('pending','applied','failed','not_applied');
