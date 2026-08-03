ALTER TABLE delivery_manifests ADD COLUMN IF NOT EXISTS logical_batch_id TEXT;
UPDATE delivery_manifests
SET logical_batch_id='legacy:' || position_id
WHERE logical_batch_id IS NULL;
ALTER TABLE delivery_manifests ALTER COLUMN logical_batch_id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS delivery_manifests_logical_batch_idx
  ON delivery_manifests (flow_incarnation_id,destination_revision_id,logical_batch_id);

ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS logical_batch_id TEXT;
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS attempt_number INTEGER NOT NULL DEFAULT 1;
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS attempt_state TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS terminal_at TIMESTAMPTZ;
ALTER TABLE delivery_attempts ADD COLUMN IF NOT EXISTS last_error TEXT;
UPDATE delivery_attempts
SET logical_batch_id='legacy:' || position_id
WHERE logical_batch_id IS NULL;
ALTER TABLE delivery_attempts ALTER COLUMN logical_batch_id SET NOT NULL;
ALTER TABLE delivery_attempts DROP CONSTRAINT IF EXISTS delivery_attempts_state_valid;
ALTER TABLE delivery_attempts ADD CONSTRAINT delivery_attempts_state_valid
  CHECK (attempt_state IN ('pending','applied','not_applied','failed'));
ALTER TABLE delivery_attempts DROP CONSTRAINT IF EXISTS delivery_attempts_number_positive;
ALTER TABLE delivery_attempts ADD CONSTRAINT delivery_attempts_number_positive CHECK (attempt_number > 0);
CREATE INDEX IF NOT EXISTS delivery_attempts_retry_idx
  ON delivery_attempts (flow_incarnation_id,destination_revision_id,next_attempt_at)
  WHERE attempt_state='pending';

ALTER TABLE delivery_receipts ADD COLUMN IF NOT EXISTS logical_batch_id TEXT;
UPDATE delivery_receipts
SET logical_batch_id='legacy:' || position_id
WHERE logical_batch_id IS NULL;
ALTER TABLE delivery_receipts ALTER COLUMN logical_batch_id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS delivery_receipts_logical_batch_idx
  ON delivery_receipts (flow_incarnation_id,destination_revision_id,logical_batch_id);

CREATE TABLE IF NOT EXISTS delivery_retention_roots (
  flow_incarnation_id UUID PRIMARY KEY,
  minimum_position_id TEXT NOT NULL,
  retained_after TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

DROP TRIGGER IF EXISTS delivery_retention_roots_require_authority_v2 ON delivery_retention_roots;
CREATE TRIGGER delivery_retention_roots_require_authority_v2
BEFORE INSERT OR UPDATE OR DELETE ON delivery_retention_roots
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2();
