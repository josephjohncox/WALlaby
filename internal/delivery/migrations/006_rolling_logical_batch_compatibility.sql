-- Keep authority-v2 checkpoint-1 workers writable during a rolling upgrade.
-- New workers always write logical_batch_id; old workers may omit it. Partial
-- uniqueness preserves the new identity without making an additive column a
-- same-protocol breaking change.
ALTER TABLE delivery_manifests ALTER COLUMN logical_batch_id DROP NOT NULL;
ALTER TABLE delivery_attempts ALTER COLUMN logical_batch_id DROP NOT NULL;
ALTER TABLE delivery_receipts ALTER COLUMN logical_batch_id DROP NOT NULL;

DROP INDEX IF EXISTS delivery_manifests_logical_batch_idx;
CREATE UNIQUE INDEX delivery_manifests_logical_batch_idx
  ON delivery_manifests (flow_incarnation_id,destination_revision_id,logical_batch_id)
  WHERE logical_batch_id IS NOT NULL;

DROP INDEX IF EXISTS delivery_receipts_logical_batch_idx;
CREATE UNIQUE INDEX delivery_receipts_logical_batch_idx
  ON delivery_receipts (flow_incarnation_id,destination_revision_id,logical_batch_id)
  WHERE logical_batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS delivery_attempts_logical_batch_idx
  ON delivery_attempts (flow_incarnation_id,destination_revision_id,logical_batch_id,attempt_number DESC)
  WHERE logical_batch_id IS NOT NULL;
