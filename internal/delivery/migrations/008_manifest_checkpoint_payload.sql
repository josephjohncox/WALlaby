-- Persist the complete immutable source checkpoint before destination I/O so
-- commit-before-receipt recovery never depends on replay caller payload.
-- Columns remain nullable for monotonic rolling upgrades: legacy rows cannot be
-- backfilled from absent history and are handled fail-closed by the coordinator.
ALTER TABLE delivery_manifests
  ADD COLUMN IF NOT EXISTS checkpoint_metadata JSONB,
  ADD COLUMN IF NOT EXISTS checkpoint_timestamp TIMESTAMPTZ;
