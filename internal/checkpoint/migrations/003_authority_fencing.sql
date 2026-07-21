CREATE TABLE IF NOT EXISTS authoritative_checkpoints (
  flow_incarnation_id UUID PRIMARY KEY,
  flow_id TEXT NOT NULL,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  lsn TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS authoritative_checkpoints_flow_idx
  ON authoritative_checkpoints (flow_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS authoritative_checkpoint_outbox (
  flow_incarnation_id UUID NOT NULL,
  flow_id TEXT NOT NULL,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  destination_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  batch_hash TEXT NOT NULL,
  codec TEXT NOT NULL,
  batch_json BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  delivered_at TIMESTAMPTZ,
  PRIMARY KEY (flow_incarnation_id, destination_id, position_id)
);
CREATE INDEX IF NOT EXISTS authoritative_outbox_pending_idx
  ON authoritative_checkpoint_outbox (flow_incarnation_id, created_at, destination_id)
  WHERE delivered_at IS NULL;
