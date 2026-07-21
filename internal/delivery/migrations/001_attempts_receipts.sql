CREATE TABLE IF NOT EXISTS destination_revisions (
  destination_revision_id TEXT PRIMARY KEY,
  destination_name TEXT NOT NULL,
  config_fingerprint TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS delivery_manifests (
  flow_incarnation_id UUID NOT NULL,
  destination_revision_id TEXT NOT NULL,
  source_lineage_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  source_transaction_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  checkpoint_lsn TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_incarnation_id,destination_revision_id,position_id)
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
  attempt_id UUID PRIMARY KEY,
  flow_incarnation_id UUID NOT NULL,
  flow_id TEXT NOT NULL,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  destination_revision_id TEXT NOT NULL,
  source_lineage_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  prepared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS delivery_attempts_manifest_idx
  ON delivery_attempts (flow_incarnation_id,destination_revision_id,position_id,prepared_at DESC);

CREATE TABLE IF NOT EXISTS delivery_attempt_evidence (
  attempt_id UUID PRIMARY KEY,
  external_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  recorded_by_acquisition_id UUID NOT NULL,
  recorded_by_lease_epoch BIGINT NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS delivery_attempt_evidence_attempt_idx
  ON delivery_attempt_evidence (attempt_id);

CREATE TABLE IF NOT EXISTS delivery_receipts (
  flow_incarnation_id UUID NOT NULL,
  destination_revision_id TEXT NOT NULL,
  source_lineage_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  attempt_id UUID NOT NULL,
  external_id TEXT NOT NULL,
  adopted_by_acquisition_id UUID NOT NULL,
  adopted_by_lease_epoch BIGINT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_incarnation_id,destination_revision_id,position_id)
);
CREATE INDEX IF NOT EXISTS delivery_receipts_attempt_idx ON delivery_receipts (attempt_id);

CREATE TABLE IF NOT EXISTS source_ack_intents (
  flow_incarnation_id UUID NOT NULL,
  position_id TEXT NOT NULL,
  checkpoint_lsn TEXT NOT NULL,
  generation BIGINT NOT NULL,
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL,
  authorized_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_incarnation_id,position_id)
);

CREATE TABLE IF NOT EXISTS source_ack_receipts (
  flow_incarnation_id UUID NOT NULL,
  position_id TEXT NOT NULL,
  checkpoint_lsn TEXT NOT NULL,
  observed_flush_lsn TEXT,
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_incarnation_id,position_id)
);
