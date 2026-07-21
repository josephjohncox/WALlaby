CREATE SEQUENCE IF NOT EXISTS wallaby_bootstrap_generation_seq AS BIGINT START WITH 1;

CREATE TABLE IF NOT EXISTS source_bootstraps (
  bootstrap_id UUID PRIMARY KEY,
  flow_incarnation_id UUID NOT NULL,
  flow_id TEXT NOT NULL,
  generation BIGINT NOT NULL CHECK (generation > 0),
  bootstrap_generation BIGINT NOT NULL CHECK (bootstrap_generation > 0),
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  source_system_id TEXT NOT NULL,
  database_name TEXT NOT NULL,
  slot_name TEXT NOT NULL,
  publication_name TEXT NOT NULL,
  plugin TEXT NOT NULL,
  consistent_lsn TEXT NOT NULL,
  snapshot_name TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('exporting','snapshotting','published','streaming','abandoning','abandoned')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  abandoned_reason TEXT,
  UNIQUE (flow_incarnation_id,bootstrap_generation),
  UNIQUE (slot_name)
);

CREATE TABLE IF NOT EXISTS source_bootstrap_tasks (
  bootstrap_id UUID NOT NULL,
  relation_id OID NOT NULL,
  task_id TEXT NOT NULL,
  claim_epoch BIGINT NOT NULL DEFAULT 0,
  durable_cursor JSONB,
  receipt_hash TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending','running','complete')),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (bootstrap_id,relation_id,task_id)
);

CREATE TABLE IF NOT EXISTS snapshot_publication_receipts (
  bootstrap_id UUID PRIMARY KEY,
  content_hash TEXT NOT NULL,
  destination_revision_id TEXT NOT NULL,
  attempt_id UUID NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
