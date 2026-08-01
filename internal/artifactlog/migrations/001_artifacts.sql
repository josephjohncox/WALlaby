CREATE TABLE IF NOT EXISTS canonical_schemas (
  schema_id TEXT PRIMARY KEY,
  projection_id TEXT NOT NULL,
  schema_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS artifact_streams (
  flow_incarnation_id UUID PRIMARY KEY REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  flow_id TEXT NOT NULL,
  hard_retained_bytes BIGINT NOT NULL CHECK (hard_retained_bytes > 0),
  backlog_count_high BIGINT NOT NULL CHECK (backlog_count_high > 0),
  backlog_bytes_high BIGINT NOT NULL CHECK (backlog_bytes_high > 0),
  backlog_age_high_seconds BIGINT NOT NULL CHECK (backlog_age_high_seconds > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS artifact_objects (
  artifact_id TEXT PRIMARY KEY,
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  source_position TEXT NOT NULL,
  fragment_ordinal BIGINT NOT NULL,
  schema_id TEXT NOT NULL,
  logical_content_hash TEXT NOT NULL,
  encoded_byte_hash TEXT NOT NULL,
  encoded_length BIGINT NOT NULL CHECK (encoded_length > 0),
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  version_id TEXT,
  checksum_sha256 TEXT NOT NULL,
  encoding TEXT NOT NULL,
  encryption_mode TEXT NOT NULL DEFAULT '',
  object_lock_evidence TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL CHECK (state IN ('reserved','uploaded','verified','rooted','deleting','deleted')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (flow_incarnation_id,source_position,fragment_ordinal),
  UNIQUE (bucket,object_key),
  FOREIGN KEY (schema_id) REFERENCES canonical_schemas(schema_id) ON DELETE RESTRICT,
  CHECK (state='reserved' OR version_id IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS artifact_upload_attempts (
  attempt_id UUID PRIMARY KEY,
  artifact_id TEXT NOT NULL REFERENCES artifact_objects(artifact_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL,
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL,
  prepared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS artifact_publications (
  publication_id UUID PRIMARY KEY,
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  source_lineage_id TEXT NOT NULL,
  source_transaction_id TEXT NOT NULL,
  source_xid BIGINT NOT NULL,
  begin_lsn TEXT NOT NULL,
  commit_lsn TEXT NOT NULL,
  source_position TEXT NOT NULL,
  checkpoint_lsn TEXT NOT NULL,
  position_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  generation BIGINT NOT NULL,
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL,
  rooted_bytes BIGINT NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (flow_incarnation_id,source_position)
);

CREATE TABLE IF NOT EXISTS artifact_publication_objects (
  publication_id UUID NOT NULL REFERENCES artifact_publications(publication_id) ON DELETE RESTRICT,
  artifact_id TEXT NOT NULL REFERENCES artifact_objects(artifact_id) ON DELETE RESTRICT,
  ordinal BIGINT NOT NULL,
  PRIMARY KEY (publication_id,artifact_id),
  UNIQUE (publication_id,ordinal)
);

CREATE TABLE IF NOT EXISTS artifact_deliveries (
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  consumer_revision_id TEXT NOT NULL,
  publication_id UUID NOT NULL REFERENCES artifact_publications(publication_id) ON DELETE RESTRICT,
  sequence BIGSERIAL,
  bytes BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  delivered_at TIMESTAMPTZ,
  PRIMARY KEY (flow_incarnation_id,consumer_revision_id,publication_id),
  UNIQUE (flow_incarnation_id,consumer_revision_id,sequence)
);

CREATE TABLE IF NOT EXISTS artifact_quota_accounts (
  flow_incarnation_id UUID PRIMARY KEY REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  hard_limit_bytes BIGINT NOT NULL CHECK (hard_limit_bytes > 0),
  reserved_bytes BIGINT NOT NULL DEFAULT 0 CHECK (reserved_bytes >= 0),
  rooted_bytes BIGINT NOT NULL DEFAULT 0 CHECK (rooted_bytes >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS artifact_quota_reservations (
  artifact_id TEXT PRIMARY KEY REFERENCES artifact_objects(artifact_id) ON DELETE RESTRICT,
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  bytes BIGINT NOT NULL CHECK (bytes > 0),
  converted_at TIMESTAMPTZ,
  released_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (NOT (converted_at IS NOT NULL AND released_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS artifact_quota_reservations_flow_active_idx
  ON artifact_quota_reservations(flow_incarnation_id)
  WHERE converted_at IS NULL AND released_at IS NULL;

CREATE TABLE IF NOT EXISTS artifact_gc_claims (
  artifact_id TEXT PRIMARY KEY REFERENCES artifact_objects(artifact_id) ON DELETE RESTRICT,
  claim_epoch BIGINT NOT NULL,
  generation BIGINT NOT NULL,
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
