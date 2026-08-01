CREATE TABLE IF NOT EXISTS artifact_delivery_attempts (
  attempt_id UUID PRIMARY KEY,
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  consumer_revision_id TEXT NOT NULL,
  publication_id UUID NOT NULL REFERENCES artifact_publications(publication_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL,
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL,
  prepared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS artifact_delivery_attempts_lookup_idx
  ON artifact_delivery_attempts (flow_incarnation_id,consumer_revision_id,publication_id,prepared_at DESC);
CREATE INDEX IF NOT EXISTS artifact_deliveries_pending_idx
  ON artifact_deliveries (flow_incarnation_id,consumer_revision_id,sequence)
  WHERE delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS artifact_delivery_receipts (
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  consumer_revision_id TEXT NOT NULL,
  publication_id UUID NOT NULL REFERENCES artifact_publications(publication_id) ON DELETE RESTRICT,
  attempt_id UUID NOT NULL REFERENCES artifact_delivery_attempts(attempt_id) ON DELETE RESTRICT,
  snapshot_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_incarnation_id,consumer_revision_id,publication_id)
);
CREATE INDEX IF NOT EXISTS artifact_delivery_receipts_attempt_idx
  ON artifact_delivery_receipts(attempt_id);
