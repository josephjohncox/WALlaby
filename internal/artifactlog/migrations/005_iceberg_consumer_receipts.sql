ALTER TABLE artifact_delivery_attempts
  ADD COLUMN IF NOT EXISTS commit_id TEXT NOT NULL DEFAULT '';
ALTER TABLE artifact_delivery_attempts
  ADD COLUMN IF NOT EXISTS manifest_sha256 TEXT NOT NULL DEFAULT '';
ALTER TABLE artifact_delivery_attempts
  ADD COLUMN IF NOT EXISTS logical_batch_id TEXT NOT NULL DEFAULT '';
UPDATE artifact_delivery_attempts
SET commit_id='legacy:' || attempt_id::TEXT
WHERE commit_id='';
CREATE INDEX IF NOT EXISTS artifact_delivery_attempts_commit_idx
  ON artifact_delivery_attempts(flow_incarnation_id,consumer_revision_id,commit_id,prepared_at DESC);

ALTER TABLE artifact_delivery_receipts
  ADD COLUMN IF NOT EXISTS commit_id TEXT NOT NULL DEFAULT '';
ALTER TABLE artifact_delivery_receipts
  ADD COLUMN IF NOT EXISTS logical_batch_id TEXT NOT NULL DEFAULT '';
ALTER TABLE artifact_delivery_receipts
  ADD COLUMN IF NOT EXISTS publication_sequence BIGINT NOT NULL DEFAULT 0;
ALTER TABLE artifact_delivery_receipts
  ADD COLUMN IF NOT EXISTS position_id TEXT NOT NULL DEFAULT '';
ALTER TABLE artifact_delivery_receipts
  ADD COLUMN IF NOT EXISTS checkpoint_lsn TEXT NOT NULL DEFAULT '';
ALTER TABLE artifact_delivery_receipts
  ADD COLUMN IF NOT EXISTS snapshot_ids JSONB NOT NULL DEFAULT '{}'::JSONB;
UPDATE artifact_delivery_receipts AS receipt
SET logical_batch_id=publication.logical_batch_id,
    publication_sequence=publication.sequence,
    position_id=publication.position_id,
    checkpoint_lsn=publication.checkpoint_lsn,
    commit_id=CASE WHEN receipt.commit_id='' THEN 'legacy:' || receipt.attempt_id::TEXT ELSE receipt.commit_id END
FROM artifact_publications AS publication
WHERE publication.publication_id=receipt.publication_id;

CREATE TABLE IF NOT EXISTS artifact_consumer_checkpoints (
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  consumer_revision_id TEXT NOT NULL,
  publication_sequence BIGINT NOT NULL CHECK (publication_sequence > 0),
  publication_id UUID NOT NULL REFERENCES artifact_publications(publication_id) ON DELETE RESTRICT,
  position_id TEXT NOT NULL,
  checkpoint_lsn TEXT NOT NULL,
  commit_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  advanced_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_incarnation_id,consumer_revision_id),
  UNIQUE (flow_incarnation_id,consumer_revision_id,publication_sequence),
  UNIQUE (flow_incarnation_id,consumer_revision_id,publication_id)
);

INSERT INTO artifact_consumer_checkpoints (
  flow_incarnation_id,consumer_revision_id,publication_sequence,publication_id,
  position_id,checkpoint_lsn,commit_id,snapshot_id,advanced_at
)
SELECT DISTINCT ON (receipt.flow_incarnation_id,receipt.consumer_revision_id)
  receipt.flow_incarnation_id,receipt.consumer_revision_id,receipt.publication_sequence,
  receipt.publication_id,receipt.position_id,receipt.checkpoint_lsn,
  receipt.commit_id,receipt.snapshot_id,receipt.committed_at
FROM artifact_delivery_receipts AS receipt
WHERE receipt.publication_sequence > 0
ORDER BY receipt.flow_incarnation_id,receipt.consumer_revision_id,receipt.publication_sequence DESC
ON CONFLICT (flow_incarnation_id,consumer_revision_id) DO NOTHING;

DROP TRIGGER IF EXISTS artifact_consumer_checkpoints_require_authority_v2 ON artifact_consumer_checkpoints;
CREATE TRIGGER artifact_consumer_checkpoints_require_authority_v2
BEFORE INSERT OR UPDATE OR DELETE ON artifact_consumer_checkpoints
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2();
