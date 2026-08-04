ALTER TABLE artifact_streams
  ADD COLUMN IF NOT EXISTS projection_id TEXT NOT NULL DEFAULT 'canonical_cdc_parquet_v1';
UPDATE artifact_streams AS stream
SET projection_id='wallaby-canonical-parquet-arrow18-zstd3-us-v2'
WHERE EXISTS (
  SELECT 1
  FROM artifact_objects AS object
  JOIN canonical_schemas AS schema ON schema.schema_id=object.schema_id
  WHERE object.flow_incarnation_id=stream.flow_incarnation_id
    AND schema.projection_id<>'canonical_cdc_parquet_v1'
);
ALTER TABLE artifact_streams
  ADD COLUMN IF NOT EXISTS consumer_fingerprint TEXT NOT NULL DEFAULT '';
ALTER TABLE artifact_streams
  ADD COLUMN IF NOT EXISTS next_publication_sequence BIGINT NOT NULL DEFAULT 1;
ALTER TABLE artifact_streams
  ADD COLUMN IF NOT EXISTS gc_epoch BIGINT NOT NULL DEFAULT 0;

ALTER TABLE artifact_objects ADD COLUMN IF NOT EXISTS logical_batch_id TEXT;
ALTER TABLE artifact_objects ADD COLUMN IF NOT EXISTS namespace TEXT;
ALTER TABLE artifact_objects ADD COLUMN IF NOT EXISTS table_name TEXT;
ALTER TABLE artifact_objects ADD COLUMN IF NOT EXISTS partition_value TEXT;
ALTER TABLE artifact_objects ADD COLUMN IF NOT EXISTS shard INTEGER;
ALTER TABLE artifact_objects ADD COLUMN IF NOT EXISTS first_record_ordinal BIGINT;
ALTER TABLE artifact_objects ADD COLUMN IF NOT EXISTS record_count BIGINT;
UPDATE artifact_objects SET
  logical_batch_id=COALESCE(logical_batch_id,'legacy:' || source_position),
  namespace=COALESCE(namespace,''),
  table_name=COALESCE(table_name,''),
  partition_value=COALESCE(partition_value,'unpartitioned'),
  shard=COALESCE(shard,fragment_ordinal::INTEGER),
  first_record_ordinal=COALESCE(first_record_ordinal,fragment_ordinal),
  record_count=COALESCE(record_count,1);
ALTER TABLE artifact_objects ALTER COLUMN logical_batch_id SET NOT NULL;
ALTER TABLE artifact_objects ALTER COLUMN namespace SET NOT NULL;
ALTER TABLE artifact_objects ALTER COLUMN table_name SET NOT NULL;
ALTER TABLE artifact_objects ALTER COLUMN partition_value SET NOT NULL;
ALTER TABLE artifact_objects ALTER COLUMN shard SET NOT NULL;
ALTER TABLE artifact_objects ALTER COLUMN first_record_ordinal SET NOT NULL;
ALTER TABLE artifact_objects ALTER COLUMN record_count SET NOT NULL;
ALTER TABLE artifact_objects DROP CONSTRAINT IF EXISTS artifact_objects_flow_incarnation_id_source_position_fragment_ordinal_key;
CREATE UNIQUE INDEX IF NOT EXISTS artifact_objects_logical_shard_idx
  ON artifact_objects(flow_incarnation_id,logical_batch_id,namespace,table_name,schema_id,partition_value,shard);
ALTER TABLE artifact_objects DROP CONSTRAINT IF EXISTS artifact_objects_check;
ALTER TABLE artifact_objects DROP CONSTRAINT IF EXISTS artifact_objects_version_evidence;
ALTER TABLE artifact_objects ADD CONSTRAINT artifact_objects_version_evidence
  CHECK (state IN ('reserved','deleting','deleted') OR version_id IS NOT NULL);
ALTER TABLE artifact_objects DROP CONSTRAINT IF EXISTS artifact_objects_record_count_positive;
ALTER TABLE artifact_objects ADD CONSTRAINT artifact_objects_record_count_positive CHECK (record_count > 0);
ALTER TABLE artifact_objects DROP CONSTRAINT IF EXISTS artifact_objects_shard_nonnegative;
ALTER TABLE artifact_objects ADD CONSTRAINT artifact_objects_shard_nonnegative CHECK (shard >= 0);

ALTER TABLE artifact_upload_attempts
  ADD COLUMN IF NOT EXISTS attempt_state TEXT NOT NULL DEFAULT 'prepared';
ALTER TABLE artifact_upload_attempts ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE artifact_upload_attempts ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE artifact_upload_attempts DROP CONSTRAINT IF EXISTS artifact_upload_attempts_state_valid;
ALTER TABLE artifact_upload_attempts ADD CONSTRAINT artifact_upload_attempts_state_valid
  CHECK (attempt_state IN ('prepared','uploaded','verified','failed'));
CREATE INDEX IF NOT EXISTS artifact_upload_attempts_artifact_state_idx
  ON artifact_upload_attempts(artifact_id,attempt_state,generation,acquisition_id,lease_epoch);

ALTER TABLE artifact_publications ADD COLUMN IF NOT EXISTS logical_batch_id TEXT;
ALTER TABLE artifact_publications ADD COLUMN IF NOT EXISTS sequence BIGINT;
ALTER TABLE artifact_publications
  ADD COLUMN IF NOT EXISTS checkpoint_metadata JSONB NOT NULL DEFAULT '{}'::JSONB;
UPDATE artifact_publications SET logical_batch_id=COALESCE(logical_batch_id,'legacy:' || position_id);
WITH numbered AS (
  SELECT publication_id,row_number() OVER (
    PARTITION BY flow_incarnation_id ORDER BY published_at,publication_id
  ) AS sequence
  FROM artifact_publications
)
UPDATE artifact_publications AS publication
SET sequence=numbered.sequence
FROM numbered
WHERE publication.publication_id=numbered.publication_id
  AND publication.sequence IS NULL;
ALTER TABLE artifact_publications ALTER COLUMN logical_batch_id SET NOT NULL;
ALTER TABLE artifact_publications ALTER COLUMN sequence SET NOT NULL;
UPDATE artifact_streams AS stream
SET next_publication_sequence=COALESCE((
  SELECT max(publication.sequence)+1
  FROM artifact_publications AS publication
  WHERE publication.flow_incarnation_id=stream.flow_incarnation_id
),1);
CREATE UNIQUE INDEX IF NOT EXISTS artifact_publications_logical_batch_idx
  ON artifact_publications(flow_incarnation_id,logical_batch_id);
CREATE UNIQUE INDEX IF NOT EXISTS artifact_publications_sequence_idx
  ON artifact_publications(flow_incarnation_id,sequence);

ALTER TABLE artifact_publication_objects ADD COLUMN IF NOT EXISTS release_marked_at TIMESTAMPTZ;
ALTER TABLE artifact_publication_objects ADD COLUMN IF NOT EXISTS released_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS artifact_publication_objects_active_roots_idx
  ON artifact_publication_objects(artifact_id)
  WHERE release_marked_at IS NULL AND released_at IS NULL;

INSERT INTO source_ack_retention_roots(flow_incarnation_id,position_id,root_kind,root_id)
SELECT publication.flow_incarnation_id,publication.position_id,'artifact_publication',publication.publication_id::TEXT
FROM artifact_publications AS publication
WHERE EXISTS (
  SELECT 1 FROM artifact_publication_objects AS root
  WHERE root.publication_id=publication.publication_id AND root.released_at IS NULL
)
ON CONFLICT (flow_incarnation_id,position_id,root_kind,root_id) DO NOTHING;

DROP TRIGGER IF EXISTS source_ack_retention_roots_require_authority_v2 ON source_ack_retention_roots;
CREATE TRIGGER source_ack_retention_roots_require_authority_v2
BEFORE INSERT OR UPDATE OR DELETE ON source_ack_retention_roots
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2();

CREATE TABLE IF NOT EXISTS artifact_barriers (
  publication_id UUID NOT NULL REFERENCES artifact_publications(publication_id) ON DELETE RESTRICT,
  ordinal BIGINT NOT NULL,
  fragment_ordinal BIGINT NOT NULL,
  record_ordinal BIGINT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('ddl','schema')),
  namespace TEXT NOT NULL,
  table_name TEXT NOT NULL,
  schema_id TEXT,
  ddl TEXT NOT NULL DEFAULT '',
  ddl_plan BYTEA NOT NULL DEFAULT ''::BYTEA,
  content_hash TEXT NOT NULL,
  PRIMARY KEY (publication_id,ordinal),
  UNIQUE (publication_id,record_ordinal)
);

ALTER TABLE artifact_gc_claims
  ADD COLUMN IF NOT EXISTS claim_kind TEXT NOT NULL DEFAULT 'orphan';
ALTER TABLE artifact_gc_claims ADD COLUMN IF NOT EXISTS publication_id UUID;
ALTER TABLE artifact_gc_claims DROP CONSTRAINT IF EXISTS artifact_gc_claims_kind_valid;
ALTER TABLE artifact_gc_claims ADD CONSTRAINT artifact_gc_claims_kind_valid
  CHECK (claim_kind IN ('orphan','retention'));
ALTER TABLE artifact_gc_claims DROP CONSTRAINT IF EXISTS artifact_gc_claims_publication_kind;
ALTER TABLE artifact_gc_claims ADD CONSTRAINT artifact_gc_claims_publication_kind
  CHECK ((claim_kind='orphan' AND publication_id IS NULL) OR (claim_kind='retention' AND publication_id IS NOT NULL));

DROP TRIGGER IF EXISTS artifact_barriers_require_authority_v2 ON artifact_barriers;
CREATE TRIGGER artifact_barriers_require_authority_v2
BEFORE INSERT OR UPDATE OR DELETE ON artifact_barriers
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2();
