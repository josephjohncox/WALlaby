-- Complete the managed bootstrap authority schema. All rows are keyed by the
-- immutable flow incarnation and carry positive producer provenance.
ALTER TABLE source_bootstraps
  ADD COLUMN IF NOT EXISTS selection_hash TEXT,
  ADD COLUMN IF NOT EXISTS exporter_execution_id TEXT,
  ADD COLUMN IF NOT EXISTS publication_revision TEXT,
  ADD COLUMN IF NOT EXISTS source_lineage_id TEXT;
UPDATE source_bootstraps
SET selection_hash = manifest_hash
WHERE selection_hash IS NULL;
ALTER TABLE source_bootstraps ALTER COLUMN selection_hash SET NOT NULL;

ALTER TABLE source_bootstrap_tasks
  ADD COLUMN IF NOT EXISTS flow_incarnation_id UUID,
  ADD COLUMN IF NOT EXISTS generation BIGINT,
  ADD COLUMN IF NOT EXISTS acquisition_id UUID,
  ADD COLUMN IF NOT EXISTS lease_epoch BIGINT,
  ADD COLUMN IF NOT EXISTS table_schema TEXT,
  ADD COLUMN IF NOT EXISTS table_name TEXT,
  ADD COLUMN IF NOT EXISTS schema_json JSONB,
  ADD COLUMN IF NOT EXISTS key_columns JSONB,
  ADD COLUMN IF NOT EXISTS batch_ordinal BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS receipt_count BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS authority_origin TEXT NOT NULL DEFAULT 'legacy_unfenced';

ALTER TABLE snapshot_publication_receipts
  ADD COLUMN IF NOT EXISTS flow_incarnation_id UUID,
  ADD COLUMN IF NOT EXISTS generation BIGINT,
  ADD COLUMN IF NOT EXISTS acquisition_id UUID,
  ADD COLUMN IF NOT EXISTS lease_epoch BIGINT,
  ADD COLUMN IF NOT EXISTS authority_origin TEXT NOT NULL DEFAULT 'legacy_unfenced';

ALTER TABLE source_bootstrap_tasks DROP CONSTRAINT IF EXISTS source_bootstrap_tasks_authority_complete;
ALTER TABLE source_bootstrap_tasks ADD CONSTRAINT source_bootstrap_tasks_authority_complete CHECK (
  (authority_origin='legacy_unfenced' AND flow_incarnation_id IS NULL AND generation IS NULL AND acquisition_id IS NULL AND lease_epoch IS NULL)
  OR
  (authority_origin='fenced' AND flow_incarnation_id IS NOT NULL AND generation > 0 AND acquisition_id IS NOT NULL AND lease_epoch > 0)
);
ALTER TABLE snapshot_publication_receipts DROP CONSTRAINT IF EXISTS snapshot_publication_receipts_authority_complete;
ALTER TABLE snapshot_publication_receipts ADD CONSTRAINT snapshot_publication_receipts_authority_complete CHECK (
  (authority_origin='legacy_unfenced' AND flow_incarnation_id IS NULL AND generation IS NULL AND acquisition_id IS NULL AND lease_epoch IS NULL)
  OR
  (authority_origin='fenced' AND flow_incarnation_id IS NOT NULL AND generation > 0 AND acquisition_id IS NOT NULL AND lease_epoch > 0)
);

CREATE TABLE IF NOT EXISTS source_resources (
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  resource_kind TEXT NOT NULL CHECK (resource_kind IN ('slot','publication','ddl_capture','source_state')),
  resource_id UUID NOT NULL,
  flow_id TEXT NOT NULL,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL REFERENCES execution_acquisitions(acquisition_id) ON DELETE RESTRICT,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  source_system_id TEXT NOT NULL,
  database_name TEXT NOT NULL,
  physical_name TEXT NOT NULL,
  ownership TEXT NOT NULL CHECK (ownership IN ('owned','adopted')),
  revision TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('prepared','ready','cleanup_pending','retired')),
  bootstrap_id UUID REFERENCES source_bootstraps(bootstrap_id) ON DELETE RESTRICT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_incarnation_id,resource_kind,resource_id),
  UNIQUE (source_system_id,database_name,resource_kind,physical_name)
);
CREATE UNIQUE INDEX IF NOT EXISTS source_resources_current_kind_idx
  ON source_resources(flow_incarnation_id,resource_kind)
  WHERE state IN ('prepared','ready','cleanup_pending');

CREATE TABLE IF NOT EXISTS source_resource_operations (
  operation_id UUID PRIMARY KEY,
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  resource_kind TEXT NOT NULL,
  resource_id UUID NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('create','adopt','update','drop','mark_streaming')),
  desired_revision TEXT NOT NULL,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL REFERENCES execution_acquisitions(acquisition_id) ON DELETE RESTRICT,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  status TEXT NOT NULL CHECK (status IN ('prepared','applied','indeterminate','rejected')),
  bootstrap_id UUID,
  source_system_id TEXT,
  database_name TEXT,
  physical_name TEXT,
  external_evidence JSONB,
  prepared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  completed_at TIMESTAMPTZ,
  UNIQUE (flow_incarnation_id,resource_kind,resource_id,operation,desired_revision,acquisition_id,lease_epoch)
);

CREATE TABLE IF NOT EXISTS snapshot_delivery_attempts (
  attempt_id UUID PRIMARY KEY,
  bootstrap_id UUID NOT NULL REFERENCES source_bootstraps(bootstrap_id) ON DELETE RESTRICT,
  relation_id OID NOT NULL,
  task_id TEXT NOT NULL,
  batch_ordinal BIGINT NOT NULL CHECK (batch_ordinal > 0),
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL REFERENCES execution_acquisitions(acquisition_id) ON DELETE RESTRICT,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  claim_epoch BIGINT NOT NULL CHECK (claim_epoch > 0),
  destination_revision_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  prepared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (bootstrap_id,relation_id,task_id,batch_ordinal)
);

CREATE TABLE IF NOT EXISTS snapshot_delivery_evidence (
  attempt_id UUID PRIMARY KEY REFERENCES snapshot_delivery_attempts(attempt_id) ON DELETE RESTRICT,
  external_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS snapshot_delivery_receipts (
  bootstrap_id UUID NOT NULL REFERENCES source_bootstraps(bootstrap_id) ON DELETE RESTRICT,
  relation_id OID NOT NULL,
  task_id TEXT NOT NULL,
  batch_ordinal BIGINT NOT NULL,
  attempt_id UUID NOT NULL REFERENCES snapshot_delivery_attempts(attempt_id) ON DELETE RESTRICT,
  position_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  external_id TEXT NOT NULL,
  durable_cursor JSONB,
  completed_task BOOLEAN NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (bootstrap_id,relation_id,task_id,batch_ordinal)
);

CREATE OR REPLACE FUNCTION wallaby_require_authority_protocol_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('wallaby.authority_protocol', true) IS DISTINCT FROM 'v1' THEN
    RAISE EXCEPTION 'this database requires a Wallaby authority-v1 client; stop the stale binary and upgrade it before retrying'
      USING ERRCODE = '42501';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END
$$;

DO $$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'source_bootstraps','source_bootstrap_tasks','snapshot_publication_receipts',
    'source_resources','source_resource_operations','snapshot_delivery_attempts',
    'snapshot_delivery_evidence','snapshot_delivery_receipts'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_require_authority_v1 ON %I', table_name, table_name);
    EXECUTE format('CREATE TRIGGER %I_require_authority_v1 BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1()', table_name, table_name);
  END LOOP;
END
$$;
