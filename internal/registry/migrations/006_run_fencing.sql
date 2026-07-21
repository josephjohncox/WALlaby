-- Add complete-or-legacy provenance. Fenced writes cannot use zero/default
-- authority, while historical and compatibility rows remain readable.
ALTER TABLE schema_versions
  ADD COLUMN IF NOT EXISTS flow_incarnation_id UUID,
  ADD COLUMN IF NOT EXISTS generation BIGINT,
  ADD COLUMN IF NOT EXISTS acquisition_id UUID,
  ADD COLUMN IF NOT EXISTS lease_epoch BIGINT,
  ADD COLUMN IF NOT EXISTS authority_origin TEXT NOT NULL DEFAULT 'legacy_unfenced';
ALTER TABLE ddl_events
  ADD COLUMN IF NOT EXISTS flow_incarnation_id UUID,
  ADD COLUMN IF NOT EXISTS generation BIGINT,
  ADD COLUMN IF NOT EXISTS acquisition_id UUID,
  ADD COLUMN IF NOT EXISTS lease_epoch BIGINT,
  ADD COLUMN IF NOT EXISTS authority_origin TEXT NOT NULL DEFAULT 'legacy_unfenced';
ALTER TABLE ddl_execution_attempts
  ADD COLUMN IF NOT EXISTS flow_incarnation_id UUID,
  ADD COLUMN IF NOT EXISTS generation BIGINT,
  ADD COLUMN IF NOT EXISTS acquisition_id UUID,
  ADD COLUMN IF NOT EXISTS lease_epoch BIGINT,
  ADD COLUMN IF NOT EXISTS authority_origin TEXT NOT NULL DEFAULT 'legacy_unfenced';
ALTER TABLE ddl_execution_receipts
  ADD COLUMN IF NOT EXISTS flow_incarnation_id UUID,
  ADD COLUMN IF NOT EXISTS generation BIGINT,
  ADD COLUMN IF NOT EXISTS acquisition_id UUID,
  ADD COLUMN IF NOT EXISTS lease_epoch BIGINT,
  ADD COLUMN IF NOT EXISTS authority_origin TEXT NOT NULL DEFAULT 'legacy_unfenced';

DO $$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY['schema_versions','ddl_events','ddl_execution_attempts','ddl_execution_receipts'] LOOP
    EXECUTE format('ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I', table_name, table_name || '_authority_complete');
    EXECUTE format($constraint$
      ALTER TABLE %I ADD CONSTRAINT %I CHECK (
        (authority_origin='legacy_unfenced' AND flow_incarnation_id IS NULL AND generation IS NULL AND acquisition_id IS NULL AND lease_epoch IS NULL)
        OR
        (authority_origin='fenced' AND flow_incarnation_id IS NOT NULL AND generation > 0 AND acquisition_id IS NOT NULL AND lease_epoch > 0)
      )
    $constraint$, table_name, table_name || '_authority_complete');
  END LOOP;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS ddl_events_fenced_flow_lsn_unique
  ON ddl_events(flow_incarnation_id,lsn)
  WHERE authority_origin='fenced' AND lsn IS NOT NULL AND lsn <> '';

-- Legacy registry-only tools may run without lifecycle authority tables. They
-- retain read/legacy-write compatibility but cannot create fenced attempts.
-- The centralized production migrator runs workflow first, so production gets
-- the FK-backed tables and protocol triggers below. A registry-only database
-- that is later promoted to managed operation must run the centralized
-- migration entrypoint on a fresh migration ledger; missing fenced tables fail
-- closed rather than accepting an unfenced write.
DO $$
BEGIN
  DROP TRIGGER IF EXISTS ddl_execution_manifests_require_authority_v1 ON ddl_execution_manifests;
  CREATE TRIGGER ddl_execution_manifests_require_authority_v1
  BEFORE INSERT OR UPDATE OR DELETE ON ddl_execution_manifests
  FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();

  IF to_regclass('flow_incarnations') IS NULL OR to_regclass('execution_acquisitions') IS NULL THEN
    RAISE NOTICE 'skipping fenced registry operation tables: lifecycle authority schema is absent';
    RETURN;
  END IF;

  CREATE TABLE IF NOT EXISTS ddl_execution_run_attempts (
    attempt_id UUID PRIMARY KEY,
    event_id BIGINT NOT NULL REFERENCES ddl_events(id) ON DELETE CASCADE,
    destination TEXT NOT NULL,
    flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
    flow_id TEXT NOT NULL,
    lsn TEXT NOT NULL,
    generation BIGINT NOT NULL CHECK(generation > 0),
    acquisition_id UUID NOT NULL REFERENCES execution_acquisitions(acquisition_id) ON DELETE RESTRICT,
    lease_epoch BIGINT NOT NULL CHECK(lease_epoch > 0),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(event_id,destination,acquisition_id,lease_epoch)
  );

  CREATE TABLE IF NOT EXISTS schema_publication_operations (
    operation_id UUID PRIMARY KEY,
    flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
    flow_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL,
    registry_revision TEXT NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    acquisition_id UUID NOT NULL REFERENCES execution_acquisitions(acquisition_id) ON DELETE RESTRICT,
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
    status TEXT NOT NULL CHECK (status IN ('prepared','applied','indeterminate')),
    external_id TEXT,
    prepared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    UNIQUE(flow_incarnation_id,subject,schema_fingerprint,registry_revision)
  );

  DROP TRIGGER IF EXISTS ddl_execution_run_attempts_require_authority_v1 ON ddl_execution_run_attempts;
  CREATE TRIGGER ddl_execution_run_attempts_require_authority_v1
  BEFORE INSERT OR UPDATE OR DELETE ON ddl_execution_run_attempts
  FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();
  DROP TRIGGER IF EXISTS schema_publication_operations_require_authority_v1 ON schema_publication_operations;
  CREATE TRIGGER schema_publication_operations_require_authority_v1
  BEFORE INSERT OR UPDATE OR DELETE ON schema_publication_operations
  FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();
END
$$;
