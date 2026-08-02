-- Registry migrations 006/007 intentionally support registry-only databases
-- where workflow authority relations do not exist yet. Once the centralized
-- controlplane entrypoint has installed workflow, repair the skipped FK-backed
-- registry relations under a separate monotonic migration ledger. Keep every
-- name explicit: startup verification treats these objects as part of the
-- managed authority contract.
CREATE TABLE IF NOT EXISTS ddl_execution_run_attempts (
  attempt_id UUID,
  event_id BIGINT,
  destination TEXT,
  flow_incarnation_id UUID,
  flow_id TEXT,
  lsn TEXT,
  generation BIGINT,
  acquisition_id UUID,
  lease_epoch BIGINT,
  started_at TIMESTAMPTZ
);
ALTER TABLE ddl_execution_run_attempts
  ADD COLUMN IF NOT EXISTS attempt_id UUID,
  ADD COLUMN IF NOT EXISTS event_id BIGINT,
  ADD COLUMN IF NOT EXISTS destination TEXT,
  ADD COLUMN IF NOT EXISTS flow_incarnation_id UUID,
  ADD COLUMN IF NOT EXISTS flow_id TEXT,
  ADD COLUMN IF NOT EXISTS lsn TEXT,
  ADD COLUMN IF NOT EXISTS generation BIGINT,
  ADD COLUMN IF NOT EXISTS acquisition_id UUID,
  ADD COLUMN IF NOT EXISTS lease_epoch BIGINT,
  ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
ALTER TABLE ddl_execution_run_attempts
  ALTER COLUMN attempt_id SET NOT NULL,
  ALTER COLUMN event_id SET NOT NULL,
  ALTER COLUMN destination SET NOT NULL,
  ALTER COLUMN flow_incarnation_id SET NOT NULL,
  ALTER COLUMN flow_id SET NOT NULL,
  ALTER COLUMN lsn SET NOT NULL,
  ALTER COLUMN generation SET NOT NULL,
  ALTER COLUMN acquisition_id SET NOT NULL,
  ALTER COLUMN lease_epoch SET NOT NULL,
  ALTER COLUMN started_at SET DEFAULT clock_timestamp(),
  ALTER COLUMN started_at SET NOT NULL;

ALTER TABLE ddl_execution_run_attempts DROP CONSTRAINT IF EXISTS ddl_execution_run_attempts_pkey;
ALTER TABLE ddl_execution_run_attempts DROP CONSTRAINT IF EXISTS ddl_execution_run_attempts_event_id_fkey;
ALTER TABLE ddl_execution_run_attempts DROP CONSTRAINT IF EXISTS ddl_execution_run_attempts_flow_incarnation_id_fkey;
ALTER TABLE ddl_execution_run_attempts DROP CONSTRAINT IF EXISTS ddl_execution_run_attempts_acquisition_id_fkey;
ALTER TABLE ddl_execution_run_attempts DROP CONSTRAINT IF EXISTS ddl_execution_run_attempts_generation_check;
ALTER TABLE ddl_execution_run_attempts DROP CONSTRAINT IF EXISTS ddl_execution_run_attempts_lease_epoch_check;
ALTER TABLE ddl_execution_run_attempts DROP CONSTRAINT IF EXISTS ddl_execution_run_attempts_event_id_destination_acquisition_id_;
ALTER TABLE ddl_execution_run_attempts DROP CONSTRAINT IF EXISTS ddl_execution_run_attempts_owner_key;
ALTER TABLE ddl_execution_run_attempts ADD CONSTRAINT ddl_execution_run_attempts_pkey PRIMARY KEY (attempt_id);
ALTER TABLE ddl_execution_run_attempts ADD CONSTRAINT ddl_execution_run_attempts_event_id_fkey FOREIGN KEY (event_id) REFERENCES ddl_events(id) ON DELETE CASCADE;
ALTER TABLE ddl_execution_run_attempts ADD CONSTRAINT ddl_execution_run_attempts_flow_incarnation_id_fkey FOREIGN KEY (flow_incarnation_id) REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT;
ALTER TABLE ddl_execution_run_attempts ADD CONSTRAINT ddl_execution_run_attempts_acquisition_id_fkey FOREIGN KEY (acquisition_id) REFERENCES execution_acquisitions(acquisition_id) ON DELETE RESTRICT;
ALTER TABLE ddl_execution_run_attempts ADD CONSTRAINT ddl_execution_run_attempts_generation_check CHECK (generation > 0);
ALTER TABLE ddl_execution_run_attempts ADD CONSTRAINT ddl_execution_run_attempts_lease_epoch_check CHECK (lease_epoch > 0);
ALTER TABLE ddl_execution_run_attempts ADD CONSTRAINT ddl_execution_run_attempts_owner_key UNIQUE (event_id,destination,acquisition_id,lease_epoch);

CREATE TABLE IF NOT EXISTS schema_publication_operations (
  operation_id UUID,
  flow_incarnation_id UUID,
  flow_id TEXT,
  subject TEXT,
  schema_fingerprint TEXT,
  registry_revision TEXT,
  generation BIGINT,
  acquisition_id UUID,
  lease_epoch BIGINT,
  status TEXT,
  external_id TEXT,
  prepared_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ
);
ALTER TABLE schema_publication_operations
  ADD COLUMN IF NOT EXISTS operation_id UUID,
  ADD COLUMN IF NOT EXISTS flow_incarnation_id UUID,
  ADD COLUMN IF NOT EXISTS flow_id TEXT,
  ADD COLUMN IF NOT EXISTS subject TEXT,
  ADD COLUMN IF NOT EXISTS schema_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS registry_revision TEXT,
  ADD COLUMN IF NOT EXISTS generation BIGINT,
  ADD COLUMN IF NOT EXISTS acquisition_id UUID,
  ADD COLUMN IF NOT EXISTS lease_epoch BIGINT,
  ADD COLUMN IF NOT EXISTS status TEXT,
  ADD COLUMN IF NOT EXISTS external_id TEXT,
  ADD COLUMN IF NOT EXISTS prepared_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE schema_publication_operations
  ALTER COLUMN operation_id SET NOT NULL,
  ALTER COLUMN flow_incarnation_id SET NOT NULL,
  ALTER COLUMN flow_id SET NOT NULL,
  ALTER COLUMN subject SET NOT NULL,
  ALTER COLUMN schema_fingerprint SET NOT NULL,
  ALTER COLUMN registry_revision SET NOT NULL,
  ALTER COLUMN generation SET NOT NULL,
  ALTER COLUMN acquisition_id SET NOT NULL,
  ALTER COLUMN lease_epoch SET NOT NULL,
  ALTER COLUMN status SET NOT NULL,
  ALTER COLUMN prepared_at SET DEFAULT clock_timestamp(),
  ALTER COLUMN prepared_at SET NOT NULL;

ALTER TABLE schema_publication_operations DROP CONSTRAINT IF EXISTS schema_publication_operations_pkey;
ALTER TABLE schema_publication_operations DROP CONSTRAINT IF EXISTS schema_publication_operations_flow_incarnation_id_fkey;
ALTER TABLE schema_publication_operations DROP CONSTRAINT IF EXISTS schema_publication_operations_acquisition_id_fkey;
ALTER TABLE schema_publication_operations DROP CONSTRAINT IF EXISTS schema_publication_operations_generation_check;
ALTER TABLE schema_publication_operations DROP CONSTRAINT IF EXISTS schema_publication_operations_lease_epoch_check;
ALTER TABLE schema_publication_operations DROP CONSTRAINT IF EXISTS schema_publication_operations_status_check;
ALTER TABLE schema_publication_operations DROP CONSTRAINT IF EXISTS schema_publication_operations_flow_incarnation_id_subject_schem;
ALTER TABLE schema_publication_operations DROP CONSTRAINT IF EXISTS schema_publication_operations_identity_key;
ALTER TABLE schema_publication_operations ADD CONSTRAINT schema_publication_operations_pkey PRIMARY KEY (operation_id);
ALTER TABLE schema_publication_operations ADD CONSTRAINT schema_publication_operations_flow_incarnation_id_fkey FOREIGN KEY (flow_incarnation_id) REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT;
ALTER TABLE schema_publication_operations ADD CONSTRAINT schema_publication_operations_acquisition_id_fkey FOREIGN KEY (acquisition_id) REFERENCES execution_acquisitions(acquisition_id) ON DELETE RESTRICT;
ALTER TABLE schema_publication_operations ADD CONSTRAINT schema_publication_operations_generation_check CHECK (generation > 0);
ALTER TABLE schema_publication_operations ADD CONSTRAINT schema_publication_operations_lease_epoch_check CHECK (lease_epoch > 0);
ALTER TABLE schema_publication_operations ADD CONSTRAINT schema_publication_operations_status_check CHECK (status IN ('prepared','applied','indeterminate'));
ALTER TABLE schema_publication_operations ADD CONSTRAINT schema_publication_operations_identity_key UNIQUE (flow_incarnation_id,subject,schema_fingerprint,registry_revision);

DROP INDEX IF EXISTS ddl_execution_run_attempts_event_destination_idx;
DROP INDEX IF EXISTS ddl_execution_run_attempts_incarnation_idx;
DROP INDEX IF EXISTS ddl_execution_run_attempts_acquisition_idx;
DROP INDEX IF EXISTS schema_publication_operations_incarnation_idx;
DROP INDEX IF EXISTS schema_publication_operations_acquisition_idx;
CREATE INDEX ddl_execution_run_attempts_event_destination_idx
  ON ddl_execution_run_attempts(event_id,destination);
CREATE INDEX ddl_execution_run_attempts_incarnation_idx
  ON ddl_execution_run_attempts(flow_incarnation_id);
CREATE INDEX ddl_execution_run_attempts_acquisition_idx
  ON ddl_execution_run_attempts(acquisition_id);
CREATE INDEX schema_publication_operations_incarnation_idx
  ON schema_publication_operations(flow_incarnation_id);
CREATE INDEX schema_publication_operations_acquisition_idx
  ON schema_publication_operations(acquisition_id);

DROP TRIGGER IF EXISTS ddl_execution_run_attempts_require_authority_v1 ON ddl_execution_run_attempts;
DROP TRIGGER IF EXISTS ddl_execution_run_attempts_require_authority_v2 ON ddl_execution_run_attempts;
CREATE TRIGGER ddl_execution_run_attempts_require_authority_v2
BEFORE INSERT OR UPDATE OR DELETE ON ddl_execution_run_attempts
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2();
ALTER TABLE ddl_execution_run_attempts ENABLE TRIGGER ddl_execution_run_attempts_require_authority_v2;

DROP TRIGGER IF EXISTS schema_publication_operations_require_authority_v1 ON schema_publication_operations;
DROP TRIGGER IF EXISTS schema_publication_operations_require_authority_v2 ON schema_publication_operations;
CREATE TRIGGER schema_publication_operations_require_authority_v2
BEFORE INSERT OR UPDATE OR DELETE ON schema_publication_operations
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2();
ALTER TABLE schema_publication_operations ENABLE TRIGGER schema_publication_operations_require_authority_v2;
