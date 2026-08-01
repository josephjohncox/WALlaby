-- Introduce immutable flow incarnations and lease-epoch ownership without
-- changing the six public lifecycle states. Migration 006 is also a quiesced
-- cutover: pre-006 workers cannot supply acquisition/lease provenance.
LOCK TABLE flows, flow_executions IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM flows WHERE state IN ('running', 'stopping'))
     OR EXISTS (SELECT 1 FROM flow_executions WHERE status = 'running') THEN
    RAISE EXCEPTION 'Wallaby authority migration 006 requires a quiesced upgrade: running/stopping flows or active executions exist; stop and drain all workers before retrying'
      USING ERRCODE = '55000';
  END IF;
END
$$;

CREATE TABLE IF NOT EXISTS flow_incarnations (
  incarnation_id UUID PRIMARY KEY,
  flow_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  retired_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS flow_incarnations_flow_idx
  ON flow_incarnations (flow_id, created_at DESC);

ALTER TABLE flows ADD COLUMN IF NOT EXISTS incarnation_id UUID;
UPDATE flows SET incarnation_id = gen_random_uuid() WHERE incarnation_id IS NULL;
INSERT INTO flow_incarnations (incarnation_id, flow_id)
SELECT incarnation_id, id FROM flows
ON CONFLICT (incarnation_id) DO NOTHING;
ALTER TABLE flows ALTER COLUMN incarnation_id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS flows_incarnation_unique ON flows (incarnation_id);

CREATE OR REPLACE FUNCTION wallaby_register_flow_incarnation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.incarnation_id IS NULL THEN
    NEW.incarnation_id := gen_random_uuid();
  END IF;
  INSERT INTO flow_incarnations (incarnation_id, flow_id)
  VALUES (NEW.incarnation_id, NEW.id);
  RETURN NEW;
END
$$;
DROP TRIGGER IF EXISTS wallaby_register_flow_incarnation_trigger ON flows;
CREATE TRIGGER wallaby_register_flow_incarnation_trigger
BEFORE INSERT ON flows
FOR EACH ROW EXECUTE FUNCTION wallaby_register_flow_incarnation();

CREATE OR REPLACE FUNCTION wallaby_retire_flow_incarnation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  UPDATE flow_incarnations
  SET retired_at = COALESCE(retired_at, clock_timestamp())
  WHERE incarnation_id = OLD.incarnation_id;
  RETURN OLD;
END
$$;
DROP TRIGGER IF EXISTS wallaby_retire_flow_incarnation_trigger ON flows;
CREATE TRIGGER wallaby_retire_flow_incarnation_trigger
AFTER DELETE ON flows
FOR EACH ROW EXECUTE FUNCTION wallaby_retire_flow_incarnation();

ALTER TABLE flow_executions ADD COLUMN IF NOT EXISTS incarnation_id UUID;
UPDATE flow_executions AS execution
SET incarnation_id = flow.incarnation_id
FROM flows AS flow
WHERE execution.flow_id = flow.id AND execution.incarnation_id IS NULL;
ALTER TABLE flow_executions ALTER COLUMN incarnation_id SET NOT NULL;
ALTER TABLE flow_executions DROP CONSTRAINT IF EXISTS flow_executions_flow_id_fkey;
ALTER TABLE flow_executions DROP CONSTRAINT IF EXISTS flow_executions_pkey;
ALTER TABLE flow_executions ADD CONSTRAINT flow_executions_pkey PRIMARY KEY (incarnation_id, execution_id);
ALTER TABLE flow_executions DROP CONSTRAINT IF EXISTS flow_executions_incarnation_id_fkey;
ALTER TABLE flow_executions ADD CONSTRAINT flow_executions_incarnation_id_fkey
  FOREIGN KEY (incarnation_id) REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT;
CREATE INDEX IF NOT EXISTS flow_executions_incarnation_active_idx
  ON flow_executions (incarnation_id, generation) WHERE status = 'running';

ALTER TABLE flow_state_events ADD COLUMN IF NOT EXISTS incarnation_id UUID;
UPDATE flow_state_events AS event
SET incarnation_id = flow.incarnation_id
FROM flows AS flow
WHERE event.flow_id = flow.id AND event.incarnation_id IS NULL;
ALTER TABLE flow_state_events DROP CONSTRAINT IF EXISTS flow_state_events_flow_id_fkey;
ALTER TABLE flow_state_events DROP CONSTRAINT IF EXISTS flow_state_events_incarnation_id_fkey;
ALTER TABLE flow_state_events ADD CONSTRAINT flow_state_events_incarnation_id_fkey
  FOREIGN KEY (incarnation_id) REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT;

CREATE TABLE IF NOT EXISTS execution_acquisitions (
  acquisition_id UUID PRIMARY KEY,
  incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK (generation > 0),
  execution_id TEXT NOT NULL,
  backend TEXT NOT NULL,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  finished_at TIMESTAMPTZ,
  finish_reason TEXT,
  UNIQUE (incarnation_id, acquisition_id),
  UNIQUE (incarnation_id, generation, lease_epoch)
);

CREATE TABLE IF NOT EXISTS producer_leases (
  incarnation_id UUID PRIMARY KEY REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL REFERENCES execution_acquisitions(acquisition_id) ON DELETE RESTRICT,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  lease_expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS work_claims (
  incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  claim_kind TEXT NOT NULL,
  work_id TEXT NOT NULL,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL REFERENCES execution_acquisitions(acquisition_id) ON DELETE RESTRICT,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  claim_epoch BIGINT NOT NULL CHECK (claim_epoch > 0),
  claim_expires_at TIMESTAMPTZ NOT NULL,
  released_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (incarnation_id, claim_kind, work_id)
);
CREATE INDEX IF NOT EXISTS work_claims_owner_idx
  ON work_claims (incarnation_id, acquisition_id, lease_epoch)
  WHERE released_at IS NULL;

CREATE OR REPLACE FUNCTION wallaby_require_authority_protocol_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF current_setting('wallaby.authority_protocol', true) IS DISTINCT FROM 'v1' THEN
    RAISE EXCEPTION 'this database requires a Wallaby authority-v1 client; stop the stale binary and upgrade it before retrying'
      USING ERRCODE = '42501';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS flows_require_authority_v1 ON flows;
CREATE TRIGGER flows_require_authority_v1
BEFORE INSERT OR UPDATE OR DELETE ON flows
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();

DROP TRIGGER IF EXISTS flow_executions_require_authority_v1 ON flow_executions;
CREATE TRIGGER flow_executions_require_authority_v1
BEFORE INSERT OR UPDATE OR DELETE ON flow_executions
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();
