-- Migration 005 changes the meaning of running work. An old worker has no
-- generation fence or lease, so upgrading it in place would make that worker
-- impossible to account for safely. Hold writers out and fail closed until an
-- operator has stopped and drained every legacy running/stopping flow.
LOCK TABLE flows, flow_executions IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM flows WHERE state IN ('running', 'stopping'))
     OR EXISTS (SELECT 1 FROM flow_executions WHERE status = 'running') THEN
    RAISE EXCEPTION 'Wallaby lifecycle migration 005 requires a quiesced upgrade: legacy running/stopping flows or active executions exist; stop and drain all workers before retrying'
      USING ERRCODE = '55000';
  END IF;
END
$$;

ALTER TABLE flows
  ADD COLUMN IF NOT EXISTS lifecycle_target TEXT;
ALTER TABLE flows
  ADD COLUMN IF NOT EXISTS lifecycle_generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE flows
  ADD COLUMN IF NOT EXISTS dispatch_pending BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE flows
SET lifecycle_target = CASE state
  WHEN 'running' THEN 'running'
  WHEN 'paused' THEN 'paused'
  WHEN 'stopped' THEN 'stopped'
  WHEN 'failed' THEN 'failed'
  ELSE 'created'
END
WHERE lifecycle_target IS NULL;

ALTER TABLE flows
  ALTER COLUMN lifecycle_target SET NOT NULL;
ALTER TABLE flows DROP CONSTRAINT IF EXISTS flows_lifecycle_target_check;
ALTER TABLE flows ADD CONSTRAINT flows_lifecycle_target_check
  CHECK (lifecycle_target IN ('created', 'running', 'paused', 'stopped', 'failed'));

ALTER TABLE flow_executions
  ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE flow_executions
  ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE flow_executions
  ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE flow_executions
  ADD COLUMN IF NOT EXISTS finish_reason TEXT;

UPDATE flow_executions AS execution
SET generation = flow.lifecycle_generation
FROM flows AS flow
WHERE execution.flow_id = flow.id
  AND execution.status = 'running'
  AND execution.generation = 0;

CREATE INDEX IF NOT EXISTS flow_executions_generation_active_idx
  ON flow_executions (flow_id, generation) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS flows_lifecycle_reconcile_idx
  ON flows (dispatch_pending, lifecycle_target, state)
  WHERE dispatch_pending OR state = 'stopping' OR lifecycle_target <> state;
