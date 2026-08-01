ALTER TABLE flows DROP CONSTRAINT IF EXISTS flows_state_check;
ALTER TABLE flows ADD CONSTRAINT flows_state_check
  CHECK (state IN ('created', 'running', 'paused', 'stopping', 'stopped', 'failed'));

CREATE TABLE IF NOT EXISTS flow_executions (
  flow_id TEXT NOT NULL REFERENCES flows(id) ON DELETE CASCADE,
  execution_id TEXT NOT NULL,
  backend TEXT,
  status TEXT NOT NULL CHECK (status IN ('running', 'finished')),
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  PRIMARY KEY (flow_id, execution_id)
);

CREATE INDEX IF NOT EXISTS flow_executions_active_idx
  ON flow_executions (flow_id) WHERE status = 'running';
