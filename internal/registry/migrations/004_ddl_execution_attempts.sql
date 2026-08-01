CREATE TABLE IF NOT EXISTS ddl_execution_attempts (
  event_id BIGINT NOT NULL REFERENCES ddl_events(id) ON DELETE CASCADE,
  destination TEXT NOT NULL,
  flow_id TEXT,
  lsn TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, destination)
);

CREATE INDEX IF NOT EXISTS idx_ddl_execution_attempts_flow_lsn
  ON ddl_execution_attempts(flow_id, lsn);
