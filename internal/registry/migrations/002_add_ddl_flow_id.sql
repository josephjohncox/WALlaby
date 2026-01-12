ALTER TABLE ddl_events
  ADD COLUMN IF NOT EXISTS flow_id TEXT;

CREATE INDEX IF NOT EXISTS ddl_events_flow_status_idx
  ON ddl_events (flow_id, status);

CREATE INDEX IF NOT EXISTS ddl_events_flow_lsn_idx
  ON ddl_events (flow_id, lsn);
