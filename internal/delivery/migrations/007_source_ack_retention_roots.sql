CREATE TABLE IF NOT EXISTS source_ack_retention_roots (
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  position_id TEXT NOT NULL,
  root_kind TEXT NOT NULL,
  root_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  released_at TIMESTAMPTZ,
  PRIMARY KEY (flow_incarnation_id,position_id,root_kind,root_id)
);

CREATE INDEX IF NOT EXISTS source_ack_retention_roots_active_idx
  ON source_ack_retention_roots(flow_incarnation_id,position_id)
  WHERE released_at IS NULL;
