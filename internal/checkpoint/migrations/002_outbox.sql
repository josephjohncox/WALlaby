CREATE TABLE IF NOT EXISTS checkpoint_outbox (
  flow_id TEXT NOT NULL,
  destination_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  batch_hash TEXT NOT NULL,
  codec TEXT NOT NULL,
  batch_json BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (flow_id, destination_id, position_id)
);

CREATE INDEX IF NOT EXISTS checkpoint_outbox_flow_created_idx
  ON checkpoint_outbox (flow_id, created_at, destination_id);
