CREATE TABLE IF NOT EXISTS ddl_execution_manifests (
  event_id BIGINT PRIMARY KEY REFERENCES ddl_events(id) ON DELETE CASCADE,
  destinations TEXT[] NOT NULL,
  manifest_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ddl_execution_receipts (
  event_id BIGINT NOT NULL REFERENCES ddl_events(id) ON DELETE CASCADE,
  destination TEXT NOT NULL,
  flow_id TEXT,
  lsn TEXT NOT NULL,
  receipt_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, destination),
  UNIQUE (receipt_hash)
);

CREATE INDEX IF NOT EXISTS idx_ddl_execution_receipts_flow_lsn
  ON ddl_execution_receipts(flow_id, lsn);
