CREATE TABLE IF NOT EXISTS checkpoints (
  flow_id TEXT PRIMARY KEY,
  lsn TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS checkpoints_updated_at_idx ON checkpoints (updated_at);
