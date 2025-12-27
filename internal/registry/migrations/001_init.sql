CREATE TABLE IF NOT EXISTS schema_versions (
  id BIGSERIAL PRIMARY KEY,
  namespace TEXT NOT NULL,
  name TEXT NOT NULL,
  version BIGINT NOT NULL,
  schema_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(namespace, name, version)
);

CREATE TABLE IF NOT EXISTS ddl_events (
  id BIGSERIAL PRIMARY KEY,
  namespace TEXT,
  name TEXT,
  ddl TEXT,
  plan_json JSONB,
  lsn TEXT,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  applied_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ddl_events_status_idx ON ddl_events(status);
