CREATE TABLE IF NOT EXISTS wallaby_schema_registry (
  id BIGSERIAL PRIMARY KEY,
  subject TEXT NOT NULL,
  schema_type TEXT NOT NULL,
  schema TEXT NOT NULL,
  schema_hash TEXT NOT NULL,
  references JSONB NOT NULL DEFAULT '[]',
  references_hash TEXT NOT NULL,
  version INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS wallaby_schema_registry_subject_hash_idx
  ON wallaby_schema_registry (subject, schema_hash, references_hash);

CREATE INDEX IF NOT EXISTS wallaby_schema_registry_subject_version_idx
  ON wallaby_schema_registry (subject, version);
