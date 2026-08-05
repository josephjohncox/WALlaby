-- Freeze the mapped destination schema and write contract separately from the
-- source query schema. Existing task rows cannot be inferred safely because
-- projection, filtering, and write policy participate in delivery identity.
ALTER TABLE source_bootstrap_tasks
  ADD COLUMN destination_schema_json JSONB,
  ADD COLUMN write_policy_json JSONB,
  ADD COLUMN projection_fingerprint TEXT,
  ADD COLUMN projection_version BIGINT;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM source_bootstrap_tasks
    WHERE destination_schema_json IS NULL
       OR write_policy_json IS NULL
       OR projection_fingerprint IS NULL
       OR btrim(projection_fingerprint) = ''
       OR projection_version IS NULL
  ) THEN
    RAISE EXCEPTION 'legacy snapshot tasks lack an immutable destination delivery contract; abandon and recreate the bootstrap generation'
      USING ERRCODE = '23514';
  END IF;
END
$$;

ALTER TABLE source_bootstrap_tasks
  ALTER COLUMN destination_schema_json SET NOT NULL,
  ALTER COLUMN write_policy_json SET NOT NULL,
  ALTER COLUMN projection_fingerprint SET NOT NULL,
  ALTER COLUMN projection_version SET NOT NULL,
  ADD CONSTRAINT source_bootstrap_tasks_projection_version_current CHECK (projection_version = 1),
  ADD CONSTRAINT source_bootstrap_tasks_projection_fingerprint_current CHECK (
    projection_fingerprint = btrim(projection_fingerprint)
    AND projection_fingerprint <> ''
  ),
  ADD CONSTRAINT source_bootstrap_tasks_destination_contract_json CHECK (
    jsonb_typeof(destination_schema_json) = 'object'
    AND jsonb_typeof(write_policy_json) = 'object'
  );
