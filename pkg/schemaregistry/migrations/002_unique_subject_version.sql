DO $$
DECLARE
  duplicate RECORD;
BEGIN
  SELECT subject, version, count(*) AS copies
  INTO duplicate
  FROM wallaby_schema_registry
  GROUP BY subject, version
  HAVING count(*) > 1
  ORDER BY subject, version
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION USING
      ERRCODE = '23505',
      MESSAGE = format(
        'schema registry migration 002 cannot enforce unique subject/version: subject %L version %s has %s rows',
        duplicate.subject,
        duplicate.version,
        duplicate.copies
      ),
      HINT = 'Reconcile duplicate rows without changing externally published schema versions, then restart WALlaby to retry the migration.';
  END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS wallaby_schema_registry_subject_version_unique_idx
  ON wallaby_schema_registry (subject, version);
