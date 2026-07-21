CREATE OR REPLACE FUNCTION wallaby_require_authority_protocol_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'authority-v1 is retired; stop the stale v1 binary and upgrade it before retrying' USING ERRCODE='42501';
END
$$;

CREATE OR REPLACE FUNCTION wallaby_require_authority_protocol_v2()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('wallaby.authority_protocol',true) IS DISTINCT FROM 'v2' THEN
    RAISE EXCEPTION 'this database requires a Wallaby authority-v2 client; stop the stale v1 binary and upgrade it before retrying' USING ERRCODE='42501';
  END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END
$$;

DO $$
DECLARE dirty_ids TEXT;
BEGIN
  SELECT string_agg(id::text,',' ORDER BY id)
  INTO dirty_ids
  FROM (SELECT id FROM ddl_events WHERE authority_origin='fenced' AND COALESCE(lsn,'')='' ORDER BY id LIMIT 20) AS dirty;
  IF dirty_ids IS NOT NULL THEN
    RAISE EXCEPTION 'registry authority-v2 cutover found fenced DDL events without WAL LSN (sample event ids: %); stop workers, restore each source WAL identity or delete the invalid pre-release rows, then retry',dirty_ids
      USING ERRCODE='23514';
  END IF;
END
$$;

ALTER TABLE ddl_events DROP CONSTRAINT IF EXISTS ddl_events_fenced_lsn_required;
ALTER TABLE ddl_events ADD CONSTRAINT ddl_events_fenced_lsn_required
  CHECK (authority_origin <> 'fenced' OR COALESCE(lsn,'') <> '');
DROP INDEX IF EXISTS ddl_events_fenced_flow_lsn_unique;
CREATE UNIQUE INDEX ddl_events_fenced_incarnation_lsn_unique
  ON ddl_events(flow_incarnation_id,lsn)
  WHERE authority_origin='fenced';

DO $$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'schema_versions','ddl_events','ddl_execution_attempts','ddl_execution_receipts',
    'ddl_execution_manifests','ddl_execution_run_attempts','schema_publication_operations'
  ] LOOP
    -- Registry-only migration is supported before workflow authority exists.
    -- 006 conditionally omitted the two FK-backed relations in that case; a
    -- separately-ledgered controlplane repair creates them after workflow.
    IF to_regclass(table_name) IS NOT NULL THEN
      EXECUTE format('DROP TRIGGER IF EXISTS %I_require_authority_v1 ON %I',table_name,table_name);
      EXECUTE format('DROP TRIGGER IF EXISTS %I_require_authority_v2 ON %I',table_name,table_name);
      EXECUTE format('CREATE TRIGGER %I_require_authority_v2 BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2()',table_name,table_name);
    END IF;
  END LOOP;
END
$$;
