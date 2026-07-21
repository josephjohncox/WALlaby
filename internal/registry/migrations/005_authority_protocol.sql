CREATE OR REPLACE FUNCTION wallaby_require_authority_protocol_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF current_setting('wallaby.authority_protocol', true) IS DISTINCT FROM 'v1' THEN
    RAISE EXCEPTION 'this database requires a Wallaby authority-v1 client; stop the stale binary and upgrade it before retrying'
      USING ERRCODE = '42501';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS schema_versions_require_authority_v1 ON schema_versions;
CREATE TRIGGER schema_versions_require_authority_v1
BEFORE INSERT OR UPDATE OR DELETE ON schema_versions
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();

DROP TRIGGER IF EXISTS ddl_events_require_authority_v1 ON ddl_events;
CREATE TRIGGER ddl_events_require_authority_v1
BEFORE INSERT OR UPDATE OR DELETE ON ddl_events
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();

DROP TRIGGER IF EXISTS ddl_execution_attempts_require_authority_v1 ON ddl_execution_attempts;
CREATE TRIGGER ddl_execution_attempts_require_authority_v1
BEFORE INSERT OR UPDATE OR DELETE ON ddl_execution_attempts
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();

DROP TRIGGER IF EXISTS ddl_execution_receipts_require_authority_v1 ON ddl_execution_receipts;
CREATE TRIGGER ddl_execution_receipts_require_authority_v1
BEFORE INSERT OR UPDATE OR DELETE ON ddl_execution_receipts
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();
