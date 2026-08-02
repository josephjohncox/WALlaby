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
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY['checkpoints','checkpoint_outbox','authoritative_checkpoints','authoritative_checkpoint_outbox'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_require_authority_v1 ON %I',table_name,table_name);
    EXECUTE format('DROP TRIGGER IF EXISTS %I_require_authority_v2 ON %I',table_name,table_name);
    EXECUTE format('CREATE TRIGGER %I_require_authority_v2 BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2()',table_name,table_name);
  END LOOP;
END
$$;
