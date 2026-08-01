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

DROP TRIGGER IF EXISTS checkpoints_require_authority_v1 ON checkpoints;
CREATE TRIGGER checkpoints_require_authority_v1
BEFORE INSERT OR UPDATE OR DELETE ON checkpoints
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();

DROP TRIGGER IF EXISTS checkpoint_outbox_require_authority_v1 ON checkpoint_outbox;
CREATE TRIGGER checkpoint_outbox_require_authority_v1
BEFORE INSERT OR UPDATE OR DELETE ON checkpoint_outbox
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1();
