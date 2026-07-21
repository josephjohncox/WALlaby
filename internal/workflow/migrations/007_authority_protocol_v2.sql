-- Canonical authority-v2 cutover. A v1 worker can still hold an execution or
-- producer lease even when no row is currently being changed, so do not swap
-- the mutation triggers until every live owner has drained.
LOCK TABLE flows, flow_executions, execution_acquisitions, producer_leases, work_claims IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
  IF EXISTS (
       SELECT 1 FROM flow_executions
       WHERE status='running' AND (lease_expires_at IS NULL OR lease_expires_at > clock_timestamp())
     ) OR EXISTS (
       SELECT 1
       FROM producer_leases AS producer
       JOIN execution_acquisitions AS acquisition ON acquisition.acquisition_id=producer.acquisition_id
       WHERE producer.lease_expires_at > clock_timestamp() AND acquisition.finished_at IS NULL
     ) THEN
    RAISE EXCEPTION 'Wallaby authority-v2 cutover requires a quiesced upgrade: live v1 execution/producer owners exist; stop and drain all v1 workers before retrying'
      USING ERRCODE='55000';
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION wallaby_require_authority_protocol_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'authority-v1 is retired; stop the stale v1 binary and upgrade it before retrying' USING ERRCODE='42501';
END
$$;

CREATE OR REPLACE FUNCTION wallaby_require_authority_protocol_v2()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('wallaby.authority_protocol', true) IS DISTINCT FROM 'v2' THEN
    RAISE EXCEPTION 'this database requires a Wallaby authority-v2 client; stop the stale v1 binary and upgrade it before retrying'
      USING ERRCODE='42501';
  END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END
$$;

DO $$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'flows','flow_incarnations','flow_state_events','flow_executions',
    'execution_acquisitions','producer_leases','work_claims'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_require_authority_v1 ON %I',table_name,table_name);
    EXECUTE format('DROP TRIGGER IF EXISTS %I_require_authority_v2 ON %I',table_name,table_name);
    EXECUTE format('CREATE TRIGGER %I_require_authority_v2 BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2()',table_name,table_name);
  END LOOP;
END
$$;
