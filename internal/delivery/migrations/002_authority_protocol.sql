ALTER TABLE source_ack_receipts
  ADD COLUMN IF NOT EXISTS generation BIGINT;

ALTER TABLE delivery_attempts DROP CONSTRAINT IF EXISTS delivery_attempts_authority_positive;
ALTER TABLE delivery_attempts ADD CONSTRAINT delivery_attempts_authority_positive CHECK (generation > 0 AND lease_epoch > 0);
ALTER TABLE delivery_attempt_evidence DROP CONSTRAINT IF EXISTS delivery_attempt_evidence_authority_positive;
ALTER TABLE delivery_attempt_evidence ADD CONSTRAINT delivery_attempt_evidence_authority_positive CHECK (recorded_by_lease_epoch > 0);
ALTER TABLE delivery_receipts DROP CONSTRAINT IF EXISTS delivery_receipts_authority_positive;
ALTER TABLE delivery_receipts ADD CONSTRAINT delivery_receipts_authority_positive CHECK (adopted_by_lease_epoch > 0);
ALTER TABLE source_ack_intents DROP CONSTRAINT IF EXISTS source_ack_intents_authority_positive;
ALTER TABLE source_ack_intents ADD CONSTRAINT source_ack_intents_authority_positive CHECK (generation > 0 AND lease_epoch > 0);

UPDATE source_ack_receipts AS receipt
SET generation=intent.generation
FROM source_ack_intents AS intent
WHERE receipt.flow_incarnation_id=intent.flow_incarnation_id
  AND receipt.position_id=intent.position_id
  AND receipt.generation IS NULL;
DO $$
BEGIN
  IF EXISTS(SELECT 1 FROM source_ack_receipts WHERE generation IS NULL OR lease_epoch <= 0) THEN
    RAISE EXCEPTION 'delivery authority migration requires ACK receipts with complete positive provenance; reconcile legacy rows before upgrade'
      USING ERRCODE='55000';
  END IF;
END
$$;
ALTER TABLE source_ack_receipts ALTER COLUMN generation SET NOT NULL;
ALTER TABLE source_ack_receipts DROP CONSTRAINT IF EXISTS source_ack_receipts_authority_positive;
ALTER TABLE source_ack_receipts ADD CONSTRAINT source_ack_receipts_authority_positive CHECK (generation > 0 AND lease_epoch > 0);

CREATE OR REPLACE FUNCTION wallaby_require_authority_protocol_v1()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('wallaby.authority_protocol', true) IS DISTINCT FROM 'v1' THEN
    RAISE EXCEPTION 'this database requires a Wallaby authority-v1 client; stop the stale binary and upgrade it before retrying'
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
    'destination_revisions','delivery_manifests','delivery_attempts',
    'delivery_attempt_evidence','delivery_receipts','source_ack_intents','source_ack_receipts'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_require_authority_v1 ON %I',table_name,table_name);
    EXECUTE format('CREATE TRIGGER %I_require_authority_v1 BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v1()',table_name,table_name);
  END LOOP;
END
$$;
