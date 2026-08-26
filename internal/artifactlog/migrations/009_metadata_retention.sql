-- Metadata pruning is a durable, PostgreSQL-authoritative operation. Claims do
-- not reference artifact_publications so the final publication/evidence bundle
-- and claim can be deleted atomically. Frozen object and schema IDs remain
-- available after partial root/object pruning; retry_after prevents a deferred
-- claim from monopolizing the bounded publication scan.
CREATE TABLE artifact_metadata_prune_claims (
  publication_id UUID PRIMARY KEY,
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  claim_epoch BIGINT NOT NULL CHECK (claim_epoch > 0),
  artifact_ids JSONB NOT NULL,
  schema_ids JSONB NOT NULL,
  catalog_evidence JSONB NOT NULL,
  eligible_at TIMESTAMPTZ NOT NULL,
  retry_after TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT artifact_metadata_prune_claims_artifact_ids_array
    CHECK (jsonb_typeof(artifact_ids)='array'),
  CONSTRAINT artifact_metadata_prune_claims_schema_ids_array
    CHECK (jsonb_typeof(schema_ids)='array'),
  CONSTRAINT artifact_metadata_prune_claims_catalog_evidence_object
    CHECK (jsonb_typeof(catalog_evidence)='object'
      AND jsonb_typeof(catalog_evidence->'publication')='object'
      AND jsonb_typeof(catalog_evidence->'consumers')='array')
);

CREATE INDEX artifact_metadata_prune_claims_flow_idx
  ON artifact_metadata_prune_claims(flow_incarnation_id,retry_after,claimed_at,publication_id);
CREATE INDEX artifact_publications_metadata_retention_idx
  ON artifact_publications(flow_incarnation_id,published_at,sequence,publication_id);
CREATE INDEX artifact_gc_claims_publication_idx
  ON artifact_gc_claims(publication_id) WHERE publication_id IS NOT NULL;
CREATE INDEX artifact_deliveries_publication_idx
  ON artifact_deliveries(publication_id,delivered_at);
CREATE INDEX artifact_delivery_attempts_publication_idx
  ON artifact_delivery_attempts(publication_id,attempt_id);

CREATE TRIGGER artifact_metadata_prune_claims_require_authority_v2
BEFORE INSERT OR UPDATE OR DELETE ON artifact_metadata_prune_claims
FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2();

-- A metadata claim is an authoritative tombstone. It freezes the exact terminal
-- catalog evidence before bounded sweeps remove the original rows, so no stale
-- producer can recreate or mutate children while pruning is in progress.
CREATE FUNCTION wallaby_reject_metadata_prune_dependent() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM artifact_metadata_prune_claims
    WHERE publication_id=NEW.publication_id
  ) THEN
    RAISE EXCEPTION 'artifact publication metadata is under authoritative retention'
      USING ERRCODE='55000';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER artifact_deliveries_reject_metadata_prune
BEFORE INSERT OR UPDATE ON artifact_deliveries
FOR EACH ROW EXECUTE FUNCTION wallaby_reject_metadata_prune_dependent();
CREATE TRIGGER artifact_delivery_attempts_reject_metadata_prune
BEFORE INSERT OR UPDATE ON artifact_delivery_attempts
FOR EACH ROW EXECUTE FUNCTION wallaby_reject_metadata_prune_dependent();
CREATE TRIGGER artifact_delivery_receipts_reject_metadata_prune
BEFORE INSERT OR UPDATE ON artifact_delivery_receipts
FOR EACH ROW EXECUTE FUNCTION wallaby_reject_metadata_prune_dependent();
