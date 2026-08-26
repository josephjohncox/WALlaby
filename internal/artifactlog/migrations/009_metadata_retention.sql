-- Metadata pruning is a durable, PostgreSQL-authoritative operation. Claims do
-- not reference artifact_publications so the publication and claim can be
-- removed in separate row-bounded transactions after all restrictive children
-- are gone. artifact_ids freezes the original root set before links are pruned.
CREATE TABLE artifact_metadata_prune_claims (
  publication_id UUID PRIMARY KEY,
  flow_incarnation_id UUID NOT NULL REFERENCES flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  claim_epoch BIGINT NOT NULL CHECK (claim_epoch > 0),
  artifact_ids JSONB NOT NULL,
  eligible_at TIMESTAMPTZ NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT artifact_metadata_prune_claims_artifact_ids_array
    CHECK (jsonb_typeof(artifact_ids)='array')
);

CREATE INDEX artifact_metadata_prune_claims_flow_idx
  ON artifact_metadata_prune_claims(flow_incarnation_id,claimed_at,publication_id);
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
