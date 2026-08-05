-- Managed logical-decoding schema baselines are control-plane authority. They
-- are isolated by immutable flow incarnation and source lineage and carry the
-- exact producer fence that last persisted or adopted them. Existing global
-- registry schema rows are deliberately not imported.
CREATE TABLE public.managed_schema_baselines (
  flow_id TEXT NOT NULL,
  flow_incarnation_id UUID NOT NULL,
  source_lineage_id TEXT NOT NULL,
  source_namespace TEXT NOT NULL,
  source_relation TEXT NOT NULL,
  generation BIGINT NOT NULL,
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL,
  schema_json JSONB NOT NULL,
  schema_fingerprint TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT managed_schema_baselines_pkey PRIMARY KEY (
    flow_id,flow_incarnation_id,source_lineage_id,source_namespace,source_relation
  ),
  CONSTRAINT managed_schema_baselines_flow_incarnation_id_fkey
    FOREIGN KEY (flow_incarnation_id) REFERENCES public.flow_incarnations(incarnation_id) ON DELETE RESTRICT,
  CONSTRAINT managed_schema_baselines_acquisition_id_fkey
    FOREIGN KEY (acquisition_id) REFERENCES public.execution_acquisitions(acquisition_id) ON DELETE RESTRICT,
  CONSTRAINT managed_schema_baselines_generation_check CHECK (generation > 0),
  CONSTRAINT managed_schema_baselines_lease_epoch_check CHECK (lease_epoch > 0),
  CONSTRAINT managed_schema_baselines_lineage_check CHECK (btrim(source_lineage_id) <> ''),
  CONSTRAINT managed_schema_baselines_namespace_check CHECK (source_namespace <> ''),
  CONSTRAINT managed_schema_baselines_relation_check CHECK (source_relation <> ''),
  CONSTRAINT managed_schema_baselines_fingerprint_check CHECK (schema_fingerprint ~ '^[0-9a-f]{64}$')
);
CREATE INDEX managed_schema_baselines_current_fence_idx
  ON public.managed_schema_baselines (
    flow_incarnation_id,generation,acquisition_id,lease_epoch,source_lineage_id
  );
CREATE TRIGGER managed_schema_baselines_require_authority_v2
BEFORE INSERT OR UPDATE OR DELETE ON public.managed_schema_baselines
FOR EACH ROW EXECUTE FUNCTION public.wallaby_require_authority_protocol_v2();
ALTER TABLE public.managed_schema_baselines ENABLE TRIGGER managed_schema_baselines_require_authority_v2;
