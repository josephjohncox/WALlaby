-- Schema baselines used by logical replication are flow-scoped. Catalog-only
-- snapshots use the explicit empty flow scope. Existing unscoped rows cannot
-- be attributed safely and must be reconciled or removed before upgrade.
ALTER TABLE public.schema_versions
  ADD COLUMN flow_id TEXT;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM ONLY public.schema_versions WHERE flow_id IS NULL) THEN
    RAISE EXCEPTION 'schema_versions contains legacy rows without an authoritative flow scope; reconcile or remove them before upgrade';
  END IF;
END
$$;

ALTER TABLE public.schema_versions
  ALTER COLUMN flow_id SET NOT NULL;

ALTER TABLE public.schema_versions
  DROP CONSTRAINT schema_versions_pkey;
ALTER TABLE public.schema_versions
  DROP CONSTRAINT schema_versions_namespace_name_version_key;
ALTER TABLE public.schema_versions
  ADD CONSTRAINT schema_versions_pkey PRIMARY KEY (flow_id, namespace, name, version);
