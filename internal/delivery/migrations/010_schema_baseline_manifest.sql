-- Baseline advancement is inseparable from receipt/checkpoint/ACK finalization.
-- Existing current-schema manifests are not inferred or backfilled.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.delivery_manifests) THEN
    RAISE EXCEPTION 'delivery baseline-binding migration requires empty delivery manifests; existing baseline payloads are not inferred';
  END IF;
END
$$;
ALTER TABLE public.delivery_manifests
  ADD COLUMN schema_baseline_payload JSONB NOT NULL,
  ADD COLUMN schema_baseline_fingerprint TEXT NOT NULL,
  ADD CONSTRAINT delivery_manifests_schema_baseline_fingerprint_check
    CHECK (schema_baseline_fingerprint ~ '^[0-9a-f]{64}$');
