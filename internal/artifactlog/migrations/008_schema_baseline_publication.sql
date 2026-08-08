-- Canonical publication identity binds the exact decoder baseline advanced in
-- the same root/checkpoint/ACK transaction. Existing rows are never inferred.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.artifact_publications) THEN
    RAISE EXCEPTION 'artifact baseline-binding migration requires empty publications; existing baseline payloads are not inferred';
  END IF;
END
$$;
ALTER TABLE public.artifact_publications
  ADD COLUMN schema_baseline_payload JSONB NOT NULL,
  ADD COLUMN schema_baseline_fingerprint TEXT NOT NULL,
  ADD CONSTRAINT artifact_publications_schema_baseline_fingerprint_check
    CHECK (schema_baseline_fingerprint ~ '^[0-9a-f]{64}$');
