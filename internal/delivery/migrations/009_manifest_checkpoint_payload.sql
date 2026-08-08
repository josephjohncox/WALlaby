-- Persist the complete immutable source checkpoint before destination I/O so
-- commit-before-receipt recovery never depends on replay caller payload.
-- WALlaby does not support legacy delivery-state compatibility: historical
-- manifests cannot be assigned missing checkpoint payloads safely.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.delivery_manifests) THEN
    RAISE EXCEPTION 'delivery checkpoint-payload migration requires empty delivery manifests; existing checkpoint payloads are not inferred';
  END IF;
END
$$;

ALTER TABLE public.delivery_manifests
  ADD COLUMN checkpoint_metadata JSONB NOT NULL,
  ADD COLUMN checkpoint_timestamp TIMESTAMPTZ NOT NULL;
