ALTER TABLE snapshot_delivery_attempts ADD COLUMN logical_batch_id TEXT;
ALTER TABLE snapshot_delivery_evidence ADD COLUMN logical_batch_id TEXT;
ALTER TABLE snapshot_delivery_receipts ADD COLUMN logical_batch_id TEXT;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM snapshot_delivery_attempts WHERE logical_batch_id IS NULL)
     OR EXISTS (SELECT 1 FROM snapshot_delivery_evidence WHERE logical_batch_id IS NULL)
     OR EXISTS (SELECT 1 FROM snapshot_delivery_receipts WHERE logical_batch_id IS NULL) THEN
    RAISE EXCEPTION 'legacy snapshot delivery rows lack logical batch identity; recreate the managed bootstrap';
  END IF;
END $$;

ALTER TABLE snapshot_delivery_attempts ALTER COLUMN logical_batch_id SET NOT NULL;
ALTER TABLE snapshot_delivery_evidence ALTER COLUMN logical_batch_id SET NOT NULL;
ALTER TABLE snapshot_delivery_receipts ALTER COLUMN logical_batch_id SET NOT NULL;
