ALTER TABLE canonical_schemas ADD COLUMN mapping_fingerprint TEXT;
ALTER TABLE artifact_streams ADD COLUMN mapping_fingerprint TEXT;
ALTER TABLE artifact_publications ADD COLUMN projection_id TEXT;
ALTER TABLE artifact_publications ADD COLUMN mapping_fingerprint TEXT;
ALTER TABLE artifact_objects ADD COLUMN projection_id TEXT;
ALTER TABLE artifact_objects ADD COLUMN mapping_fingerprint TEXT;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM canonical_schemas WHERE mapping_fingerprint IS NULL)
     OR EXISTS (SELECT 1 FROM artifact_streams WHERE mapping_fingerprint IS NULL)
     OR EXISTS (SELECT 1 FROM artifact_publications WHERE projection_id IS NULL OR mapping_fingerprint IS NULL)
     OR EXISTS (SELECT 1 FROM artifact_objects WHERE projection_id IS NULL OR mapping_fingerprint IS NULL) THEN
    RAISE EXCEPTION 'legacy artifact rows lack explicit projection v2 identity; recreate the materialized flow';
  END IF;
END $$;

ALTER TABLE canonical_schemas ALTER COLUMN mapping_fingerprint SET NOT NULL;
ALTER TABLE artifact_streams ALTER COLUMN mapping_fingerprint SET NOT NULL;
ALTER TABLE artifact_publications ALTER COLUMN projection_id SET NOT NULL;
ALTER TABLE artifact_publications ALTER COLUMN mapping_fingerprint SET NOT NULL;
ALTER TABLE artifact_objects ALTER COLUMN projection_id SET NOT NULL;
ALTER TABLE artifact_objects ALTER COLUMN mapping_fingerprint SET NOT NULL;

ALTER TABLE canonical_schemas ADD CONSTRAINT canonical_schemas_projection_mapping_contract CHECK (
  (projection_id='canonical_cdc_parquet_v1' AND mapping_fingerprint='') OR
  (projection_id='canonical_cdc_parquet_v2' AND mapping_fingerprint<>'')
);
ALTER TABLE artifact_streams ADD CONSTRAINT artifact_streams_projection_mapping_contract CHECK (
  (projection_id='canonical_cdc_parquet_v1' AND mapping_fingerprint='') OR
  (projection_id='canonical_cdc_parquet_v2' AND mapping_fingerprint<>'')
);
ALTER TABLE artifact_objects ADD CONSTRAINT artifact_objects_projection_mapping_contract CHECK (
  (projection_id='canonical_cdc_parquet_v1' AND mapping_fingerprint='') OR
  (projection_id='canonical_cdc_parquet_v2' AND mapping_fingerprint<>'')
);
ALTER TABLE artifact_publications ADD CONSTRAINT artifact_publications_projection_mapping_contract CHECK (
  (projection_id='canonical_cdc_parquet_v1' AND mapping_fingerprint='') OR
  (projection_id='canonical_cdc_parquet_v2' AND mapping_fingerprint<>'')
);
