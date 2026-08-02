-- Preserve immutable creation provenance while allowing a later producer
-- generation to adopt the live bootstrap/resource owner capability.
ALTER TABLE source_bootstraps
  ADD COLUMN IF NOT EXISTS owner_generation BIGINT,
  ADD COLUMN IF NOT EXISTS owner_acquisition_id UUID,
  ADD COLUMN IF NOT EXISTS owner_lease_epoch BIGINT;
UPDATE source_bootstraps
SET owner_generation = generation,
    owner_acquisition_id = acquisition_id,
    owner_lease_epoch = lease_epoch
WHERE owner_generation IS NULL OR owner_acquisition_id IS NULL OR owner_lease_epoch IS NULL;
ALTER TABLE source_bootstraps
  ALTER COLUMN owner_generation SET NOT NULL,
  ALTER COLUMN owner_acquisition_id SET NOT NULL,
  ALTER COLUMN owner_lease_epoch SET NOT NULL;
ALTER TABLE source_bootstraps DROP CONSTRAINT IF EXISTS source_bootstraps_owner_generation_positive;
ALTER TABLE source_bootstraps ADD CONSTRAINT source_bootstraps_owner_generation_positive CHECK (owner_generation > 0);
ALTER TABLE source_bootstraps DROP CONSTRAINT IF EXISTS source_bootstraps_owner_lease_epoch_positive;
ALTER TABLE source_bootstraps ADD CONSTRAINT source_bootstraps_owner_lease_epoch_positive CHECK (owner_lease_epoch > 0);

ALTER TABLE source_resources
  ADD COLUMN IF NOT EXISTS created_generation BIGINT,
  ADD COLUMN IF NOT EXISTS created_acquisition_id UUID,
  ADD COLUMN IF NOT EXISTS created_lease_epoch BIGINT;
UPDATE source_resources
SET created_generation = generation,
    created_acquisition_id = acquisition_id,
    created_lease_epoch = lease_epoch
WHERE created_generation IS NULL OR created_acquisition_id IS NULL OR created_lease_epoch IS NULL;
ALTER TABLE source_resources
  ALTER COLUMN created_generation SET NOT NULL,
  ALTER COLUMN created_acquisition_id SET NOT NULL,
  ALTER COLUMN created_lease_epoch SET NOT NULL;
ALTER TABLE source_resources DROP CONSTRAINT IF EXISTS source_resources_created_generation_positive;
ALTER TABLE source_resources ADD CONSTRAINT source_resources_created_generation_positive CHECK (created_generation > 0);
ALTER TABLE source_resources DROP CONSTRAINT IF EXISTS source_resources_created_lease_epoch_positive;
ALTER TABLE source_resources ADD CONSTRAINT source_resources_created_lease_epoch_positive CHECK (created_lease_epoch > 0);
