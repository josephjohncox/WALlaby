-- Retired resources are immutable history, not active ownership claims. The
-- original table-level uniqueness constraint prevented a later flow
-- incarnation from safely reusing a physical name after fenced cleanup had
-- proved the old resource absent.
ALTER TABLE source_resources
  DROP CONSTRAINT IF EXISTS source_resources_source_system_id_database_name_resource_ki_key;

CREATE UNIQUE INDEX IF NOT EXISTS source_resources_active_physical_name_unique
  ON source_resources(source_system_id,database_name,resource_kind,physical_name)
  WHERE state <> 'retired';
