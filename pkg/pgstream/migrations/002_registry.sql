ALTER TABLE IF EXISTS stream_events
  ADD COLUMN IF NOT EXISTS registry_subject TEXT,
  ADD COLUMN IF NOT EXISTS registry_id TEXT,
  ADD COLUMN IF NOT EXISTS registry_version INT;
