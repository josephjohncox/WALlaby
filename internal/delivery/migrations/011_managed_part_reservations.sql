CREATE TABLE managed_part_reservations (
  reservation_id UUID PRIMARY KEY,
  flow_incarnation_id UUID NOT NULL,
  flow_id TEXT NOT NULL,
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  destination_revision_id TEXT NOT NULL REFERENCES destination_revisions(destination_revision_id) ON DELETE RESTRICT,
  source_lineage_id TEXT NOT NULL,
  logical_batch_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  plan_hash TEXT NOT NULL CHECK (plan_hash ~ '^[0-9a-f]{64}$'),
  resource TEXT NOT NULL CHECK (resource = 'clickhouse_active_parts_v1'),
  server_active_parts BIGINT NOT NULL CHECK (server_active_parts >= 0),
  planned_parts INTEGER NOT NULL CHECK (planned_parts > 0),
  capacity BIGINT NOT NULL CHECK (capacity > 0),
  reservation_epoch BIGINT NOT NULL DEFAULT 1 CHECK (reservation_epoch > 0),
  reclaim_epoch BIGINT NOT NULL DEFAULT 0 CHECK (reclaim_epoch >= 0),
  observation_epoch BIGINT NOT NULL DEFAULT 1 CHECK (observation_epoch > 0),
  reservation_state TEXT NOT NULL DEFAULT 'reserved' CHECK (reservation_state IN ('reserved','completed_pending_observation','reclaim_pending','released')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  completed_at TIMESTAMPTZ,
  reclaim_started_at TIMESTAMPTZ,
  released_at TIMESTAMPTZ,
  CONSTRAINT managed_part_reservations_manifest_fkey FOREIGN KEY (
    flow_incarnation_id,destination_revision_id,logical_batch_id
  ) REFERENCES delivery_manifests (
    flow_incarnation_id,destination_revision_id,logical_batch_id
  ) ON DELETE RESTRICT,
  CONSTRAINT managed_part_reservations_state_complete CHECK (
    (reservation_state='reserved' AND completed_at IS NULL AND reclaim_started_at IS NULL AND released_at IS NULL) OR
    (reservation_state='completed_pending_observation' AND completed_at IS NOT NULL AND reclaim_started_at IS NULL AND released_at IS NULL) OR
    (reservation_state='reclaim_pending' AND completed_at IS NULL AND reclaim_started_at IS NOT NULL AND released_at IS NULL) OR
    (reservation_state='released' AND released_at IS NOT NULL)
  ),
  CONSTRAINT managed_part_reservations_identity_key UNIQUE (
    destination_revision_id,logical_batch_id
  )
);

CREATE INDEX managed_part_reservations_budget_idx
  ON managed_part_reservations(destination_revision_id,resource,reservation_state)
  WHERE reservation_state IN ('reserved','completed_pending_observation','reclaim_pending');

CREATE TABLE managed_part_reservation_parts (
  reservation_id UUID NOT NULL REFERENCES managed_part_reservations(reservation_id) ON DELETE RESTRICT,
  part_kind TEXT NOT NULL CHECK (part_kind IN ('changelog','receipt')),
  part_ordinal BIGINT NOT NULL CHECK (part_ordinal >= 0),
  query_id TEXT NOT NULL,
  part_state TEXT NOT NULL DEFAULT 'reserved' CHECK (part_state IN ('reserved','durable','released')),
  charge_state TEXT NOT NULL DEFAULT 'charged' CHECK (charge_state IN ('charged','observed','released')),
  durable_at TIMESTAMPTZ,
  observed_at TIMESTAMPTZ,
  released_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT managed_part_reservation_parts_pkey PRIMARY KEY (reservation_id,part_kind,part_ordinal),
  CONSTRAINT managed_part_reservation_parts_query_id_key UNIQUE (query_id),
  CONSTRAINT managed_part_reservation_parts_state_complete CHECK (
    (part_state='reserved' AND durable_at IS NULL AND charge_state='charged' AND observed_at IS NULL AND released_at IS NULL) OR
    (part_state='durable' AND durable_at IS NOT NULL AND charge_state IN ('charged','observed') AND released_at IS NULL) OR
    (part_state='released' AND released_at IS NOT NULL AND charge_state='released')
  ),
  CONSTRAINT managed_part_reservation_parts_observation_complete CHECK (
    (charge_state='charged' AND observed_at IS NULL) OR
    (charge_state='observed' AND observed_at IS NOT NULL AND released_at IS NULL) OR
    (charge_state='released' AND released_at IS NOT NULL)
  )
);

CREATE TABLE managed_part_reservation_events (
  event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  reservation_id UUID NOT NULL REFERENCES managed_part_reservations(reservation_id) ON DELETE RESTRICT,
  reservation_epoch BIGINT NOT NULL CHECK (reservation_epoch > 0),
  observation_epoch BIGINT NOT NULL CHECK (observation_epoch > 0),
  reclaim_epoch BIGINT NOT NULL CHECK (reclaim_epoch >= 0),
  event_kind TEXT NOT NULL CHECK (event_kind IN ('reserved','adopted','observed','completed','reclaim_started','released','rereserved')),
  generation BIGINT NOT NULL CHECK (generation > 0),
  acquisition_id UUID NOT NULL,
  lease_epoch BIGINT NOT NULL CHECK (lease_epoch > 0),
  server_active_parts BIGINT NOT NULL CHECK (server_active_parts >= 0),
  charged_parts BIGINT NOT NULL CHECK (charged_parts >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX managed_part_reservation_events_reservation_idx
  ON managed_part_reservation_events(reservation_id,event_id);

CREATE TRIGGER managed_part_reservations_require_authority_v2
  BEFORE INSERT OR UPDATE OR DELETE ON managed_part_reservations
  FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2();

CREATE TRIGGER managed_part_reservation_parts_require_authority_v2
  BEFORE INSERT OR UPDATE OR DELETE ON managed_part_reservation_parts
  FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2();

CREATE TRIGGER managed_part_reservation_events_require_authority_v2
  BEFORE INSERT OR UPDATE OR DELETE ON managed_part_reservation_events
  FOR EACH ROW EXECUTE FUNCTION wallaby_require_authority_protocol_v2();
