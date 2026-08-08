DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM checkpoint_outbox LIMIT 1)
     OR EXISTS (SELECT 1 FROM authoritative_checkpoint_outbox LIMIT 1) THEN
    RAISE EXCEPTION 'checkpoint outbox contains legacy rows without authoritative projection fingerprints and replay order; reconcile or remove them before upgrade';
  END IF;
END $$;

ALTER TABLE checkpoint_outbox
  ADD COLUMN projection_fingerprint TEXT NOT NULL,
  ADD COLUMN replay_order BIGINT GENERATED ALWAYS AS IDENTITY;
ALTER TABLE authoritative_checkpoint_outbox
  ADD COLUMN projection_fingerprint TEXT NOT NULL,
  ADD COLUMN replay_order BIGINT GENERATED ALWAYS AS IDENTITY;
CREATE UNIQUE INDEX checkpoint_outbox_replay_order_idx ON checkpoint_outbox(replay_order);
CREATE INDEX checkpoint_outbox_flow_replay_idx ON checkpoint_outbox(flow_id,replay_order);
CREATE UNIQUE INDEX authoritative_outbox_replay_order_idx ON authoritative_checkpoint_outbox(replay_order);
CREATE INDEX authoritative_outbox_pending_replay_idx ON authoritative_checkpoint_outbox(flow_incarnation_id,replay_order) WHERE delivered_at IS NULL;
