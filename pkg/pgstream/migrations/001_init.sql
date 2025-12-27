CREATE TABLE IF NOT EXISTS stream_events (
  id BIGSERIAL PRIMARY KEY,
  stream TEXT NOT NULL,
  namespace TEXT,
  table_name TEXT,
  lsn TEXT,
  wire_format TEXT,
  payload BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS stream_events_stream_idx ON stream_events(stream, id);
CREATE INDEX IF NOT EXISTS stream_events_stream_lsn_idx ON stream_events(stream, lsn);

CREATE TABLE IF NOT EXISTS stream_deliveries (
  event_id BIGINT NOT NULL REFERENCES stream_events(id) ON DELETE CASCADE,
  consumer_group TEXT NOT NULL,
  status TEXT NOT NULL,
  visible_at TIMESTAMPTZ NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  consumer_id TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, consumer_group)
);

CREATE INDEX IF NOT EXISTS stream_deliveries_visible_idx ON stream_deliveries(consumer_group, status, visible_at);
