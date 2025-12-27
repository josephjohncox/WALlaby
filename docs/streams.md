# Streams

DuctStream includes a Postgres‑backed stream sink (`pgstream`) that behaves like a durable queue with consumer groups and visibility timeouts.
This is useful for workflow fan‑out, webhooks, and downstream processing pipelines.

## How It Works
- Each CDC record is encoded with the configured wire format and written into `stream_events`.
- For each consumer group, delivery state is tracked in `stream_deliveries`.
- `Pull` claims messages and sets a visibility timeout.
- `Ack` marks messages as delivered and prevents redelivery.
- If the timeout expires, messages become visible again.

## Basic Usage

### 1) Create a flow with `pgstream` destination
See `examples/flows/postgres_to_pgstream.json`.

### 2) Pull messages
```bash
./bin/ductstream-admin stream pull -stream orders -group search -max 10 -visibility 30
```

For scripting, use JSON output:

```bash
./bin/ductstream-admin stream pull --json -stream orders -group search -max 10 -visibility 30
```

For human-readable JSON:

```bash
./bin/ductstream-admin stream pull --pretty -stream orders -group search -max 10 -visibility 30
```

### 3) Ack messages
```bash
./bin/ductstream-admin stream ack -stream orders -group search -ids 1,2,3
```

## Visibility Timeout Semantics
- When a consumer pulls, the messages are marked `claimed` with a `visible_at` timestamp.
- If the consumer crashes before acking, messages are eligible for re‑delivery once `visible_at` passes.

## Replay + Recovery
You can reset deliveries for a consumer group in two ways:

```bash
./bin/ductstream-admin stream replay -stream orders -group search -from-lsn 0/16B6C50
```

```bash
./bin/ductstream-admin stream replay -stream orders -group search -since 2025-01-01T00:00:00Z
```

This moves matching deliveries back to `available` so they can be reprocessed.

## Failure Recovery Playbooks

### Consumer crash during processing
1. Do nothing; the visibility timeout will expire.
2. Messages become available for another consumer in the same group.

### Poison message / repeated failure
1. Inspect failing record via `pull`.
2. Fix downstream handler or apply filtering.
3. Use `replay` to reprocess from a specific LSN/time window.

### Rebuild downstream state
1. Run a backfill in the source (worker `-mode backfill`).
2. Replay stream deliveries from the earliest LSN or timestamp.

## Example Consumer
See `examples/stream_consumer.sh` (bash + jq) or `examples/stream_consumer.go` (pure Go) for minimal pull/ack loops.

Run the Go example:

```bash
go run examples/stream_consumer.go -endpoint localhost:8080 -stream orders -group search
```

## Operational Notes
- Use a distinct consumer group per downstream processor.
- Keep visibility timeout aligned with expected processing time.
- Treat `stream_events` as append‑only; retention policies can be added later (e.g., time‑based pruning after all groups ack).
