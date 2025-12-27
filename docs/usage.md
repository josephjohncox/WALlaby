# Usage

This guide covers how to run DuctStream and operate flows using the gRPC API, worker mode, and DBOS scheduling.

## Prerequisites
- PostgreSQL with logical replication enabled (`wal_level=logical`).
- A replication slot + publication for the source database (DuctStream can create the publication/slot when permitted).
- Destinations (Kafka, S3) reachable from the DuctStream process.

## API Server
Start the gRPC API server:

```bash
export DUCTSTREAM_POSTGRES_DSN="postgres://user:pass@localhost:5432/ductstream?sslmode=disable"
export DUCTSTREAM_GRPC_LISTEN=":8080"
export DUCTSTREAM_WIRE_FORMAT="arrow"
export DUCTSTREAM_WIRE_ENFORCE="true"
./bin/ductstream
```

### Create a Flow (gRPC)
Use `grpcurl` with local proto files:

```bash
examples/grpc/create_flow.sh
```

Flow definitions can also be copied from `examples/flows/*.json`.

## Worker Mode (Per-Flow Process)
Run a single flow in its own process. This is recommended for Kubernetes or when you want isolated scaling per flow.

```bash
./bin/ductstream-worker -flow-id "<flow-id>"
```

Add `-max-empty-reads 1` to stop after one empty poll. This is useful for periodic/scheduled runs.

## DBOS Scheduling
Enable DBOS and (optionally) a schedule to run flows as durable jobs:

```bash
export DUCTSTREAM_DBOS_ENABLED="true"
export DUCTSTREAM_DBOS_APP="ductstream"
export DUCTSTREAM_DBOS_QUEUE="ductstream"
export DUCTSTREAM_DBOS_SCHEDULE="*/10 * * * * *" # every 10 seconds
```

If `DUCTSTREAM_DBOS_SCHEDULE` is set, DBOS enqueues one run for each flow in `running` state.

## Wire Formats
DuctStream supports multiple wire formats. Set the default at the service level or per-flow:

```bash
export DUCTSTREAM_WIRE_FORMAT="arrow"   # arrow | parquet | avro | proto | json
export DUCTSTREAM_WIRE_ENFORCE="true"
```

Per-flow overrides are supported via `flow.wire_format` or connector `options.format`.

## Postgres Source Options
Key Postgres source options (connector `options`):
- `dsn` (required)
- `slot` (required; created automatically when `create_slot=true`)
- `publication` (required)
- `create_slot` (default `true`)
- `ensure_publication` (default `true`) — create publication if missing
- `publication_tables` (optional) — comma-separated list for publication creation
- `validate_replication` (default `true`) — checks `wal_level`, `max_replication_slots`, `max_wal_senders`
- `ensure_state` (default `true`) — creates a durable source-state table for cleanup and auditing
- `state_schema` (default `ductstream`)
- `state_table` (default `source_state`)
- `flow_id` (optional) — stable ID used in source-state records

## HTTP/Webhook Destination
Use the HTTP destination to push each change as a request to an API endpoint:

Key options:
- `url` (required)
- `method` (default `POST`)
- `format` (default `json`)
- `headers` (comma-separated `Key:Value`)
- `max_retries`, `backoff_base`, `backoff_max`, `backoff_factor`
- `idempotency_header` (default `Idempotency-Key`)

The idempotency key is derived from `(table, primary key, lsn)` and hashed to a fixed string.

## Postgres Stream Destination
The `pgstream` destination writes events into a Postgres-backed stream with consumer groups and visibility timeouts.

Destination options:
- `dsn` (required)
- `stream` (defaults to the destination name)
- `format` (default `json`)

Consumers can pull from the stream using the StreamService or the admin CLI:

```bash
./bin/ductstream-admin stream pull -stream orders -group search -max 10 -visibility 30
```

Ack messages when processed:

```bash
./bin/ductstream-admin stream ack -stream orders -group search -ids 1,2,3
```

## DDL Governance
Enable gating to require approval before applying DDL-derived schema changes:

```bash
export DUCTSTREAM_DDL_GATE="true"
export DUCTSTREAM_DDL_AUTO_APPROVE="false"
export DUCTSTREAM_DDL_AUTO_APPLY="false"
```

Use the admin CLI to list and approve DDL events:

```bash
./bin/ductstream-admin ddl list -status pending
./bin/ductstream-admin ddl approve -id 1
./bin/ductstream-admin ddl apply -id 1
```

## Backfill + Replay
Run a backfill by switching the worker to `backfill` mode and providing tables:

```bash
./bin/ductstream-worker -flow-id \"<flow-id>\" -mode backfill -tables public.users,public.orders
```

Replay from a specific LSN (if your replication slot retains WAL):

```bash
./bin/ductstream-worker -flow-id \"<flow-id>\" -start-lsn \"0/16B6C50\"
```

For Postgres stream consumers, use the admin CLI to reset deliveries:

```bash
./bin/ductstream-admin stream replay -stream orders -group search -since 2025-01-01T00:00:00Z
```

## Checkpointing
Checkpoints are stored in Postgres and are used to resume streams from the last confirmed LSN. If a checkpoint exists, the runner sets `start_lsn` on the source before opening the replication stream.
