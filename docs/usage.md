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

Flow fields you can set:
- `wire_format` — default wire format for the flow
- `parallelism` — max concurrent destination writes per batch (default `1`)

To reconfigure destinations or wire format, call `UpdateFlow` with a full `Flow` payload; state is preserved.

## Worker Mode (Per-Flow Process)
Run a single flow in its own process. This is recommended for Kubernetes or when you want isolated scaling per flow.

```bash
./bin/ductstream-worker -flow-id "<flow-id>"
```

Add `-max-empty-reads 1` to stop after one empty poll. This is useful for periodic/scheduled runs.
For backfill runs that land in staging tables, add `-resolve-staging` to apply staged data without relying on process shutdown.

## DBOS Scheduling
Enable DBOS and (optionally) a schedule to run flows as durable jobs:

```bash
export DUCTSTREAM_DBOS_ENABLED="true"
export DUCTSTREAM_DBOS_APP="ductstream"
export DUCTSTREAM_DBOS_QUEUE="ductstream"
export DUCTSTREAM_DBOS_SCHEDULE="*/10 * * * * *" # every 10 seconds
```

If `DUCTSTREAM_DBOS_SCHEDULE` is set, DBOS enqueues one run for each flow in `running` state.

## Checkpoint Store
By default, checkpoints use Postgres when `DUCTSTREAM_POSTGRES_DSN` is set. For standalone runs, SQLite is supported:

```bash
export DUCTSTREAM_CHECKPOINT_BACKEND="sqlite"
export DUCTSTREAM_CHECKPOINT_PATH="$HOME/.ductstream/checkpoints.db"
```

Set `DUCTSTREAM_CHECKPOINT_DSN` to override the full SQLite DSN.

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
- `publication_schemas` (optional) — comma-separated schemas for auto-discovery
- `validate_replication` (default `true`) — checks `wal_level`, `max_replication_slots`, `max_wal_senders`
- `batch_size` (default `100`) — max records per batch
- `batch_timeout` (default `1s`) — flush interval when idle
- `status_interval` (default `10s`) — standby status update interval
- `emit_empty` (default `false`) — emit empty batches (useful for scheduled runs)
- `resolve_types` (default `true`) — resolve type OIDs using `pg_type` (captures extension types)
- `sync_publication` (default `false`) — add/drop tables at start
- `sync_publication_mode` (`add` default, or `sync` to drop extras)
- `ensure_state` (default `true`) — creates a durable source-state table for cleanup and auditing
- `state_schema` (default `ductstream`)
- `state_table` (default `source_state`)
- `flow_id` (optional) — stable ID used in source-state records

## Publication Lifecycle
Use `sync_publication` with `publication_tables` or `publication_schemas` to add/drop tables when a flow starts. For ad-hoc changes, the admin CLI can update the publication:

```bash
./bin/ductstream-admin publication list -flow-id "<flow-id>"
./bin/ductstream-admin publication sync -flow-id "<flow-id>" -schemas public -mode add -pause -resume
```

To add tables and snapshot them:

```bash
./bin/ductstream-admin publication sync -flow-id "<flow-id>" -tables public.new_table -snapshot -pause -resume
```

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

## Snowflake Destination
Write directly into Snowflake tables with row-level change application.

Options:
- `dsn` (required)
- `schema`, `table` (optional; defaults to source schema/table)
- `write_mode` (`target` default, or `append`)
- `batch_mode` (`target` default, or `staging` for backfill loads)
- `batch_resolution` (`none` default, or `append` / `replace`)
- `staging_schema`, `staging_table`, `staging_suffix` (default suffix `_staging`)
- `meta_table_enabled` (default `true`) — create/update `meta_schema.meta_table`
- `meta_schema` (default `DUCTSTREAM_META`)
- `meta_table` (default `__METADATA`)
- `meta_pk_prefix` (default `pk_`)

## Snowpipe Destination
Bulk-load batches to a Snowflake stage and optionally trigger `COPY INTO`.

Options:
- `dsn` (required)
- `stage` (required unless using table stage via `@%schema.table`)
- `stage_path` (optional prefix inside the stage)
- `schema`, `table` (required if `copy_on_write=true`)
- `format` (`parquet` default; `parquet` | `avro` | `json`)
- `file_format` (optional Snowflake file format object name)
- `copy_on_write` (default `true`)
- `auto_ingest` (default `false`) — set `true` to only upload (skip COPY)
- `meta_table_enabled` (default `true`)

Snowpipe runs in append mode; use `append_mode` + metadata columns to preserve operation semantics.

## DuckDB Destination
Write directly into DuckDB (local file or in-memory).

Options:
- `dsn` (required; e.g. `./data/warehouse.duckdb`)
- `schema`, `table` (optional; defaults to source schema/table)
- `write_mode` (`target` default, or `append`)
- `batch_mode` (`target` default, or `staging` for backfill loads)
- `batch_resolution` (`none` default, or `append` / `replace`)
- `staging_schema`, `staging_table`, `staging_suffix` (default suffix `_staging`)
- `meta_table_enabled` (default `true`)

## ClickHouse Destination
Apply changes using ClickHouse mutations.

Options:
- `dsn` (required)
- `database` or `schema`, `table` (optional; defaults to source namespace/table)
- `write_mode` (`target` default, or `append`)
- `batch_mode` (`target` default, or `staging` for backfill loads)
- `batch_resolution` (`none` default, or `append` / `replace`)
- `staging_schema`, `staging_table`, `staging_suffix` (default suffix `_staging`)
- `meta_table_enabled` (default `true`)
- `meta_engine` (default `MergeTree`)
- `meta_order_by` (default `flow_id, source_schema, source_table, key_json`)

## Bufstream Destination
Bufstream is Kafka-compatible; use the same options as the Kafka destination (`brokers`, `topic`, `format`, `compression`, `acks`).

## Destination Metadata + Append/Soft Delete
Destinations can opt into metadata columns (synced time, soft-delete flags, watermarks) and append-only behavior:

Options (on destination `options`):
- `meta_enabled` (default `false`) — enable metadata columns
- `meta_synced_at` (default `__ds_synced_at`)
- `meta_deleted` (default `__ds_is_deleted`)
- `meta_watermark` (default `__ds_watermark`, set `-` to disable)
- `meta_op` (default `__ds_op`)
- `watermark_source` (`timestamp` default, or `lsn`)
- `soft_delete` (default `false`) — converts deletes to updates with `meta_deleted=true`
- `append_mode` (default `false`) — converts updates/deletes to inserts and stores original op in `meta_op`

Metadata columns are added to the destination schema; choose names that do not collide with source columns.

## Destination Type Mappings
Destinations can override source types for downstream compatibility:

Options (on destination `options`):
- `type_mappings` — JSON map of source type → destination type
- `type_mappings_file` — path to a JSON map (same format)

Example:

```json
{"public.geometry": "GEOGRAPHY", "jsonb": "VARIANT"}
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

Backfill performance options (source `options`):
- `snapshot_workers` (default `1`) — parallel table/partition workers
- `parallel_tables` (alias of `snapshot_workers`)
- `partition_column` (optional) — column used to hash-partition a table
- `partition_count` (default `1`) — number of partitions per table
- `snapshot_consistent` (default `true`) — uses `pg_export_snapshot()` for a consistent snapshot
- `snapshot_state_backend` (`postgres` default, or `file`, `none`)
- `snapshot_state_schema` (default `ductstream`)
- `snapshot_state_table` (default `snapshot_state`)
- `snapshot_state_path` (required for `file` backend)

Example with parallel workers and hash partitions:

```bash
./bin/ductstream-worker -flow-id \"<flow-id>\" -mode backfill -tables public.users -snapshot-workers 4 -partition-column id -partition-count 8
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
