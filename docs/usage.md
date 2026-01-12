# Usage

This guide covers how to run WALlaby and operate flows using the gRPC API, worker mode, and DBOS scheduling.

Workflow mode templates live in `examples/workflows/` and a longer overview is in `docs/workflows.md`.

## Prerequisites
- PostgreSQL with logical replication enabled (`wal_level=logical`).
- A replication slot + publication for the source database (WALlaby can create the publication/slot when permitted).
- Destinations (Kafka, S3) reachable from the WALlaby process.

## API Server
Start the gRPC API server:

```bash
export WALLABY_POSTGRES_DSN="postgres://user:pass@localhost:5432/wallaby?sslmode=disable"
export WALLABY_GRPC_LISTEN=":8080"
export WALLABY_GRPC_REFLECTION="false"
export WALLABY_WIRE_FORMAT="arrow"
export WALLABY_WIRE_ENFORCE="true"
./bin/wallaby
```

## Kubernetes / Helm
Install the OCI Helm chart from GHCR (published on tagged releases):

```bash
helm install wallaby oci://ghcr.io/josephjohncox/wallaby/charts/wallaby --version <tag>
```

Set required env vars via Helm values (`env`) or a ConfigMap (`config.enabled=true` + `config.data`).
See `charts/wallaby/values.example.yaml` for a minimal example.

To run per-flow workers from the chart, enable `workers` and define one item per flow:

```yaml
workers:
  enabled: true
  items:
    - name: flow-a
      kind: deployment
      replicas: 1
      command: ["/usr/local/bin/wallaby-worker"]
      args: ["--flow-id=flow-a"]
```

Use `kind: job` for one-off runs or `kind: cronjob` with `schedule` for periodic backfills.

### Create a Flow (gRPC)
Use `grpcurl` with local proto files:

```bash
examples/grpc/create_flow.sh
```

Flow definitions can also be copied from `examples/flows/*.json`.

Flow fields you can set:
- `wire_format` — default wire format for the flow
- `parallelism` — max concurrent destination writes per batch (default `1`)
- `config.ack_policy` — `all` (default) or `primary`
- `config.primary_destination` — destination name used when `ack_policy=primary`
- `config.failure_mode` — `hold_slot` (default) or `drop_slot`
- `config.give_up_policy` — `on_retry_exhaustion` (default) or `never`

Why fan‑out instead of multiple replication slots?
- One slot means WAL is decoded once, reducing CPU/I/O on the primary.
- Fewer slots reduces WAL retention risk and slot‑quota pressure.
- A single stream preserves ordering and shares DDL gating across destinations.
- `ack_policy=primary` lets the primary destination advance the slot while secondaries replay.

To reconfigure destinations or wire format, call `UpdateFlow` with a full `Flow` payload; state is preserved.

## Worker Mode (Per-Flow Process)
Run a single flow in its own process. This is recommended for Kubernetes or when you want isolated scaling per flow.

```bash
./bin/wallaby-worker -flow-id "<flow-id>"
```

Add `-max-empty-reads 1` to stop after one empty poll. This is useful for periodic/scheduled runs.
For backfill runs that land in staging tables, add `-resolve-staging` to apply staged data without relying on process shutdown.

## DBOS Scheduling
Enable DBOS and (optionally) a schedule to run flows as durable jobs:

```bash
export WALLABY_DBOS_ENABLED="true"
export WALLABY_DBOS_APP="wallaby"
export WALLABY_DBOS_QUEUE="wallaby"
export WALLABY_DBOS_SCHEDULE="*/10 * * * * *" # every 10 seconds
export WALLABY_DBOS_MAX_RETRIES="5" # workflow recovery retries (optional)
```

If `WALLABY_DBOS_SCHEDULE` is set, DBOS enqueues one run for each flow in `running` state.

## Kubernetes Job Dispatch
If the API server runs inside Kubernetes, it can launch per-flow workers as Jobs when you call `StartFlow` or `ResumeFlow`:

```bash
export WALLABY_K8S_ENABLED="true"
export WALLABY_K8S_JOB_IMAGE="ghcr.io/josephjohncox/wallaby:0.1.0"
export WALLABY_K8S_JOB_SERVICE_ACCOUNT="wallaby"
export WALLABY_K8S_JOB_ENV_FROM="secret:wallaby-secrets,configmap:wallaby-config"
```

Optional settings:
- `WALLABY_K8S_NAMESPACE` (defaults to the in-cluster namespace)
- `WALLABY_K8S_JOB_MAX_EMPTY_READS` (appends `-max-empty-reads` to workers)
- `WALLABY_K8S_JOB_ARGS` / `WALLABY_K8S_JOB_COMMAND` (comma-separated list)
- `WALLABY_K8S_JOB_LABELS` / `WALLABY_K8S_JOB_ANNOTATIONS` (`key=value` comma list)
- `WALLABY_K8S_KUBECONFIG` / `WALLABY_K8S_CONTEXT` for out-of-cluster kubeconfig usage
- `WALLABY_K8S_API_SERVER`, `WALLABY_K8S_TOKEN`, `WALLABY_K8S_CA_FILE`, `WALLABY_K8S_CA_DATA` for explicit API config
- `WALLABY_K8S_CLIENT_CERT`, `WALLABY_K8S_CLIENT_KEY` for mTLS auth
- `WALLABY_K8S_INSECURE_SKIP_TLS` to skip TLS verification (not recommended)

You can also trigger a one-off run without changing flow state via gRPC:

```bash
grpcurl -plaintext -d '{"flow_id":"<id>"}' localhost:8080 wallaby.v1.FlowService/RunFlowOnce
```

## Checkpoint Store
By default, checkpoints use Postgres when `WALLABY_POSTGRES_DSN` is set. For standalone runs, SQLite is supported:

```bash
export WALLABY_CHECKPOINT_BACKEND="sqlite"
export WALLABY_CHECKPOINT_PATH="$HOME/.wallaby/checkpoints.db"
```

Set `WALLABY_CHECKPOINT_DSN` to override the full SQLite DSN.

## Wire Formats
WALlaby supports multiple wire formats. Set the default at the service level or per-flow:

```bash
export WALLABY_WIRE_FORMAT="arrow"   # arrow | parquet | avro | proto | json
export WALLABY_WIRE_ENFORCE="true"
```

Per-flow overrides are supported via `flow.wire_format` or connector `options.format`.

## Kafka Destination
Kafka destination options (connector `options`):
- `brokers` (required) — comma-separated list
- `topic` (required)
- `format` (default `arrow`)
- `compression` (`none`, `gzip`, `snappy`, `lz4`, `zstd`)
- `acks` (`all` default, or `leader`, `none`)
- `max_message_bytes` (default `900000`) — upper bound for Kafka record batches
- `max_batch_bytes` (default = `max_message_bytes`) — size-aware split threshold for encoded batches
- `max_record_bytes` (default = `max_message_bytes`) — hard cap for single-record payloads
- `oversize_policy` (`error` default, or `drop`)

## S3 Destination
S3 destination options (connector `options`):
- `bucket` (required)
- `prefix` (optional)
- `endpoint` (optional, for MinIO/local S3)
- `access_key` / `secret_key` / `session_token` (optional)
- `force_path_style` (default `false`)
- `use_fips` / `use_dualstack` (optional; needed for GovCloud/regulated environments)
- `format` (default `json`)
- `compression` (`gzip` to compress objects)
- `partition_by` — comma-separated list of `column` or `column:day|hour|month|year`

Set `region` to GovCloud/China regions (e.g., `us-gov-west-1`, `cn-north-1`) to use the correct AWS partition.

Example partitioning:

```json
{
  "type": "s3",
  "options": {
    "bucket": "wallaby-data",
    "prefix": "cdc",
    "format": "parquet",
    "partition_by": "region,created_at:day"
  }
}
```

## HTTP / Webhook Destination
HTTP destination options (connector `options`):
- `url` (required)
- `method` (`POST` default)
- `format` (default `json`)
- `payload_mode` (`wire` default, or `record_json`/`raw`, `wal`)
- `timeout` (duration string, default `10s`)
- `headers` (comma-separated `Key:Value` list)
- `max_retries`, `backoff_base`, `backoff_max`, `backoff_factor`
- `idempotency_header` (default `Idempotency-Key`)

`payload_mode=record_json` (alias `raw`) sends a single-record JSON envelope (table, operation, key, before/after, etc.) and ignores `format`.
`payload_mode=wal` sends raw pgoutput bytes (requires a Postgres logical source).
The idempotency key is derived from `(table, key, lsn)` and hashed to a fixed string.

## gRPC Destination
gRPC destination options (connector `options`):
- `endpoint` (required, e.g. `host:port`)
- `format` (default `json`)
- `payload_mode` (`wire` default, or `record_json`/`raw`, `wal`)
- `insecure` (`true` default), `tls_ca_file`, `tls_server_name`
- `headers` (comma-separated `Key:Value` list)
- `timeout`, `max_retries`, `backoff_base`, `backoff_max`, `backoff_factor` (durations are strings like `200ms`, `5s`)
- `flow_id` (optional) — forwarded in the ingest request
- `destination` (optional) — logical destination name (defaults to the destination spec name)

The client calls `IngestService/IngestBatch` and sends `payload_mode` as gRPC metadata (`x-wallaby-payload-mode`).

## Type Mapping (Schema Translation)
Destinations that materialize tables (Snowflake, Snowpipe, ClickHouse, DuckDB) apply default Postgres → destination type mappings. Override per destination with:
- `type_mappings` — JSON or YAML map of `postgres_type` → `dest_type`
- `type_mappings_file` — path to a JSON or YAML map file

Example:

```json
{
  "timestamptz": "TIMESTAMP_TZ",
  "jsonb": "VARIANT"
}
```

YAML example:

```yaml
timestamptz: TIMESTAMP_TZ
jsonb: VARIANT
ext:postgis.geometry: STRING
ext:vector: ARRAY
```

## DuckLake Destination
DuckLake options (connector `options`):
- `dsn` (required) — DuckDB connection string
- `catalog` (required) — DuckLake metadata location (e.g. `metadata.ducklake`, `postgres:...`, `sqlite:...`)
- `catalog_name` (default `ducklake`)
- `data_path` (optional) — Parquet data root (local or object storage)
- `override_data_path` (default `false`)
- `install_extensions` (default `true`)

DuckLake uses DuckDB for execution and stores data as Parquet with a separate metadata catalog.

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
- `state_schema` (default `wallaby`)
- `state_table` (default `source_state`)
- `flow_id` (optional) — stable ID used in source-state records
- `capture_ddl` (default `false`) — installs an event trigger to emit raw DDL via logical messages
- `ddl_trigger_schema` (default `wallaby`) — schema for the DDL capture function
- `ddl_trigger_name` (default `wallaby_ddl_capture`) — event trigger name
- `ddl_message_prefix` (default `wallaby_ddl`) — logical message prefix to filter DDL events
- `toast_fetch` (`off` default, or `source`, `full`, `cache`) — how to rehydrate TOASTed/unchanged columns on UPDATE
- `toast_cache_size` (default `10000`) — LRU size used when `toast_fetch=cache`
- `aws_rds_iam` (default `false`) — enable RDS IAM auth (IRSA/role-based)
- `aws_region` (required when `aws_rds_iam=true` unless inferred from host)
- `aws_profile` (optional shared config profile)
- `aws_role_arn` / `aws_role_session_name` / `aws_role_external_id` (optional assume-role settings)
- `aws_endpoint` (optional AWS endpoint override)

RDS IAM uses the AWS SDK default credential chain (IRSA, env vars, shared config, or assume-role).

**TOAST rehydration**: Postgres may omit large unchanged columns on UPDATE. By default WALlaby emits partial updates plus `unchanged` fields. Use `toast_fetch=source` to reselect only those columns by primary key, `toast_fetch=full` to reselect the full row, or `toast_fetch=cache` for a best‑effort in‑memory merge.

## Publication Lifecycle
Use `sync_publication` with `publication_tables` or `publication_schemas` to add/drop tables when a flow starts. For ad-hoc changes, the admin CLI can update the publication:

```bash
./bin/wallaby-admin publication list -flow-id "<flow-id>"
./bin/wallaby-admin publication sync -flow-id "<flow-id>" -schemas public -mode add -pause -resume
```

For RDS IAM sources, pass `-aws-rds-iam` plus region/role flags (these override flow defaults).

To add tables and snapshot them:

```bash
./bin/wallaby-admin publication sync -flow-id "<flow-id>" -tables public.new_table -snapshot -pause -resume
```

## Postgres Stream Destination
The `pgstream` destination writes events into a Postgres-backed stream with consumer groups and visibility timeouts.

Destination options:
- `dsn` (required)
- `stream` (defaults to the destination name)
- `format` (default `json`)

Consumers can pull from the stream using the StreamService or the admin CLI:

```bash
./bin/wallaby-admin stream pull -stream orders -group search -max 10 -visibility 30
```

Ack messages when processed:

```bash
./bin/wallaby-admin stream ack -stream orders -group search -ids 1,2,3
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
- `meta_schema` (default `WALLABY_META`)
- `meta_table` (default `__METADATA`)
- `meta_pk_prefix` (default `pk_`)

Snowflake options are passed through the DSN. For GovCloud or private endpoints, use the appropriate account/host in the DSN; WALlaby does not rewrite hosts.

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

Snowpipe only supports `write_mode=append`; use metadata columns to preserve operation semantics.

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
- `type_mappings` — JSON or YAML map of source type → destination type
- `type_mappings_file` — path to a JSON or YAML map

Example:

```json
{"public.geometry": "GEOGRAPHY", "jsonb": "VARIANT"}
```

YAML example:

```yaml
ext:postgis.geometry: GEOGRAPHY
jsonb: VARIANT
```

## DDL Governance
DDL approval is configured per flow. When unset, the default is accept/apply (gate=false, auto_approve=true, auto_apply=true). Environment variables act as global defaults.

Flow config example:

```json
{
  "config": {
    "ddl": {
      "gate": true,
      "auto_approve": false,
      "auto_apply": false
    }
  }
}
```

Environment defaults (used when not set on the flow):

```bash
export WALLABY_DDL_GATE="false"
export WALLABY_DDL_AUTO_APPROVE="true"
export WALLABY_DDL_AUTO_APPLY="true"
```

Use the admin CLI to list and approve DDL events:

```bash
./bin/wallaby-admin ddl list -status pending [-flow-id <flow-id>]
./bin/wallaby-admin ddl approve -id 1
./bin/wallaby-admin ddl apply -id 1
```

When a DDL gate blocks a flow, WALlaby emits an OpenTelemetry event (`ddl.gated`)
and a trace event (`ddl_gate`). It also increments the `wallaby.ddl.gated_total` metric.

Log-to-alert example (Loki-style):

```
{service="wallaby"} |= "ddl gate:"
```

Metric alert example:

```
rate(wallaby_ddl_gated_total[5m]) > 0
```

Note: exporters may sanitize metric names (e.g., `wallaby_ddl_gated_total` in Prometheus).

## Resolve Staging Tables (Admin)
If backfill loads landed in staging tables, resolve them without running a flow:

```bash
./bin/wallaby-admin flow resolve-staging -flow-id "<flow-id>" -tables public.users,public.orders
```

Use `-schemas` to resolve all tables in schemas, and `-dest` to scope to a single destination.

## Backfill + Replay
Run a backfill by switching the worker to `backfill` mode and providing tables:

```bash
./bin/wallaby-worker -flow-id \"<flow-id>\" -mode backfill -tables public.users,public.orders
```

Backfill performance options (source `options`):
- `snapshot_workers` (default `1`) — parallel table/partition workers
- `parallel_tables` (alias of `snapshot_workers`)
- `partition_column` (optional) — column used to hash-partition a table
- `partition_count` (default `1`) — number of partitions per table
- `snapshot_consistent` (default `true`) — uses `pg_export_snapshot()` for a consistent snapshot
- `snapshot_state_backend` (`postgres` default, or `file`, `none`)
- `snapshot_state_schema` (default `wallaby`)
- `snapshot_state_table` (default `snapshot_state`)
- `snapshot_state_path` (required for `file` backend)

Example with parallel workers and hash partitions:

```bash
./bin/wallaby-worker -flow-id \"<flow-id>\" -mode backfill -tables public.users -snapshot-workers 4 -partition-column id -partition-count 8
```

Replay from a specific LSN (if your replication slot retains WAL):

```bash
./bin/wallaby-worker -flow-id \"<flow-id>\" -start-lsn \"0/16B6C50\"
```

For Postgres stream consumers, use the admin CLI to reset deliveries:

```bash
./bin/wallaby-admin stream replay -stream orders -group search -since 2025-01-01T00:00:00Z
```

## Checkpointing
Checkpoints are stored in Postgres and are used to resume streams from the last confirmed LSN. If a checkpoint exists, the runner sets `start_lsn` on the source before opening the replication stream.

## Testing (Developer)
Run the unit test suite:

```bash
make test
```

Run property tests with Rapid (default in CI uses `RAPID_CHECKS=100`):

```bash
make test-rapid RAPID_CHECKS=100
```
