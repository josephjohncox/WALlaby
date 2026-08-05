# Configuration and command reference

Use this page to look up configuration keys and command syntax. For a runnable first flow, use the [quickstart](getting-started/quickstart.md). For runtime selection, use [choose a runtime](deployment/index.md).

## Prerequisites

- PostgreSQL with logical replication enabled (`wal_level=logical`).
- A named replication slot and publication. WALlaby can create them when its database role has permission.
- Network access from each worker to its source, destinations, workflow store, and checkpoint store.

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
- `config.table_mappings` — mandatory expanded destination-scoped selection, naming, and write contract; see [Manage flows and table mappings](guides/flows.md)
- `config.ack_policy` — `all` (default), `primary`, or experimental `materialized`
- `config.primary_destination` — destination name used when `ack_policy=primary`
- `config.materialization.projection_id` — must be `canonical_cdc_parquet_v2` for current mapped Iceberg materialization (`canonical_cdc_parquet_v1` remains frozen for historical artifacts)
- `config.failure_mode` — `hold_slot` (default) or `drop_slot`
- `config.give_up_policy` — `on_retry_exhaustion` (default) or `never`
- `config.schema_registry_subject` — default registry subject for destinations (overridden by endpoint options)
- `config.schema_registry_proto_types_subject` — default subject for Proto dependency schemas
- `config.schema_registry_subject_mode` — Kafka subject mode (`topic`, `table`, `topic_table`)

Why fan‑out instead of multiple replication slots?

- One slot means WAL is decoded once, reducing CPU/I/O on the primary.
- Fewer slots reduces WAL retention risk and slot‑quota pressure.
- A single stream preserves ordering and shares DDL gating across destinations.
- `ack_policy=primary` atomically stores the checkpoint and durable per-secondary outbox entries before advancing the slot. It requires a SQLite or PostgreSQL checkpoint store, stable destination names, and idempotent writes at every destination.
- `ack_policy=materialized` is limited to managed PostgreSQL CDC with exactly one Iceberg destination revision. It acknowledges each CDC transaction only after immutable canonical objects and the fenced PostgreSQL publication/checkpoint commit. A data-free startup cut receives an object-free canonical publication before feedback. The production worker registers the Iceberg consumer behind that authority and commits it asynchronously; catalog or Snowflake visibility does not extend the source-ACK boundary. This policy is not `all` and makes no exactly-once claim. See [Canonical artifact log](concepts/artifact-log.md) and [S3 Tables to Snowflake](guides/s3-tables-snowflake.md).

Use `wallaby-admin flow mappings generate` with an explicit table, schema, or publication scope to build deterministic mappings from the PostgreSQL catalog. The CLI preserves ordered source-PK defaults only for destinations/profiles that admit explicit-key upsert; append-only destinations generate append. Repeatable `--write-mode schema.table=append|upsert` overrides can select append or request a capability- and key-checked upsert. The exact managed Snowflake SQL profile requires one selected primary-key relation, emits the complete ordered-PK upsert with future tables excluded, and rejects append, watermark, or altered match-key overrides. Review and validate the expanded flow before create. Unmanaged identity changes use `flow reconfigure`; unmanaged name or parallelism changes may use `flow update`. Managed flows reject both RPCs for every change. Stop the old managed flow, create/validate/start a distinct flow with new flow and revision identities, cut over, and delete the old flow only when safe. Terraform cannot execute this managed cutover and every managed update fails apply while retaining state.
Additional CLI operations:

- `wallaby-admin flow list [--state created|running|paused|stopping|stopped|failed]` to inspect all flows.
- `wallaby-admin flow get --flow-id <id>` to print flow definition and state.
- `wallaby-admin flow wait --flow-id <id> --state <state>` for automation/scripting.
- `wallaby-admin flow dry-run --file <path>` to normalize/inspect flow config without applying.
- `wallaby-admin flow check --file <path> [--endpoint <addr>]` for config pre-flight checks.
- `wallaby-admin flow delete --flow-id <id>` to remove a flow.
- `wallaby-admin flow validate --file <path>` to validate config before create/update.
- `wallaby-admin ddl show --id <event_id> [--status ...]` to inspect a single DDL event.
When decommissioning a flow, `wallaby-admin flow cleanup --flow-id <id>` defaults to `--drop-slot=true`,
`--drop-publication=false`, and `--drop-source-state=true`. Retain the slot or source-state row with
`--drop-slot=false` or `--drop-source-state=false`; remove the publication with `--drop-publication=true`.
- `wallaby-admin slot list --flow-id <id> [--slot <name>]` to inspect logical replication slot state.
- `wallaby-admin slot show --flow-id <id> [--slot <name>]` to print the state for one slot.
- `wallaby-admin slot drop --flow-id <id> [--slot <name>] [--if-exists]` to drop a logical replication slot.

## Worker Mode (Per-Flow Process)

Run a single flow in its own process. This is recommended for Kubernetes or when you want isolated scaling per flow.

```bash
./bin/wallaby-worker --flow-id "<flow-id>"
```

Add `--max-empty-reads 1` to stop after one empty poll. This is useful for periodic/scheduled runs.
For backfill runs that land in staging tables, add `--resolve-staging` to apply staged data without relying on process shutdown.

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
- `WALLABY_K8S_JOB_MAX_EMPTY_READS` (appends `--max-empty-reads` to workers)
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

By default, checkpoints use Postgres when `WALLABY_POSTGRES_DSN` is set. A local worker can use SQLite for checkpoints, but the production control plane still requires the PostgreSQL workflow store:

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

For connector-specific caveats (Snowpipe auto-ingest, DuckLake, Kafka payloads), see `docs/connectors.md`.

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
- `allow_oversize_skip` (`false` default; `true` drops oversize payloads and declares lossy delivery)
- `message_mode` (`batch` default, or `record`)
- `key_mode` (`hash` default, or `raw` to use the record key directly)
- `transactional_producer` (`false` default; `true` enables Kafka transactions per batch and requires `transactional_id`)
- `transactional_id` (required when `transactional_producer=true`; rejected otherwise)
- `transaction_timeout` (optional, e.g. `30s`)
- `transaction_header` (defaults to `wallaby-transaction-id`)
- `schema_registry` (`csr`, `apicurio`, `glue`, `postgres`, `local`, `none`)
- `schema_registry_url` (CSR/Apicurio)
- `schema_registry_subject` / `schema_registry_subject_mode`
- `schema_registry_username` / `schema_registry_password` / `schema_registry_token`
- `schema_registry_dsn` (postgres registry)

Kafka payload details and headers are documented in `docs/connectors.md`.

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

Direct writes use deterministic checkpoint-derived keys, conditional creation, and stored SHA-256 metadata. Repeating the same in-memory batch converges on the existing object; different content at the same identity fails closed. This remains an experimental at-least-once path because a crash can rebatch records under a different terminal checkpoint. Partition values use typed, reversible path encoding rather than human-readable lossy names.

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
- `transaction_header` (default `X-Wallaby-Transaction-Id`)
- `dedupe_window` (duration string, disables duplicates within the window)

`payload_mode=record_json` (alias `raw`) sends a single-record JSON envelope (table, operation, key, before/after, etc.) and ignores `format`.
`payload_mode=wal` sends raw pgoutput bytes (requires a Postgres logical source).
The idempotency key hashes the table, operation, per-record source position (with checkpoint fallback), key, and encoded payload. The dedupe window is process-local and records only confirmed sends: failed or cancelled requests release their reservation, while concurrent duplicates wait for the active request. Restart delivery remains at least once.

## Runbooks

For operational playbooks (DDL gating and recovery), see `docs/runbooks.md`.

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
./bin/wallaby-admin publication list --flow-id "<flow-id>"
./bin/wallaby-admin publication sync --flow-id "<flow-id>" --schemas public --mode add --pause --resume
```

For RDS IAM sources, pass `--aws-rds-iam` plus region/role flags (these override flow defaults).

To add tables and snapshot them:

```bash
./bin/wallaby-admin publication sync --flow-id "<flow-id>" --tables public.new_table --snapshot --pause --resume
```

## Postgres Stream Destination

The `pgstream` destination writes events into a Postgres-backed stream with consumer groups and visibility timeouts.

Destination options:

- `dsn` (required)
- `stream` (defaults to the destination name)
- `format` (default `json`)

Consumers can pull from the stream using the StreamService or the admin CLI:

```bash
./bin/wallaby-admin stream pull --stream orders --group search --max 10 --visibility 30
```

Ack messages when processed:

```bash
./bin/wallaby-admin stream ack --stream orders --group search --ids 1,2,3
```

## Snowflake destination

Generic Snowflake is an experimental append-only mapping destination. Configure its connection with `dsn`; put logical relation names, column names, selection, and append policy in the destination-scoped mapping. The exact `postgresql-to-snowflake-sql-v1` managed profile instead admits one current-state explicit-key upsert mapping whose ordered keys equal the complete source primary key. See the [Snowflake guide](connectors/snowflake.md).

## Snowpipe destination

Snowpipe is append-only. Configure `dsn`, `stage`, `stage_path`, file `format`, optional named `file_format`, COPY controls, and notification behavior on the endpoint. Mapping rules own logical targets and append metadata. PUT, COPY, and metadata-receipt errors are returned unchanged, and target tables change only through configured COPY or external pipe ingestion. See the [Snowpipe guide](connectors/snowpipe.md).

## DuckDB and DuckLake destinations

Both are append-only mapping destinations. DuckDB uses `dsn`. DuckLake additionally uses `catalog`, `catalog_name`, and `data_path`. They do not advertise upsert until dedicated mutation and recovery evidence exists.

## ClickHouse destination

Use the exact `postgresql-to-clickhouse-append-v1` profile for maintained append-only changelog delivery on its admitted PostgreSQL and ClickHouse/Keeper deployment. Generic ClickHouse remains experimental and append-only at the mapping boundary. See the [ClickHouse guide](connectors/clickhouse.md).

## Bufstream destination

Bufstream is Kafka-compatible; use the same transport options as Kafka. Its table mapping is append-only.

## Append metadata

Append mappings preserve every event and add `__wallaby_operation`, `__wallaby_deleted`, and `__wallaby_source_position`. A configured append watermark is projected metadata only. It never suppresses events or supplies row identity.

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
./bin/wallaby-admin ddl list --status pending [--flow-id <flow-id>]
./bin/wallaby-admin ddl approve --id 1
```

Only the data-plane runner can apply an approved event and mark it applied. It records a durable receipt for every DDL-executing destination and changes the event to `applied` after the complete immutable destination manifest has receipts. Operators use `ddl list`, `ddl show`, and `ddl approve`; after approval the running data-plane workflow performs the apply. There is no separate administrative apply subcommand.

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
./bin/wallaby-admin flow resolve-staging --flow-id "<flow-id>" --tables public.users,public.orders
```

Use `--schemas` to resolve all tables in schemas, and `--dest` to scope to a single destination.

## Backfill + Replay

Run a backfill by switching the worker to `backfill` mode and providing tables:

```bash
./bin/wallaby-worker --flow-id "<flow-id>" --mode backfill --tables public.users,public.orders
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

Snapshot checkpoints use the partition value plus the table primary key as a composite cursor. This bounds crash replay when many rows share one partition value and preserves deterministic `NULLS LAST` ordering. Tables without a primary key fall back to an inclusive partition cursor: recovery may replay every row equal to that value, but does not omit them.

Example with parallel workers and hash partitions:

```bash
./bin/wallaby-worker --flow-id "<flow-id>" --mode backfill --tables public.users --snapshot-workers 4 --partition-column id --partition-count 8
```

Replay from a specific LSN (if your replication slot retains WAL):

```bash
./bin/wallaby-worker --flow-id "<flow-id>" --start-lsn "0/16B6C50"
```

For Postgres stream consumers, use the admin CLI to reset deliveries:

```bash
./bin/wallaby-admin stream replay --stream orders --group search --since 2025-01-01T00:00:00Z
```

## Checkpointing

Checkpoints are stored in Postgres and are used to resume streams from the last confirmed LSN. If a checkpoint exists, the runner sets `start_lsn` on the source before opening the replication stream.

## Testing (Developer)

Run the unit test suite:

```bash
just test
```

Run property tests with Rapid (default in CI uses `RAPID_CHECKS=100`):

```bash
RAPID_CHECKS=100 just test-rapid
```
