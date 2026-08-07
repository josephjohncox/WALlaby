# Extended connector notes

This page records typed endpoint fields for selected non-core destination adapters. It is not a complete connector catalog. Start with the [connector overview](connectors/index.md) and the [PostgreSQL connector reference](connectors/postgres.md).

Full JSON flow definitions live under `examples/flows/`.

## Kafka

WALlaby writes wire-formatted payloads and includes metadata headers:

- `wallaby-format` (arrow|avro|proto|json)
- `wallaby-schema`
- `wallaby-namespace`
- `wallaby-schema-version`
- `wallaby-registry-subject`, `wallaby-registry-id`, `wallaby-registry-version` (when schema registry enabled)

Kafka typed fields:

- `brokers` (required native string list)
- `topic` (required)
- `format` (`WIRE_FORMAT_*` enum)
- `compression` (`COMPRESSION_*` enum)
- `acks` (`KAFKA_ACKS_*` enum)
- `max_message_bytes`, `max_batch_bytes`, `max_record_bytes`
- `allow_oversize_skip` (`false` default; `true` drops oversize payloads and declares lossy delivery)
- `message_mode` (`KAFKA_MESSAGE_MODE_BATCH` or `KAFKA_MESSAGE_MODE_RECORD`)
- `key_mode` (`KAFKA_KEY_MODE_HASH` or `KAFKA_KEY_MODE_RAW`)
- `transactional_producer` (`false` default; `true` enables transactions and requires `transactional_id`)
- `transactional_id` (required only when `transactional_producer=true`)
- `transaction_timeout`
- `transaction_header` (default `wallaby-transaction-id`)
- `schema_registry` — nested message with exactly one of `confluent`, `apicurio`, `glue`, `postgres`, or `local`; backend credentials and timeout live inside that branch. The PostgreSQL backend exposes only `postgres.connection.dsn` and `postgres.timeout`; the durable local backend requires `local.directory`.
- `schema_registry_subject`, `schema_registry_subject_mode`, and `schema_registry_proto_types_subject` are destination-scoped sibling fields on `kafka`; there are no flow-level registry-subject defaults.

Payload format notes:

- `arrow`/`avro`/`proto` use the shared schema from the flow; schema evolution is driven by DDL events.
- JSON is supported for compatibility but loses some typing fidelity; prefer Arrow/Avro/Proto for strict round-trip.

## Snowflake

Read the [Snowflake destination reference](connectors/snowflake.md) before selecting a mode. WALlaby implements three experimental modeled protocol profiles: `postgresql-to-snowflake-sql-v1` uses pre-provisioned hybrid tables and receipt-first target transactions; `postgresql-to-snowflake-staged-append-v1` loads a deterministic immutable internal-stage object into an append changelog table with fail-closed COPY and load-history reconciliation; and `postgresql-to-snowflake-streaming-rest-append-v1` models SQL-observed append completeness but refuses admission because no reviewed append transport is linked. SQL and staged COPY have no reviewed Snowflake service version or deployment cell with complete same-SHA live evidence. Streaming has that promotion gap plus the missing transport. The generic direct-table mode below is also experimental and does not inherit the named profiles' reconciliation contracts.

Generic Snowflake is an append-only mapping destination. The named managed SQL profile is the only Snowflake configuration that advertises explicit-key current-state upsert, and its mapping keys must equal the complete ordered source primary key.

Generic Snowflake typed fields:

- `dsn` (required)
- `disable_transactions` (optional native boolean; emulator-only)
- `warehouse.name`, `warehouse.size`, `warehouse.auto_suspend_seconds`, `warehouse.auto_resume`, and `warehouse.session_keep_alive`
- nested `staging`, `metadata`, `type_mappings`, and `schema_registry` messages

Managed profile messages are separate typed branches. They do not expose generic warehouse, staging, metadata, COPY, or type-mapping fields that the profile forbids or ignores.

Cost tips:

- Set `warehouse.size=xsmall` and `warehouse.auto_suspend_seconds=60` to reduce idle burn.
- Keep `warehouse.session_keep_alive=false` so sessions do not pin warehouses.

## Snowpipe

Snowpipe is an append-only file-based sink. WALlaby writes files to the configured stage and can optionally issue COPY statements. PUT, COPY, and metadata-receipt errors are returned unchanged; target tables change only through configured COPY or external pipe ingestion.

Typed `snowpipe` fields:

- `dsn` (required)
- `stage` (required) — e.g., `@my_external_stage`
- `format` (`WIRE_FORMAT_PARQUET` recommended)
- `auto_ingest` (native boolean; `true` skips COPY and relies on external notifications)
- `copy_on_write` (native boolean; set `false` with `auto_ingest=true`)
- `copy_pattern` (Snowflake COPY PATTERN)
- `copy_on_error` (Snowflake COPY ON_ERROR)
- `copy_purge` (native boolean; `true` removes staged files after COPY)
- `copy_match_by_column_name` (`case_sensitive|case_insensitive`)
- `file_format` (Snowflake named file format override)
- nested `warehouse` (optional, used for COPY cost management), `metadata`, `type_mappings`, and `schema_registry` messages

Auto-ingest mode:

- Set `auto_ingest=true` to upload only.
- You must configure an external stage + notification integration in Snowflake.
- WALlaby will not issue COPY in this mode.
- A failed upload remains a failed upload; WALlaby does not substitute another write mechanism for external notification or pipe behavior.

## DuckLake

DuckLake uses DuckDB with the DuckLake extension. WALlaby attaches a DuckLake catalog and writes tables through DuckDB.

Typed `ducklake` fields:

- `dsn` (required) — DuckDB connection string
- `catalog` (required) — DuckLake catalog path
- `catalog_name` (default `ducklake`)
- `data_path` (optional) — override catalog data path
- `install_extensions` (default `true`) — disable if extensions are preinstalled/locked down

DuckLake is append-only at the mapping boundary. Logical target names and event semantics come from `config.table_mappings`.

Caveats:

- DuckLake metadata is file-based. Avoid concurrent writers to the same catalog unless you coordinate externally.
- For production, pin DuckDB/DuckLake versions and ensure the extension is available in your runtime.

## HTTP / Webhook

HTTP delivery supports retries + exponential backoff and idempotency headers.

- `payload_mode=PAYLOAD_MODE_RECORD_JSON` sends one-record JSON envelopes.
- `payload_mode=PAYLOAD_MODE_WAL` sends raw pgoutput bytes.
- The idempotency key includes table, operation, per-record source position, key, and encoded payload.
- `transaction_header` (default `X-Wallaby-Transaction-Id`) carries the LSN or a hash fallback.
- `dedupe_window` is process-local and remembers only confirmed sends; failures and cancellations remain retryable.
- When using `payload_mode=PAYLOAD_MODE_WIRE` with Avro/Proto and a nested `schema_registry`, WALlaby emits
  `X-Wallaby-Registry-*` headers.

## S3

S3 supports Parquet/Arrow/Avro/JSON with optional partitioning. Use `region` values that match your AWS partition (GovCloud/China supported).
Exact in-memory batch retries converge through deterministic keys, conditional creation, and SHA-256 reconciliation; conflicting content at one identity fails closed. Direct S3 remains experimental and at least once because crash-time rebatching can change the terminal checkpoint identity.
If `schema_registry` is enabled with Avro/Proto, WALlaby writes registry metadata into object metadata keys
(`wallaby-registry-*`).
