# Connector Configuration Notes

This page highlights connector-specific configuration and caveats that matter in production. Refer to `examples/flows/` for full JSON specs.

## Kafka
WALlaby writes wire-formatted payloads and includes metadata headers:
- `wallaby-format` (arrow|avro|proto|json)
- `wallaby-schema`
- `wallaby-namespace`
- `wallaby-schema-version`
- `wallaby-registry-subject`, `wallaby-registry-id`, `wallaby-registry-version` (when schema registry enabled)

Kafka options:
- `brokers` (required)
- `topic` (required)
- `format` (default `arrow`)
- `compression` (`none`, `gzip`, `snappy`, `lz4`, `zstd`)
- `acks` (`all`, `leader`, `none`)
- `max_message_bytes`, `max_batch_bytes`, `max_record_bytes`
- `oversize_policy` (`error` or `drop`)
- `message_mode` (`batch` or `record`)
- `key_mode` (`hash` or `raw`)
- `transactional_id` (enables transactions per batch)
- `transaction_timeout`
- `transaction_header` (default `wallaby-transaction-id`)
- `schema_registry` (`csr`, `apicurio`, `glue`, `postgres`, `local`, `none`)
- `schema_registry_url` (CSR/Apicurio base URL; defaults to CSR when set)
- `schema_registry_username`, `schema_registry_password`, `schema_registry_token`
- `schema_registry_subject` (override subject)
- `schema_registry_subject_mode` (`topic`, `table`, `topic_table`; default `topic_table`)
- `schema_registry_proto_types_subject` (override Proto dependency subject)
- `schema_registry_dsn` (postgres registry)
- `schema_registry_timeout`
- `schema_registry_region`, `schema_registry_endpoint`, `schema_registry_profile`, `schema_registry_role_arn`,
  `schema_registry_glue_registry`, `schema_registry_glue_schema` (Glue registry)

Flow defaults:
- `config.schema_registry_subject`, `config.schema_registry_proto_types_subject`, and
  `config.schema_registry_subject_mode` can be set on the flow config. Endpoint options override flow defaults.

Payload format notes:
- `arrow`/`avro`/`proto` use the shared schema from the flow; schema evolution is driven by DDL events.
- JSON is supported for compatibility but loses some typing fidelity; prefer Arrow/Avro/Proto for strict round-trip.

## Snowflake
Snowflake is a direct table sink (UPSERT/DELETE in streaming mode). You can manage warehouse costs per destination.

Options:
- `dsn` (required)
- `schema`, `table`
- `write_mode` (`target` or `append`)
- `batch_mode` (`target` or `staging`) with `batch_resolution` (`none`, `append`, `replace`)
- `disable_transactions` (useful for emulators)
- `warehouse` (optional, used for cost management)
- `warehouse_size` (e.g., `xsmall`, `small`, `medium`)
- `warehouse_auto_suspend` (seconds; default 60 when `warehouse` set)
- `warehouse_auto_resume` (`true|false`, default `true` when `warehouse` set)
- `session_keep_alive` (`true|false`, default `false`)

Cost tips:
- Set `warehouse_size=xsmall` and `warehouse_auto_suspend=60` to reduce idle burn.
- Keep `session_keep_alive=false` so sessions don’t pin warehouses.

## Snowpipe
Snowpipe is a file-based sink. WALlaby writes files and can optionally issue COPY statements.

Options:
- `dsn` (required)
- `stage` (required) — e.g., `@my_external_stage`
- `format` (`parquet` recommended)
- `compat_mode` (`fakesnow` to enable PUT/COPY fallback inserts for emulator tests)
- `auto_ingest` (`true` to **skip COPY** and rely on external notifications)
- `copy_on_write` (`true` to run COPY immediately; set `false` with `auto_ingest=true`)
- `copy_pattern` (Snowflake COPY PATTERN)
- `copy_on_error` (Snowflake COPY ON_ERROR)
- `copy_purge` (`true|false` to remove staged files after COPY)
- `copy_match_by_column_name` (`case_sensitive|case_insensitive`)
- `file_format` (Snowflake named file format override)
- `warehouse` (optional, used for COPY cost management)
- `warehouse_size`, `warehouse_auto_suspend`, `warehouse_auto_resume`, `session_keep_alive` (same semantics as Snowflake)

Auto-ingest mode:
- Set `auto_ingest=true` to upload only.
- You must configure an external stage + notification integration in Snowflake.
- WALlaby will not issue COPY in this mode.

## DuckLake
DuckLake uses DuckDB with the DuckLake extension. WALlaby attaches a DuckLake catalog and writes tables through DuckDB.

Options:
- `dsn` (required) — DuckDB connection string
- `catalog` (required) — DuckLake catalog path
- `catalog_name` (default `ducklake`)
- `data_path` (optional) — override catalog data path
- `install_extensions` (default `true`) — disable if extensions are preinstalled/locked down
- `write_mode` (`target` default, or `append`)
- `batch_mode` (`target` or `staging`) with `batch_resolution` (`none`, `append`, `replace`)

Caveats:
- DuckLake metadata is file-based. Avoid concurrent writers to the same catalog unless you coordinate externally.
- For production, pin DuckDB/DuckLake versions and ensure the extension is available in your runtime.

## HTTP / Webhook
HTTP delivery supports retries + exponential backoff and idempotency headers.
- `payload_mode=record_json` sends one-record JSON envelopes.
- `payload_mode=wal` sends raw pgoutput bytes.
- Idempotency key is derived from `(table, key, lsn)`.
- `transaction_header` (default `X-Wallaby-Transaction-Id`) carries the LSN or a hash fallback.
- `dedupe_window` (duration) skips duplicate idempotency keys within a window.
- When using `payload_mode=wire` with Avro/Proto and `schema_registry` enabled, WALlaby emits
  `X-Wallaby-Registry-*` headers.

## S3
S3 supports Parquet/Arrow/Avro/JSON with optional partitioning. Use `region` values that match your AWS partition (GovCloud/China supported).
If `schema_registry` is enabled with Avro/Proto, WALlaby writes registry metadata into object metadata keys
(`wallaby-registry-*`).
