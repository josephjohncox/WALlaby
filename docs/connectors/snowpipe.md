# Snowpipe

Snowpipe is an experimental append-only staged destination. Destination-scoped mappings select and rename logical tables and columns; Snowpipe does not admit upsert or watermark-guarded mutation policies.

## Staged delivery

For each nonempty projected batch, WALlaby:

1. encodes the mapped append batch as Parquet, Avro, or JSON;
2. writes a temporary file;
3. executes `PUT` to the configured stage and path;
4. optionally executes `COPY INTO` with the exact staged file identity and configured COPY options; and
5. records destination metadata only after the staged operations succeed.

`auto_ingest=true` disables `COPY INTO` and leaves ingestion to the external notification and pipe configuration. WALlaby can prove the upload result, but it does not reinterpret an unobserved external pipe result.

## Failure contract

A PUT error remains a PUT error. A COPY error after a confirmed PUT remains a COPY error with the staged object left under Snowflake's normal stage semantics. A metadata-receipt error after COPY remains that receipt failure. Target tables change only through configured COPY or external pipe ingestion; an error does not select another write mechanism.

Credential-free tests use an unexported package-local staged transport. It is unavailable to production configuration. The external integration test requires `WALLABY_TEST_SNOWPIPE_DSN` and `WALLABY_TEST_SNOWPIPE_STAGE` for a real Snowflake service.

## Typed `snowpipe` fields

- `dsn` (required)
- `stage` and `stage_path`
- `format` (`WIRE_FORMAT_PARQUET`, `WIRE_FORMAT_AVRO`, or `WIRE_FORMAT_JSON`)
- `file_format`
- native booleans `copy_on_write`, `copy_purge`, and `auto_ingest`
- `copy_pattern`, `copy_on_error`, and `copy_match_by_column_name`
- nested `warehouse.name`, `warehouse.size`, `warehouse.auto_suspend_seconds`, `warehouse.auto_resume`, and `warehouse.session_keep_alive`
- nested `metadata` and `schema_registry` branches; a local registry requires `schema_registry.local.directory`

COPY string values are escaped and emitted in deterministic option order. The generated file name is bound into `FILES = (...)`, so COPY addresses the exact uploaded object rather than a broad stage scan.
