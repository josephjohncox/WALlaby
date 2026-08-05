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

## Endpoint options

- `dsn` (required)
- `stage` and `stage_path`
- `format` (`parquet`, `avro`, or `json`)
- `file_format`
- `copy_on_write`
- `copy_pattern`
- `copy_on_error`
- `copy_purge`
- `copy_match_by_column_name`
- `auto_ingest`
- `warehouse`, `warehouse_size`, `warehouse_auto_suspend`, and `warehouse_auto_resume`
- metadata and schema-registry options

COPY string values are escaped and emitted in deterministic option order. The generated file name is bound into `FILES = (...)`, so COPY addresses the exact uploaded object rather than a broad stage scan.
