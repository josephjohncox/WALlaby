# Tests

Integration tests live here and exercise logical replication, schema evolution, checkpoints, and destination writes.

Set these environment variables to enable destination tests:
- `DUCTSTREAM_TEST_CLICKHOUSE_DSN`, optional `DUCTSTREAM_TEST_CLICKHOUSE_DB`
- `DUCTSTREAM_TEST_DUCKDB_DSN`
- `DUCTSTREAM_TEST_SNOWFLAKE_DSN`, optional `DUCTSTREAM_TEST_SNOWFLAKE_SCHEMA`
- `DUCTSTREAM_TEST_SNOWPIPE_DSN`, `DUCTSTREAM_TEST_SNOWPIPE_STAGE`

Benchmarks (ClickHouse mutation vs append):

```bash
go test ./tests -bench ClickHouse -run '^$'
```

Snowflake benchmarks (requires env vars):

```bash
go test ./tests -bench Snowflake -run '^$'
```

DuckDB benchmarks:

```bash
go test ./tests -bench DuckDB -run '^$'
```

Per-destination batch size benchmarks:

```bash
go test ./tests -bench BatchSizes -run '^$'
```

Wire format encode benchmarks:

```bash
go test ./pkg/wire -bench Codec -run '^$'
```

Transform metadata benchmark:

```bash
go test ./pkg/stream -bench Transform -run '^$'
```

Stream throughput benchmark:

```bash
go test ./pkg/stream -bench StreamThroughput -run '^$'
```
