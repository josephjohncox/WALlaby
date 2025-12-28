# Tests

Integration tests live here and exercise logical replication, schema evolution, checkpoints, and destination writes.

Run the Postgres E2E test locally (starts a Postgres container with logical replication):

```bash
make test-e2e
```

Run the full integration suite with local containers (Postgres + ClickHouse):

```bash
make test-integration-ci
```

Set these environment variables to enable destination tests:
- `TEST_PG_DSN` (Postgres logical replication E2E)
- `WALLABY_TEST_DBOS_DSN` (DBOS integration; falls back to `TEST_PG_DSN`)
- `WALLABY_TEST_K8S_KUBECONFIG` (Kubernetes dispatcher integration)
- `WALLABY_TEST_K8S_NAMESPACE` (optional)
- `WALLABY_TEST_K8S_IMAGE` (optional job image)
- `WALLABY_TEST_CLICKHOUSE_DSN`, optional `WALLABY_TEST_CLICKHOUSE_DB`
- `WALLABY_TEST_DUCKDB_DSN`
- `WALLABY_TEST_SNOWFLAKE_DSN`, optional `WALLABY_TEST_SNOWFLAKE_SCHEMA`
- `WALLABY_TEST_SNOWPIPE_DSN`, `WALLABY_TEST_SNOWPIPE_STAGE`

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

Stream harness benchmark (parallelism/record width):

```bash
go test ./pkg/stream -bench StreamHarness -run '^$'
```

Export benchmark results (JSON/CSV):

```bash
go test ./pkg/stream -bench StreamHarness -run '^$' | go run ./tools/benchreport -format json > bench.json
go test ./pkg/stream -bench StreamHarness -run '^$' | go run ./tools/benchreport -format csv > bench.csv
```
