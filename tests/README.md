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

The integration script sanitizes credential helpers (AWS/Kubeconfig/Bitwarden) to avoid calling external tools during tests.
You can override the per-package test timeout with `GO_TEST_TIMEOUT` (default: 8m).

Run the Kubernetes dispatcher integration test using kind (no kubeconfig required):

```bash
make test-k8s-kind
```

Optional env vars for kind:
- `KIND_CLUSTER` (cluster name)
- `KIND_NODE_IMAGE` (override node image)
- `KIND_KEEP=1` (leave cluster running)

CLI integration tests run `go run ./cmd/wallaby-admin` against a local gRPC server and cover DDL listing, stream pull/ack, flow create/run-once, and publication sync. They use `TEST_PG_DSN` for backing Postgres storage.

Set these environment variables to enable destination tests:
- `TEST_PG_DSN` (Postgres logical replication E2E)
- `WALLABY_TEST_DBOS_DSN` (DBOS integration; falls back to `TEST_PG_DSN`)
  - DBOS backfill + streaming integration tests always run when a DBOS DSN is available.
- `WALLABY_TEST_K8S_KUBECONFIG` (Kubernetes dispatcher integration; avoids using global kubeconfig)
- `WALLABY_TEST_K8S_KIND=1` (spin up a local kind cluster and set `WALLABY_TEST_K8S_KUBECONFIG`)
- `WALLABY_TEST_K8S_NAMESPACE` (optional)
- `WALLABY_TEST_K8S_IMAGE` (optional job image)
- `WALLABY_TEST_CLICKHOUSE_DSN`, optional `WALLABY_TEST_CLICKHOUSE_DB`
- `TEST_CLICKHOUSE_HTTP_PORT` (optional clickhouse HTTP port for readiness checks)
- `WALLABY_TEST_FAKESNOW_HOST`, `WALLABY_TEST_FAKESNOW_PORT` (fakesnow Snowflake emulator)
- `WALLABY_TEST_FORCE_FAKESNOW=1` (prefer fakesnow even if Snowflake DSN is set)
- `WALLABY_TEST_RUN_FAKESNOW=1` (opt in to run Snowflake integration/benchmarks against fakesnow)
- `WALLABY_TEST_CLI_LOG=1` (print wallaby-admin output during CLI integration tests)
- `WALLABY_TEST_S3_ENDPOINT`, `WALLABY_TEST_S3_BUCKET`, `WALLABY_TEST_S3_ACCESS_KEY`, `WALLABY_TEST_S3_SECRET_KEY`, optional `WALLABY_TEST_S3_REGION`
- `WALLABY_TEST_DUCKDB_DSN`
- `WALLABY_TEST_DUCKLAKE=1` (enabled by default; requires ducklake extension)
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
