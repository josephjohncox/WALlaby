# Benchmarks

This repository includes a reproducible, local benchmark harness for:

- Postgres → Kafka (Redpanda)
- Postgres → Postgres (UPSERT/DELETE apply)
- Postgres → ClickHouse

## Quick Start

```bash
make bench PROFILE=small TARGETS=all
```

To run the full benchmark suite (small/medium/large) and generate a summary:

```bash
make benchmark
```

To capture CPU/heap profiles and Go execution traces (with per-target runs), use:

```bash
make benchmark-profile
```

You can override the suite with env vars:

```bash
PROFILES=small,medium SCENARIOS=base,ddl TARGETS=postgres make benchmark
```

Profiling outputs:

- `bench/results/run_<timestamp>/cpu_<profile>_<scenario>_<target>.pprof`
- `bench/results/run_<timestamp>/heap_<profile>_<scenario>_<target>.pprof`
- `bench/results/run_<timestamp>/trace_<profile>_<scenario>_<target>.out`
- `bench/results/run_<timestamp>/cpu_<profile>_<scenario>_<target>.svg` (flamegraph by default)
- `bench/results/run_<timestamp>/cpu_<profile>_<scenario>_<target>.top.txt`

If flamegraphs are missing, install a pprof binary (`go install github.com/google/pprof@latest`) or ensure your Go toolchain includes `go tool pprof`.

You can switch formats with `PROFILE_FORMAT=flamegraph|svg|both|none`, disable rendering with `PROFILE_RENDER=0`, or skip the top report with `PROFILE_TOP=0`.

That command:

1. Starts the local dependencies via `bench/docker-compose.yml`.
2. Seeds three source tables in `bench_src` (narrow, wide, json_heavy).
3. Runs a workload with a 70/20/10 update/insert/delete mix.
4. Writes results to `bench/results/` in JSON + CSV.

Default table sizes:

- `narrow`: ~1 KB rows
- `wide`: ~5 KB rows
- `json_heavy`: ~20 KB rows with JSON arrays/objects

## Profiles

Profiles control scale and concurrency:

- `small`: 10k seed rows, 50k ops, 4 writers
- `medium`: 50k seed rows, 250k ops, 8 writers
- `large`: 200k seed rows, 1M ops, 16 writers

Example:

```bash
make bench PROFILE=medium TARGETS=postgres,clickhouse
```

## Targets

Run all targets or a subset:

```bash
make bench TARGETS=all
make bench TARGETS=postgres
make bench TARGETS=kafka,clickhouse
```

Note: the Kafka target uses a smaller batch size (clamped to 25) plus LZ4 compression to avoid hitting default broker message-size limits.

## DDL Churn (Separate Scenario)

DDL churn is available as a separate scenario (adds/drops a column and changes a type during the run):

```bash
make bench-ddl PROFILE=small TARGETS=postgres
```

## Environment Overrides

You can override connection settings via env vars:

- `BENCH_PG_DSN` (default: `postgres://postgres:postgres@localhost:5432/wallaby?sslmode=disable`)
- `BENCH_CLICKHOUSE_DSN` (default: `clickhouse://bench:bench@localhost:9000/bench`)
- `BENCH_KAFKA_BROKERS` (default: `localhost:9092`)
- `BENCH_CLICKHOUSE_WRITE_MODE` (default: `append`) — use `target` to benchmark mutation-based upserts
- `RESET_VOLUMES` (default: `1`, set to `0` to preserve Docker volumes)

Example:

```bash
BENCH_PG_DSN=postgres://user:pass@localhost:5432/db?sslmode=disable make bench
```

## Results

Results are written to `bench/results/` as JSON and CSV with:

- throughput (records/sec, MB/sec)
- end-to-end latency (p50/p95/p99)
- duration
