# WALlaby

![Wallaby logo. Kicking a WAL block](./wallaby-small.png)

WALlaby is a Go-first CDC adapter for PostgreSQL logical replication. It is API-driven (gRPC + Terraform-friendly setup), supports multiple sources and destinations, and uses a durable workflow engine backed by Postgres. The focus is on performance, reliability, schema evolution, and observability.

## Goals
- High-throughput logical replication with checkpointing and recovery.
- Multi-source, multi-destination routing with lifecycle management.
- Efficient wire formats (Arrow, Parquet, Avro, Protobuf) and full DDL support.
- Best-in-class Go libraries with minimal custom reinvention.

## Why WALlaby (vs Airbyte/Fivetran/Debezium/PeerDB/Sequin)
- API-first control plane with desired-state flow configuration and lifecycle (start/stop/resume, publication sync, backfill/replay).
- Multi-sink fan-out with durable checkpoints and per-destination reconciliation.
- Schema fidelity via binary wire formats plus DDL capture/gating and evolution hooks.
- Lightweight Go services and per-flow workers (DBOS/K8s/CLI) instead of heavyweight runtimes.
- Built for streaming and API delivery without forcing Kafka as the control plane.

Quick contrasts:
- Airbyte/Fivetran: broad managed ELT catalogs; WALlaby focuses on streaming CDC with explicit lifecycle control.
- Debezium: JVM + Kafka-centric; WALlaby is Go-native with API-driven flows and multi-sink fan-out.
- PeerDB: strong warehouse replication; WALlaby is API-first with desired-state config and multiple destination types.
- Sequin: webhook-first; WALlaby includes HTTP delivery plus durable flows, DDL gating, and multiple sink types.

## Status
Active development. Interfaces and specs are stable, connectors and workflows are iterating quickly. See `AGENTS.md` for the current roadmap and TODOs.

## Specs & Verification
Specs are first-class artifacts in `specs/` (TLA+ models, coverage manifests). Runtime traces are validated against the spec and coverage is enforced.

Key targets:
```bash
make tla                 # run TLA+ model checks (flow, state machine, fan-out, liveness)
make tla-coverage         # generate action coverage for specs
make tla-coverage-check   # enforce minimum coverage
make trace-suite          # quick randomized trace suite (CI-friendly)
make trace-suite-large    # large randomized trace suite
make spec-verify          # regenerate spec manifests and verify they are checked in
```

Trace validation:
- `cmd/wallaby-trace-validate` validates trace files against spec invariants.
- `specs/coverage*.json` defines the shared action/invariant contract between spec and Go.

## CLI Tools
Core:
- `wallaby` — gRPC API server.
- `wallaby-admin` — flow/DDL/stream/publication admin CLI.
- `wallaby-worker` — run a single flow (standalone or scheduled).

Dev/validation helpers:
- `wallaby-speccheck` — runs `specaction` static checks.
- `wallaby-spec-sync` — validates local TLA spec declarations against manifests.
- `wallaby-spec-manifest` — regenerates local spec coverage manifests.
- `wallaby-tla-coverage` — computes and validates TLA+ action/invariant coverage.
- `wallaby-trace-validate` — validates traces against spec manifests.
- `wallaby-bench` / `wallaby-bench-summary` — benchmark tooling.

Examples:
```bash
wallaby-admin flow create -file examples/flows/postgres_to_s3_parquet.json
wallaby-admin stream pull -stream orders -group g1 -max 10 -json
wallaby-admin ddl list -status pending
wallaby-admin ddl approve -id 1
wallaby-admin version
wallaby-worker -flow-id <flow-id> -max-empty-reads 1
wallaby-speccheck --version
```

### Configuration references
- `wallaby` reads config from:
  - `--config path`
  - `WALLABY_CONFIG`
  - `wallaby.yaml` / `wallaby.yml` in current working directory (when `--config`/env path is not set)
- `wallaby-admin` reads config from:
  - `--config path`
  - `WALLABY_ADMIN_CONFIG`
  - `wallaby-admin.yaml` / `wallaby-admin.yml` in the current directory or `$HOME/.config/wallaby` (when no explicit config path is set)
- `wallaby-worker` reads config from:
  - `--config path`
  - `WALLABY_WORKER_CONFIG`
  - `wallaby-worker.yaml` / `wallaby-worker.yml` in the current working directory
- `wallaby-speccheck` reads config from:
  - `--config path`
  - environment-backed flags via `WALLABY_SPECCHECK_*` (for example: `WALLABY_SPECCHECK_MANIFEST`, `WALLABY_SPECCHECK_VERBOSE`)
- `wallaby-spec-sync` reads flag values from:
  - environment-backed flags via `WALLABY_SPEC_SYNC_*` (for example: `WALLABY_SPEC_SYNC_SPEC_DIR`, `WALLABY_SPEC_SYNC_MANIFEST_DIR`)
- `wallaby-spec-manifest` reads flag values from:
  - environment-backed flags via `WALLABY_SPEC_MANIFEST_*` (for example: `WALLABY_SPEC_MANIFEST_DIR`, `WALLABY_SPEC_MANIFEST_OUT`)
- `wallaby-tla-coverage` reads flag values from:
  - environment-backed flags via `WALLABY_TLA_COVERAGE_*` (for example: `WALLABY_TLA_COVERAGE_DIR`, `WALLABY_TLA_COVERAGE_MIN`)
- `wallaby-trace-validate` reads flag values from:
  - environment-backed flags via `WALLABY_TRACE_VALIDATE_*` (for example: `WALLABY_TRACE_VALIDATE_INPUT`, `WALLABY_TRACE_VALIDATE_MANIFEST`)
- `wallaby-bench` reads flag values from:
  - environment-backed flags via `BENCH_*` (for example: `BENCH_PG_DSN`, `BENCH_CLICKHOUSE_DSN`, `BENCH_KAFKA_BROKERS`)
- `wallaby-bench-summary` reads flag values from:
  - environment-backed flags via `WALLABY_BENCH_SUMMARY_*` (for example: `WALLABY_BENCH_SUMMARY_DIR`, `WALLABY_BENCH_SUMMARY_FORMAT`)

## Examples
See `examples/README.md` for gRPC, Terraform, and worker usage examples.

Flow specs you can copy from `examples/flows/`:
- Postgres → Kafka / HTTP / S3 (Parquet)
- Postgres → Snowflake / Snowpipe
- Postgres → ClickHouse / DuckDB / DuckLake
- Postgres → Bufstream / gRPC / pgstream

Snowpipe auto-ingest (upload-only) snippet:
```json
{
  "name": "snowpipe-out",
  "type": "snowpipe",
  "options": {
    "dsn": "user:pass@account/db/schema?role=SYSADMIN",
    "stage": "@my_external_stage",
    "format": "parquet",
    "auto_ingest": "true",
    "copy_on_write": "false"
  }
}
```

## Documentation
- `docs/usage.md` — operational usage and configuration.
- `docs/tutorials.md` — step-by-step tutorials.
- `docs/architecture.md` — system architecture overview.
- `docs/streams.md` — stream consumer operations and recovery playbooks.
- `docs/workflows.md` — DBOS/Kubernetes/CLI workflow modes.
- `docs/specs.md` — TLA+ specs and trace verification flow.
- `docs/connectors.md` — connector configuration notes and caveats.
- `docs/runbooks.md` — operational runbooks (DDL gating, recovery).
- `docs/benchmarks.md` — benchmark harness and results.

## Development & Testing
Common commands:
```bash
make fmt
make lint
make test
make test-rapid RAPID_CHECKS=100
make test-integration-ci
make check            # spec + TLA + unit tests
make check-coverage   # trace + TLA coverage + e2e
```

Benchmarking:
```bash
make benchmark
make benchmark-profile
make benchstat BASELINE=bench/results/<baseline> CANDIDATE=bench/results/<candidate>
```

## Helm (OCI via GHCR)
Once a tagged release is published, install via OCI Helm chart:
```bash
helm install wallaby oci://ghcr.io/josephjohncox/wallaby/charts/wallaby --version <tag>
```

An example values file is at `charts/wallaby/values.example.yaml`.
For production-style values (ConfigMap + Secret pattern), see `charts/wallaby/values-prod.yaml`.

## License
PolyForm Perimeter License 1.0.1. See `LICENSE` for details.
