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
- `wallaby` — gRPC API server.
- `wallaby-admin` — flow/DDL/stream/publication admin CLI.
- `wallaby-worker` — run a single flow (standalone or scheduled).

Examples:
```bash
wallaby-admin flow create -file examples/flows/postgres_to_s3_parquet.json
wallaby-admin stream pull -stream orders -group g1 -max 10 -json
wallaby-admin ddl list -status pending
wallaby-admin ddl approve -id 1
wallaby-worker -flow-id <flow-id> -max-empty-reads 1
```

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
