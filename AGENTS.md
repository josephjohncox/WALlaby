# Repository Guidelines

## Project Overview
WALlaby is a Go-first CDC adapter for Postgres logical replication. It is API-driven (gRPC), supports multiple sources and destinations, and uses a workflow engine. Priorities: performance, lifecycle management, DDL handling, schema evolution, checkpointing, and OTEL.

## Project Structure & Module Organization
Keep new code organized as follows:
- `cmd/wallaby/`: API server entrypoint.
- `cmd/wallaby-worker/`: per-flow worker process (run a single flow in its own process).
- `internal/`: core engine (replication, workflow, schema evolution, checkpoints).
- `internal/orchestrator/`: DBOS-backed flow scheduling/dispatch.
- `internal/registry/`: schema registry + DDL event storage.
- `internal/ddl/`: pg_catalog diff scanner.
- `pkg/`: stable library APIs for adapters and shared types.
- `proto/`: gRPC/Protobuf definitions (API surface, wire contracts).
- `connectors/`: source/destination implementations (e.g., Snowflake, S3, Kafka).
- `terraform/provider/`: Terraform provider implementation.
- `tests/`: integration tests and fixtures.
- `tools/benchreport/`: benchmark output converter (JSON/CSV).

## Build, Test, and Development Commands
Use the Makefile for consistent workflows:
- `make fmt` — gofmt all packages.
- `make lint` — run golangci-lint.
- `make test` — run unit tests.
- `make test-integration` — run integration tests (requires logical replication enabled).
- `make proto` — regenerate Protobuf/gRPC stubs.
- `make tidy` — sync module dependencies (includes tools).

## Coding Style & Naming Conventions
- Go formatting: `gofmt` is required; use `goimports` for import cleanup.
- Indentation: tabs per Go conventions.
- Packages: short, lowercase; avoid `util`/`common` unless scoped.
- Tests: `*_test.go` for unit; `*_integration_test.go` for integration-only.
- Protobuf: snake_case file names; `service` and `rpc` names in UpperCamel.

## Testing Guidelines
- Prefer table-driven unit tests in `internal/` and `pkg/`.
- Integration tests should validate logical replication, DDL changes, and checkpoints.
- Include cases for schema evolution (add/drop/alter column, generated columns).

## Commit & Pull Request Guidelines
No Git history exists yet. Use Conventional Commits (e.g., `feat: add wal streaming`), and keep commits focused.
PRs should include description, test evidence, and performance/compatibility notes.

## Pending Tasks / Roadmap
- Orchestration & lifecycle: DBOS integration while keeping the lifecycle engine; separate consumer processes per flow (standalone + DBOS tasks); run-once RPC; durable K8s job dispatch (client-go, kubeconfig/out-of-cluster).
- Sources & snapshots: automate logical replication setup (wal_level, slots, publications, cleanup); initial snapshotting with `pg_export_snapshot()`; resumeable snapshot state + ack policy; publication/table add/remove lifecycle; schema/type compatibility (including extensions).
- Schema & DDL: schema registry; pg_catalog diffs + DDL capture stream; DDL approval/auto-apply gating with CLI; full DDL apply per destination with dialect mapping; evolution semantics (add/drop/alter/rename/typed changes).
- Destinations: implement Snowflake/Snowpipe/DuckDB/ClickHouse/Bufstream + HTTP/webhook + Postgres stream sink; hidden metadata tables (global, materialized PKs); append mode + watermark columns; JSON/JSONB arrays support everywhere; default type-mapping tables per destination.
- Wire formats & storage: selectable wire format (proto/arrow/avro) with consistent system-wide config; optional durable S3 storage (Parquet/Arrow/Avro/native column).
- Terraform & Helm: Terraform provider + example configs + acceptance harness; Helm chart (workers/pools, values.yaml + values-prod, chart tests, helm lint); publish workflow (OCI/ghcr, multi-arch, cosign).
- Benchmarks & perf: destination-specific DDL/mutation benchmarks; baseline comparisons vs Sequin/Debezium/PeerDB; flamegraphs/traces in bench runs; vary parallelism/record width and export CSV/JSON.
- Linting & analysis: golangci-lint config (Go-only) with CI blocking; property-based protocol invariants; statistical regression checks for bench results.
- CLI/admin & docs: stream pull/ack CLI with pretty JSON, DDL list/approve/apply, staging resolve flag; examples under `examples/`; usage/tutorial/architecture docs and `docs/streams.md`.
### Formal Verification
- [in progress] Lightweight TLA+/PlusCal spec for CDC protocol + flow lifecycle (now includes DDL gating + retry bounds); mirror invariants in property tests; trace log validation tool.
### Execution Order (current focus)
1) [done] Type system completeness + extension mapping (pgvector, postgis, hstore, citext, ltree); centralize type mapping + casts; round-trip tests per destination.
2) [done] Schema evolution lifecycle: DDL capture → approval → apply → checkpointed DDL stream; apply tests for Snowflake/ClickHouse/DuckLake/Postgres.
3) [done] Snapshot/backfill: `pg_export_snapshot()` + persistent snapshot state + resume; parallel snapshot workers; snapshot→stream switch tests.
4) [done] Workflow engines + orchestrators: DBOS/K8s/CLI parity, retry/recovery tests, run-once coverage. (DBOS retry/backoff test + K8s backoff test + CLI lifecycle tests.)
5) [in progress] Destinations parity: finish/verify Snowflake/Snowpipe/ClickHouse/DuckDB/DuckLake/Postgres/Kafka/HTTP; S3 partition + metadata. (Kafka/HTTP parity tests + Redpanda/HTTP fixtures added; Avro/Arrow/Proto Kafka decode assertions added.)
6) Wire formats + schema registry: enforce end-to-end wire format, registry for Avro/Proto, evolution tests.
7) Benchmarks + profiling: flamegraphs/traces + per-destination bench matrix.
8) Operational controls: publication add/remove lifecycle, durable state tables, cleanup/reconfig.
9) [done] Optional type mapping overrides via config (proto/terraform options, JSON/YAML files).
10) [done] RDS / AWS support with AWS IRSA/role-based auth.
11) [done] IAM support on the CLI endpoints as flags (so wallaby-admin publication ... can auth to RDS directly)

## Observability & Lifecycle Expectations
All new components must emit OpenTelemetry traces/metrics and honor flow lifecycle state transitions. Checkpointing and recovery paths should be tested and documented.
