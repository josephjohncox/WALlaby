# Repository Guidelines

## Project Overview
DuctStream is a Go-first CDC adapter for Postgres logical replication. It is API-driven (gRPC), supports multiple sources and destinations, and uses a workflow engine. Priorities: performance, lifecycle management, DDL handling, schema evolution, checkpointing, and OTEL.

## Project Structure & Module Organization
Keep new code organized as follows:
- `cmd/ductstream/`: API server entrypoint.
- `cmd/ductstream-worker/`: per-flow worker process (run a single flow in its own process).
- `internal/`: core engine (replication, workflow, schema evolution, checkpoints).
- `internal/orchestrator/`: DBOS-backed flow scheduling/dispatch.
- `internal/registry/`: schema registry + DDL event storage.
- `internal/ddl/`: pg_catalog diff scanner.
- `pkg/`: stable library APIs for adapters and shared types.
- `proto/`: gRPC/Protobuf definitions (API surface, wire contracts).
- `connectors/`: source/destination implementations (e.g., Snowflake, S3, Kafka).
- `terraform/provider/`: Terraform provider implementation.
- `tests/`: integration tests and fixtures.

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
- Optional: SQLite-backed schema registry / DDL store for non-Postgres deployments.

## Observability & Lifecycle Expectations
All new components must emit OpenTelemetry traces/metrics and honor flow lifecycle state transitions. Checkpointing and recovery paths should be tested and documented.
