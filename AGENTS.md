# Repository Guidelines

## Project Overview
DuctStream is a Go-first CDC adapter for Postgres logical replication. It is API-driven (gRPC) with Terraform-friendly setup, supports multiple sources and destinations, and uses a durable workflow engine backed by Postgres. Design priorities include performance, reliable lifecycle management (start/stop/resume), full DDL handling, schema evolution, checkpointing, and OTEL-based observability.

## Project Structure & Module Organization
This repository is early-stage; please keep new code organized as follows:
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
- Files: `*_test.go` for tests; `*_integration_test.go` for integration-only.
- Protobuf: snake_case file names; `service` and `rpc` names in UpperCamel.

## Testing Guidelines
- Prefer table-driven unit tests in `internal/` and `pkg/`.
- Integration tests should validate logical replication, DDL changes, and checkpoints.
- Include cases for schema evolution (add/drop/alter column, generated columns).

## Commit & Pull Request Guidelines
No Git history exists yet. Use Conventional Commits (e.g., `feat: add wal streaming`), and keep commits focused.
PRs should include:
- Clear description and motivation.
- Test evidence (commands and results).
- Notes on performance or compatibility impacts.
- Screenshots or logs for API/observability changes where relevant.

## Observability & Lifecycle Expectations
All new components must emit OpenTelemetry traces/metrics and honor flow lifecycle state transitions. Checkpointing and recovery paths should be tested and documented.
