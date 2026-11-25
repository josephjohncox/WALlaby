# Repository guidelines

WALlaby is a Go-first PostgreSQL CDC adapter with a gRPC control plane and
per-flow workers. Priorities are correctness under failure, performance,
lifecycle management, schema evolution, durable checkpoints and OpenTelemetry.

## Scope and hierarchy

This file applies to the whole repository. Read the nearest `AGENTS.md` before
changing a subtree; its rules add to these rules. More-specific rules take
precedence within their scope. Do not copy this file into editor-specific rule
files that can drift from it.

| Path | Local guidance |
| --- | --- |
| `connectors/` | Adapter lifecycle, delivery guarantees and transport tests |
| `internal/` | Control plane, fencing, orchestration and durable state |
| `pkg/` | Shared public APIs, protocol invariants and wire formats |
| `proto/` | Typed API contracts and generated code |
| `terraform/provider/` | Nested Go module and typed endpoint models |
| `tests/` | Live-service tests and non-vacuous evidence |
| `specs/` | TLA+ models, manifests and trace coverage |
| `docs/` | User documentation and generated references |
| `.github/` | Public CI and dependency automation |

## Toolchain and workflows

`mise.toml` is the toolchain and task source of truth. Install mise, review the
configuration, then run `mise trust` and `mise install`. Use `mise run <task>`
for workflows and `mise exec -- <command>` for ad-hoc toolchain commands.
`mise tasks` lists all tasks. There is no second command runner.

- `mise run setup`: download modules and locked docs dependencies.
- `mise run build`: build CLI binaries and the Terraform provider into `bin/`.
- `mise run format` / `mise run format:check`: format / check Go code.
- `mise run lint`: Go lint and provider vet; `mise run proto-lint`: API lint.
- `mise run test`: complete root unit suite plus provider tests.
- `mise run test-coverage`: report-only per-package unit coverage for both Go
  modules, with the same root test-completeness accounting; not live coverage.
- `mise run generate`: regenerate Protobuf.
- `mise run tidy` / `mise run deps:check`: update / verify module manifests.
- `mise run security`: scan both Go applications and the locked Python docs.
- `mise run test-protocol`: production-loop exploration, real-store histories
  and Go/TLC mutation gates; see `docs/development/protocol-verification.md`.
- `mise run ci-check-test`: non-mutating local core gate (not every CI/live check).
- `mise run test-integration`: live harness; requires local services or kind.

Run focused tests while iterating, then the relevant complete gates. Report the
commands and results, including skips and unavailable external services. Do not
claim a live-service guarantee from a unit fake or a protocol model.

## Organization and style

- `cmd/wallaby/` is the API server; `cmd/wallaby-worker/` runs one flow.
- `connectors/` contains adapters; `internal/` contains engine implementations;
  `pkg/` contains shared library contracts; `proto/` is the API source.
- Use gofmt, tabs, short lowercase package names and table-driven tests.
- Put unit tests in `*_test.go`, service tests in `*_integration_test.go`.
- New components must honor cancellation and lifecycle transitions, release
  resources on every failure path, and emit OTEL traces/metrics.
- Test checkpoint/recovery behavior and schema evolution: add/drop/alter/rename
  columns, generated columns and extension types where applicable.

## Compatibility and security

Do not preserve backward compatibility. Remove obsolete paths instead of adding
fallbacks, migrations or parallel configuration surfaces. Regenerate derived
artifacts instead of editing them by hand.

This is a public project. Development and ordinary CI must not require a
particular employer, private CLI, internal registry, VPN, cloud account or
organization login. Examples use localhost or reserved example domains and
explicit opt-in credentials for live cloud tests. Never commit secrets, local
paths, private endpoints or diagnostic output containing credentials.

Update all affected Go modules and lockfiles together. Keep dependency versions,
toolchain pins, Docker and CI consistent. Prefer maintained upstream modules;
record any blocked update with evidence rather than suppressing an advisory.

Use focused Conventional Commits. PRs should describe behavior changes, test
evidence, performance effects and contract breaks. Roadmap items live in
`TODO.md`; executable behavior and tests, not historical checklists, are the
source of truth.
