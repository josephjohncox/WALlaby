# Contributor guide

Install [mise](https://mise.jdx.dev/), review `mise.toml`, then run
`mise trust`, `mise install` and `mise run setup`. The same mise tasks and pinned
tools run locally and in public GitHub Actions; there is no second task runner.
Use `mise tasks` to list workflows and `mise exec -- <command>` for ad-hoc tools.

DuckDB links through cgo, so builds also require a platform C/C++ compiler
(for example, build-essential on Debian/Ubuntu or Xcode Command Line Tools on
macOS). Docker and kind are only needed for the corresponding live test gates.

```bash
mise run build
mise run format
mise run ci-check-test
mise run test
RAPID_CHECKS=100 mise run test-rapid
mise run spec-verify
mise run tla
mise run docs-check
```

`mise run test` includes the root completeness-accounted suite and the separate
Terraform provider module. `mise run deps:check` also verifies the Avro shim
module and the Python lock. Read the root `AGENTS.md` and the nearest subtree's
`AGENTS.md` before changing code. See [dependency maintenance](dependencies.md)
for security updates and documented upgrade constraints.

Integration tests require PostgreSQL logical replication and, for some suites,
destination services or a kind cluster. See `tests/README.md` for harness setup.
The [native Iggy guide](../connectors/iggy.md) includes its standalone local test.

Pi Lens excludes `charts/wallaby/templates/**/*.yaml` from standalone YAML parsing because those files contain Helm Go-template directives. The project config enables Helm render validation instead. `helm lint`, `helm template`, and `scripts/helm-check.sh` are the authoritative chart checks. Values files and other YAML remain in ordinary YAML analysis.

## Generated files

Run `mise run generate` after editing Protobuf definitions. Run `mise run docs-generate` after changing Protobuf comments, public symbols under `pkg/`, or package comments. CI checks both generated trees for drift.

### Intentional Protobuf breaks

`mise run proto-breaking` remains strict against the selected base revision. Buf diagnostics are normalized to symbol identities and must exactly equal `scripts/proto-breaking.allowlist`. The allowlist contains only the Task 15 removal of `MarkDDLAppliedRequest`, `MarkDDLAppliedResponse`, and `DDLService.MarkDDLApplied`; line-number drift is ignored, but any added, missing, renamed, malformed, duplicated, or obsolete entry fails CI. A successful Buf result also fails while the allowlist remains nonempty.

These APIs were intentionally removed rather than preserved through compatibility messages, RPC aliases, adapters, or runtime shims. Delete an allowlist entry when the comparison base no longer contains that symbol. Do not add a new entry merely to make CI pass: any further wire break requires its own explicit review and policy decision.

## Documentation preview

After `mise run setup`, run:

```bash
mise run docs-preview
```

The preview server rebuilds the site as Markdown changes. `mise run docs-build` performs the strict static build used for GitHub Pages.

## Verification

The [formal verification guide](../specs.md) explains the TLA+ models, action manifests, coverage, and runtime trace validation. The [benchmark guide](../benchmarks.md) documents the benchmark harness and report formats.
