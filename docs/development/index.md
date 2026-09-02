# Contributor guide

Install [`just`](https://just.systems) (CI pins version 1.56.0). Use the root `justfile` recipes so local checks match CI.

```bash
just fmt
just test
RAPID_CHECKS=100 just test-rapid
just spec-verify
just tla
just docs-check
```

Integration tests require PostgreSQL logical replication and, for some suites, destination services or a kind cluster. See `tests/README.md` for harness setup.

Pi Lens excludes `charts/wallaby/templates/**/*.yaml` from standalone YAML parsing because those files contain Helm Go-template directives. The project config enables Helm render validation instead. `helm lint`, `helm template`, and `scripts/helm-check.sh` are the authoritative chart checks. Values files and other YAML remain in ordinary YAML analysis.

## Generated files

Run `just generate` after editing Protobuf definitions. Run `just docs-generate` after changing Protobuf comments, public symbols under `pkg/`, or package comments. CI checks both generated trees for drift.

### Intentional Protobuf breaks

`just proto-breaking` remains strict against the selected base revision. Buf diagnostics are normalized to symbol identities and must exactly equal `scripts/proto-breaking.allowlist`. The allowlist contains only the Task 15 removal of `MarkDDLAppliedRequest`, `MarkDDLAppliedResponse`, and `DDLService.MarkDDLApplied`; line-number drift is ignored, but any added, missing, renamed, malformed, duplicated, or obsolete entry fails CI. A successful Buf result also fails while the allowlist remains nonempty.

These APIs were intentionally removed rather than preserved through compatibility messages, RPC aliases, adapters, or runtime shims. Delete an allowlist entry when the comparison base no longer contains that symbol. Do not add a new entry merely to make CI pass: any further wire break requires its own explicit review and policy decision.

## Documentation preview

Install uv, Go, and Buf, then run:

```bash
just docs-preview
```

The preview server rebuilds the site as Markdown changes. `just docs-build` performs the strict static build used for GitHub Pages.

## Verification

The [formal verification guide](../specs.md) explains the TLA+ models, action manifests, coverage, and runtime trace validation. The [benchmark guide](../benchmarks.md) documents the benchmark harness and report formats.
