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

## Generated files

Run `just generate` after editing Protobuf definitions. Run `just docs-generate` after changing Protobuf comments, public symbols under `pkg/`, or package comments. CI checks both generated trees for drift.

## Documentation preview

Install uv, Go, and Buf, then run:

```bash
just docs-preview
```

The preview server rebuilds the site as Markdown changes. `just docs-build` performs the strict static build used for GitHub Pages.

## Verification

The [formal verification guide](../specs.md) explains the TLA+ models, action manifests, coverage, and runtime trace validation. The [benchmark guide](../benchmarks.md) documents the benchmark harness and report formats.
