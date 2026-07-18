# Contributor guide

Use the Makefile entry points so local checks match CI.

```bash
make fmt
make test
make test-rapid RAPID_CHECKS=100
make spec-verify
make tla
make docs-check
```

Integration tests require PostgreSQL logical replication and, for some suites, destination services or a kind cluster. See `tests/README.md` for harness setup.

## Generated files

Run `make generate` after editing Protobuf definitions. Run `make docs-generate` after changing Protobuf comments, public symbols under `pkg/`, or package comments. CI checks both generated trees for drift.

## Documentation preview

Install uv, Go, and Buf, then run:

```bash
make docs-preview
```

The preview server rebuilds the site as Markdown changes. `make docs-build` performs the strict static build used for GitHub Pages.

## Verification

The [formal verification guide](../specs.md) explains the TLA+ models, action manifests, coverage, and runtime trace validation. The [benchmark guide](../benchmarks.md) documents the benchmark harness and report formats.
