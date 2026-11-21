# Dependency maintenance

`mise.toml` pins developer tools. Go application manifests, the nested Terraform
provider module own library dependencies. `uv.lock`
locks the documentation environment. GitHub Actions use verified commit SHAs
with adjacent release comments.

The former `tools/tools.go` blank imports are removed: Buf, golangci-lint and
GoReleaser are tools supplied by mise, not application dependencies. Protobuf
plugins, staticcheck and govulncheck have explicit versions in mise tasks.

## Update procedure

1. Review release notes, supported server versions and known advisories.
2. Update the relevant mise pins, module manifests or Python constraints.
3. Run `mise run tidy` and regenerate derived artifacts with `mise run generate`.
4. Run focused tests, then `mise run ci-check-test` and `mise run docs-check`.
5. Run real-service gates for affected delivery contracts. Unit fakes, successful
   compilation and skipped integration tests are not live-delivery evidence.

`mise run deps:check` verifies both Go modules, their checksums/tidy state and
the Python lock. `mise run security` scans reachable code in both application Go
modules and audits the hashed Python lock with pip-audit 2.10.1. The Python-only
gate is `mise run security-python`. uv uses the exact mise Python pin rather than
silently reusing an older `.venv` interpreter. Dependabot monitors every Go
module, uv, Actions, Docker and Compose.
It does not update mise pins; review those with `mise outdated` and keep the Go
version aligned with both application modules and Dockerfile.

## Security changes in this update

- Snowflake uses `github.com/snowflakedb/gosnowflake/v2 v2.4.0`. The unversioned
  module ends at vulnerable `v1.19.1`; updating that import path alone cannot fix
  GHSA-qqj6-54q6-cxv6. The staged protocol now uses native v2 streaming download
  and uncompressed upload APIs instead of the removed transfer flags. SDK
  no-clobber is best effort, not an atomic create; exact byte verification remains
  mandatory. Uploads keep the already-bounded plan in memory rather than spilling
  plaintext CDC data into an unmanaged temp directory. Budget memory accordingly.
  Mise and the official container disable the optional self-extracting native
  minicore with `SF_DISABLE_MINICORE=true`. Custom embedders/deployments must set
  that policy themselves (or use the SDK build tag); Go vulnerability scans do
  not prove properties of embedded native code.
- OpenTelemetry and its OTLP exporters use `v1.47.0`, covering the attribute-limit
  and exporter-log advisories on the previous versions.
- Go JOSE is selected at `v4.1.9`, past the fixes for the JWK and JSON/CBC
  processing advisories. Removing tool-only modules also reduces the graph.
- The docs lock contains urllib3 `2.8.0` and certifi `2026.10.23`, past the fixed
  versions for the reported proxy/streaming and CA-bundle advisories.
- Wire codecs import maintained `iskorotkov/avro/v2 v2.35.0` directly. Iceberg
  0.7 uses `twmb/avro`; it no longer requires the archived hamba module. The
  obsolete alias shim, generator, replacement and nested module are removed.
  Decoder security regressions remain covered by `pkg/wire` tests.

GitHub alerts and redundant Dependabot PRs are remote state. They are not
manually dismissed by this change; GitHub must analyze the merged manifests.

## Arrow and Iceberg upgrade

The application selects Arrow Go **18.9.0** and Iceberg Go **0.7.0**, the latest
stable releases checked for this update. The Terraform module selects the same
Arrow version. Related Arrow-based constraints are lifted: Azure Blob is 1.8.3,
gocloud is 0.49.0 and Substrait protobuf bindings are 0.85.0. No older encoder,
dependency replacement or vendored Arrow implementation is retained.

Both canonical Parquet byte-hash goldens are intentionally regenerated for
Arrow 18.9.0. Logical batch/content IDs, schema IDs, artifact IDs, object keys
and mapping identities remain unchanged; the serialized byte hashes change:

| Projection | Arrow 18.9.0 encoded SHA-256 |
| --- | --- |
| `canonical_cdc_parquet_v1` | `42c8d345515f8b46c5eb494982e7ed2288b66e5c06b4e33d5eb8c7eadd1e2539` |
| `canonical_cdc_parquet_v2` | `3c97f25be44b4aaf64cae3fe3325075ad7c23e034d9f46b22765dda4f48de704` |

This is an intentional byte-compatibility break, not an attempt to reproduce the
18.5.2 encoder. Do not mix old and new publisher builds within a durable flow
incarnation; use fresh incarnations for the changed encoder. There is no old-byte
fallback or migration path. Exact stored-byte verification, schema/identity
checks, replay determinism and corruption tests remain enabled.

```bash
mise exec -- go test ./internal/artifactlog \
  -run '^TestCanonicalProjectionV[12]' -count=10
mise run test-durable-pr
mise run test-checkpoint5-iceberg-integration
```

Iceberg 0.7 partition fields expose a source-ID slice and a two-value iterator.
The consumer validates every source ID, including later entries, and rejects
an empty source list rather than accepting it vacuously.

## Remaining independent constraint

Kubernetes 0.37.2 uses structured-merge-diff/v6, so kube-openapi remains at
`v0.0.0-20260721132016-d427ff9ee9ad`; newer source returns v7 types.
This is separate from the removed Arrow/Iceberg holds. Do not treat
`go get -u ./...` succeeding as proof that an upgrade is valid.
