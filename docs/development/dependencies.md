# Dependency maintenance

`mise.toml` pins developer tools. Go application manifests, the nested Terraform
provider module and the Avro shim module own library dependencies. `uv.lock`
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

`mise run deps:check` verifies all three Go modules, their checksums/tidy state and
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
- The Avro shim forwards to maintained `iskorotkov/avro/v2 v2.35.0`; it does not
  compile the archived vulnerable hamba decoder. Keep its generated aliases and
  regression tests in sync with the nested module.

GitHub alerts and redundant Dependabot PRs are remote state. They are not
manually dismissed by this change; GitHub must analyze the merged manifests.

## Constrained upgrades

Do not treat `go get -u ./...` succeeding as proof of a valid upgrade. These
constraints were verified during the update; the regression tests remain strict.

| Dependency | Selected version | Why a newer version is not selected |
| --- | --- | --- |
| Arrow Go | `v18.5.2` | Frozen canonical v1/v2 Parquet byte hashes change in 18.6, 18.7, 18.8 and 18.9 |
| Iceberg Go | `v0.5.0` | 0.6 requires Arrow 18.6; 0.7 requires Arrow 18.8 |
| Azure Blob SDK | `v1.8.0` | Newer releases raise the Arrow minimum above the frozen encoder's version |
| gocloud | `v0.47.0` | Newer releases require newer Arrow |
| Substrait/protobuf bindings | `v0.84.0` | The updated substrait-go/v7.6 still uses the URI-based extension API removed by 0.85 bindings |
| kube-openapi | `v0.0.0-20260721132016-d427ff9ee9ad` | Newer source returns structured-merge-diff/v7 types, incompatible with Kubernetes 0.37.2's v6 types |

The Arrow check compared the same logical batch across versions. Logical IDs,
schema IDs and object keys remained stable, but serialized Parquet changed
(column offsets, optional statistics and root-schema metadata). Keeping current
Arrow with the old zstd library did **not** restore the bytes; using Arrow 18.5.2
with the updated zstd library did. The golden hashes were not regenerated:

```bash
mise exec -- go test ./internal/artifactlog \
  -run '^TestCanonicalProjectionV[12]'
```

A later Arrow upgrade needs an explicit canonical-encoding contract decision,
not a new expected hash for the same frozen projection. No new dependency
replacement, vendored encoder or silent compatibility path is introduced here.
