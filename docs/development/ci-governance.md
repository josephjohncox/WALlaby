# CI check governance

WALlaby treats required GitHub check names as a compatibility contract. Branch rulesets should require the checks below on the exact pull-request head before `main` is updated. A skipped, neutral, stale-head, or missing required check is not equivalent to a pass.

## Required checks

| Workflow | Required check contexts |
| --- | --- |
| CI Build | `build` |
| CI Lint | `lint`, `terraform-provider` |
| CI Generation | `generated-artifacts` |
| CI Spec | `spec` |
| CI Integration | `integration`, `checkpoint5-iceberg`, `postgres-managed-profile-14`, `postgres-managed-profile-15`, `postgres-managed-profile-16`, `postgres-managed-profile-17` |
| CI Evidence | `failure-matrix-model`, `failure-matrix`, `connector-matrix`, `benchmark-smoke` |
| CodeQL | `Analyze (actions)`, `Analyze (go)`, `Analyze (python)`, `CodeQL` |

Issue #79 tracks enforcement of this contract in repository-hosted branch rules. This page defines the intended required set; it does not claim that an administrator cannot weaken repository settings.

## Non-vacuous evidence checks

`checkpoint5-iceberg` runs `just test-checkpoint5-iceberg-integration` against the maintained local Iceberg REST/MinIO harness. The recipe requires both of these tests through `IT_REQUIRED_TESTS`:

- `TestIcebergRESTLiveAppendProjection`
- `TestIcebergRESTLiveSchemaEvolutionRename`

The integration JSON verifier requires each named test to emit a chronological `run` event and terminal `pass` event. Missing tests, skips, malformed JSON, package failure, or a pass without a preceding run fail the check.

`failure-matrix-model` runs `just test-failure-matrix-model` as a distinct check. It is in-process executable-model evidence only. `failure-matrix` remains the separate real-child OS-process death/restart matrix, and neither substitutes for the other or for destination implementation evidence.

## Intentionally non-required checks

The following are not maintained promotion requirements:

- `snowflake-managed-profile-unpromoted` — commercial Snowflake credentials and a reviewed deployment cell are intentionally unavailable in ordinary CI;
- `external-links` — scheduled external-network documentation check;
- `deploy` — GitHub Pages deployment runs only after documentation builds on `main`, not on pull requests; tag-only artifact publication uses separate `release-verification` and `publish` jobs;
- AWS S3 Tables and Snowflake linked-catalog live gates — credential-gated operator evidence, not ordinary branch admission.

Credential-gated checks must remain explicit and fail closed when deliberately invoked, but their absence cannot be used to imply maintained support.

## Changing the required set

A pull request that renames, splits, removes, or conditionally skips a required job must update this page and the repository ruleset in the same reviewed change. The replacement check must run on `pull_request`, use the exact checked-out head SHA, and preserve machine-readable no-skip accounting where the underlying recipe declares required tests.
