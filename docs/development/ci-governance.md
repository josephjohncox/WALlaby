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

The active `Main: reviewed PRs and required checks` repository ruleset enforces this contract for `main`. Repository settings remain the live authority. This page records the reviewed policy and its verification procedure.

## Non-vacuous evidence checks

`checkpoint5-iceberg` runs `mise run test-checkpoint5-iceberg-integration` against the maintained local Iceberg REST/MinIO harness. The recipe requires both of these tests through `IT_REQUIRED_TESTS`:

- `TestIcebergRESTLiveAppendProjection`
- `TestIcebergRESTLiveSchemaEvolutionRename`

The integration JSON verifier requires each named test to emit a chronological `run` event and terminal `pass` event. Missing tests, skips, malformed JSON, package failure, or a pass without a preceding run fail the check. Scheduled durability runs pass their explicit Go `-count` value to the verifier, which requires that exact number of complete top-level and nested `run`/`pass` cycles. Missing or extra repetitions fail closed.

`failure-matrix-model` runs `mise run test-failure-matrix-model` as a distinct check. It is in-process executable-model evidence only. `failure-matrix` remains the separate real-child OS-process death/restart matrix, and neither substitutes for the other or for destination implementation evidence.

## Intentionally non-required checks

The following are not maintained promotion requirements:

- `snowflake-managed-profile-unpromoted` — commercial Snowflake credentials and a reviewed deployment cell are intentionally unavailable in ordinary CI;
- `iggy-native` — experimental native Iggy readback against the pinned SDK/server pair; it is not a maintained promotion claim or a required branch-rule context yet;
- `external-links` — scheduled external-network documentation check;
- `deploy` — GitHub Pages deployment runs only after documentation builds on `main`, not on pull requests; tag-only artifact publication uses separate `release-verification` and `publish` jobs;
- AWS S3 Tables and Snowflake linked-catalog live gates — credential-gated operator evidence, not ordinary branch admission.

Credential-gated checks must remain explicit and fail closed when deliberately invoked, but their absence cannot be used to imply maintained support.

## Security scanning and dependency evidence

GitHub-managed CodeQL default setup analyzes Go, Python and Actions with the
extended query suite and remote-and-local threat model. The configuration's
validation run succeeded before this update was reported active. The repository
settings API remains the live authority, not the presence of this text.
CodeQL does not verify SQL, Terraform, shell, TLA+ or Lean semantics; their focused
validators and executable proof/model gates remain separate. Secret scanning and
push protection are enabled in repository settings.

`Supply chain security` reviews dependency changes on pull requests and rejects
new known vulnerabilities at low severity or above in runtime, development or
unknown scope. It uses a read-only token and does not post privileged comments.
On main pushes or explicit trusted dispatch, Component Detection scans both Go
modules and the uv lock into one explicitly correlated dependency-graph snapshot.
The uv detector is enabled explicitly; empty results fail the job. Ignored caches
and editor settings are outside the scan. Only the trusted submission job receives
contents-write permission. Its action commit is pinned; the action acquires the
current detector release. The workflow does not dismiss alerts; GitHub must
reconcile them with the submitted/default-branch manifests.

## Unit test coverage

`mise run test-coverage` runs the complete root test suite through the existing
JSON completeness checker, then the Terraform provider suite. Enumeration remains
uninstrumented; only the actual root execution receives atomic coverage flags.
The modules produce separate `.cache/coverage/root.out` and
`terraform-provider.out` profiles and function summaries, resolved in each
module's own context. CI uploads them as `go-unit-coverage`.

This is per-package unit coverage from each package's tests, with no percentage
threshold and no repository-wide/live-service coverage claim. Credential-gated
skips remain visible in completeness accounting. It is distinct from TLA action
coverage and the separately instrumented durable live-seam profile. Keep ordinary
tests, protocol mutation gates and required live-service checks intact.

## Review and merge policy

The `main` ruleset requires a pull request and one approval. A reviewer other than the last pusher must approve the final head. A new push dismisses stale approval. The pull request must resolve all review threads.

The branch must be current before merge. All required checks must pass on the exact pull-request head. The repository permits merge commits only. The ruleset blocks branch deletion and non-fast-forward updates.

Repository administrators have no silent direct-push bypass. The administrator repository role can use a pull-request-only emergency bypass. Before use, the administrator must link an incident or issue and state the reason in the pull request. GitHub records the bypass and the ruleset change in the repository audit log.

## Verify the live ruleset

Run these commands with a token that can read repository rulesets:

```bash
gh api repos/josephjohncox/WALlaby/rulesets/11786907 \
  --jq '{name,enforcement,conditions,bypass_actors,rules}'

gh api repos/josephjohncox/WALlaby/rulesets/11786907 \
  --jq '.rules[] | select(.type == "required_status_checks") | .parameters.required_status_checks[].context'
```

Verify these results:

1. The enforcement value is `active`.
2. The condition selects `~DEFAULT_BRANCH`.
3. Pull requests require one approval, last-push approval, stale-review dismissal, and resolved threads.
4. The only merge method is `merge`.
5. Strict required checks contain every name in the table above.
6. The administrator bypass mode is `pull_request`, not `always`.
7. The rules include deletion and non-fast-forward protection.

## Changing the required set

A pull request that renames, splits, removes, or conditionally skips a required job must update this page and the repository ruleset in the same reviewed change. The replacement check must run on `pull_request`, use the exact checked-out head SHA, and preserve machine-readable no-skip accounting where the underlying recipe declares required tests.
