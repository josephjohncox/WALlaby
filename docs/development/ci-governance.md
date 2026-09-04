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

`checkpoint5-iceberg` runs `just test-checkpoint5-iceberg-integration` against the maintained local Iceberg REST/MinIO harness. The recipe requires both of these tests through `IT_REQUIRED_TESTS`:

- `TestIcebergRESTLiveAppendProjection`
- `TestIcebergRESTLiveSchemaEvolutionRename`

The integration JSON verifier requires each named test to emit a chronological `run` event and terminal `pass` event. Missing tests, skips, malformed JSON, package failure, or a pass without a preceding run fail the check. Scheduled durability runs pass their explicit Go `-count` value to the verifier, which requires that exact number of complete top-level and nested `run`/`pass` cycles. Missing or extra repetitions fail closed.

`failure-matrix-model` runs `just test-failure-matrix-model` as a distinct check. It is in-process executable-model evidence only. `failure-matrix` remains the separate real-child OS-process death/restart matrix, and neither substitutes for the other or for destination implementation evidence.

## Intentionally non-required checks

The following are not maintained promotion requirements:

- `snowflake-managed-profile-unpromoted` — commercial Snowflake credentials and a reviewed deployment cell are intentionally unavailable in ordinary CI;
- `external-links` — scheduled external-network documentation check;
- `deploy` — GitHub Pages deployment runs only after documentation builds on `main`, not on pull requests; tag-only artifact publication uses separate `release-verification` and `publish` jobs;
- AWS S3 Tables and Snowflake linked-catalog live gates — credential-gated operator evidence, not ordinary branch admission.

Credential-gated checks must remain explicit and fail closed when deliberately invoked, but their absence cannot be used to imply maintained support.

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
