# Public automation rules

Read the root guidance first. Workflows must run on public GitHub-hosted runners
without private organization infrastructure.

- Install tools through .github/actions/setup-mise and mise.toml. Do not add
  another version pin or tool installer to individual jobs.
- Pin third-party actions to verified full commit SHAs with adjacent release
  comments. Keep permissions minimal and checkout credentials disabled.
- Run the same mise tasks as local contributors. Preserve named required checks,
  durable/failure-matrix separation, required-test accounting and artifact upload.
- Cache keys must include toolchain/dependency inputs. Do not restore stale tool
  executables ahead of mise's pinned binaries.
- Update every module/lockfile and action ecosystem in Dependabot. Never dismiss
  an alert or suppress a failing scan to make an update appear complete.
- Explicitly credential-gated live cloud tests are separate from ordinary PR
  checks. Never write credentials to logs, caches or uploaded test artifacts.
