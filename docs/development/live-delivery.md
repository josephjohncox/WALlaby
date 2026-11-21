# Live delivery through Go executor harnesses

```bash
mise run test-live-local
```

This opt-in task provisions PostgreSQL in an owned kind cluster and native Apache
Iggy through Docker Compose. The Go harness in `tests/integrationharness` owns
provisioning, readiness, environment setup, test execution and cleanup. It uses
real services, not a protocol model or fake destination.

## Requirements and ownership

- Running Docker engine, sufficient VM memory and a Linux container kernel at
  least 5.19 with io_uring enabled. The harness does not change host kernel policy.
- `mise install` supplies the pinned Go and kind tools. Docker Desktop/Engine is
  a platform prerequisite, not installed or restarted by the test.
- Kubernetes API access is through a private generated kubeconfig. Cluster
  creation does not rewrite the developer's default kubeconfig/current context.
- Every run has a distinct kind cluster and Compose project. Iggy uses an owned
  loopback port. Both the broker listening port and published port change
  together so metadata-based SDK routing/reconnects do not reach another broker.
- The Iggy fixture image is the same pinned SDK-compatible image used by the
  local example. Development credentials are explicit and public; the generated
  fixture and evidence directory are owner-only.
- Readiness uses the actual Iggy Go SDK's connect/authenticate handshake, not
  just a listening TCP port or a container CLI with a fixed port.

Do not point this harness at production or reuse its resource names. Existing
unrelated containers and clusters are not removed.

## Required evidence

Seven top-level tests are required, including all nested wire-format tests:

- `TestPostgresGenerationFenceRejectsStaleCommit`
- `TestPostgresCheckpointGenerationFenceRejectsStaleCommit`
- `TestPostgresCommitBeforeReceiptReconciles`
- `TestPostgresManagedTransactionCommitBeforeReceiptReconciles`
- `TestPostgresToPostgresManagedRecoveryContract`
- `TestIggyDestinationWireFormats` (JSON, Arrow, Avro, Protobuf)
- `TestPostgresToIggyCDCCheckpointRestart`

The pipeline test runs the real PostgreSQL logical source, Iggy destination,
Runner and SQLite checkpoint store. It inserts a real PostgreSQL row, reads it
back from Iggy, requires durable checkpoint progress and observed PostgreSQL
`confirmed_flush_lsn`, stops the pipeline, closes and reopens the SQLite database, then constructs a
fresh Runner/source/destination and delivers another row from the restored
checkpoint. It cleans up
its uniquely named slot, publication, schema and Iggy stream.

`IT_REQUIRED_TESTS` and the JSON verifier require exactly one chronological
run/pass repetition, complete nested suites and a passing package terminal.
Skipped, missing, malformed or failed tests cannot count as evidence. The task
preserves JSON under `.cache/live-evidence/<run>/go-test.json`; Compose service
receipts record the project, image, address, container and readiness/stop times
without private cloud credentials.

To request only native broker readback through the Go executor:

```bash
mise exec -- go test ./tests -count=1 -v \
  -run '^TestIggyDestinationWireFormats$' \
  -it-kind=false -it-services=none -it-compose-iggy
```

Ordinary unit tests do not enable these provisioning flags. The CI
`iggy-native` job runs `test-live-local` on a disposable hosted runner and uploads
its owned evidence directory. It remains an experimental, non-required
promotion context; it does not change repository branch rules.

## Snowflake is an external-service gate

Neither kind nor Compose can provision a real Snowflake account. A local
PostgreSQL container can provide its CDC source, but Snowflake delivery still
requires the explicitly admitted JWT identity and reviewed deployment cell.
Fakesnow or a local SQL substitute is not accepted as Snowflake evidence.

The existing real-service Go tests are driven by:

```bash
mise run test-snowflake-managed-profile
```

Supply `TEST_PG_DSN`, `WALLABY_TEST_SNOWFLAKE_MANAGED=1`, execution/provisioning
DSNs, account, user, host, private-key-file path, reviewed service version/region
and the distinct owner role documented in `tests/README.md`. Missing settings
fail the explicitly requested gate; an unset environment and skipped tests are
**blocked/unexecuted**, never a pass. Do not log keys, DSNs or credential values
in evidence.

## Scope

A passing run establishes the named real-service scenarios on the executed
SDK/server/platform versions. It does not establish every failure schedule,
exactly-once Iggy replay, cloud Snowflake delivery, unbounded liveness or formal
implementation refinement. The [protocol verification gate](protocol-verification.md)
checks different properties and does not replace this live gate.
