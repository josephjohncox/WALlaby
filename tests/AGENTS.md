# Integration and evidence rules

Read the root guidance and `README.md` here before changing the live harness.

- Unit tests must not require cloud credentials or running services. Live tests
  use explicit environment gates and uniquely named resources with cleanup.
- A skipped test is not implementation evidence. Promotion gates must enumerate
  required names and fail on missing, skipped or incorrectly repeated results.
  Preserve JSON accounting through scripts/verify-go-test-json.go.
- Test native protocols against the documented compatible server release.
  Separate fake-client tests from real delivery/readback/restart tests.
- Never delete pre-existing developer data or reuse an unrelated broker topic,
  schema, bucket or replication slot. Bound timeouts and background processes.
- Test source snapshot-to-stream handoff, schema evolution, checkpoint recovery
  and stale/new generation overlap when those contracts change.
- Prefer localhost/emulators for public CI. Cloud/commercial cells must be opt-in,
  fail closed without credentials when explicitly requested, and document limits.
