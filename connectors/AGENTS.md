# Connector rules

Read the root guidance first. Sources live under `sources/`, destinations under
`destinations/`; each adapter has its own package.

- Implement `pkg/connector` interfaces. Register a first-party destination in
  `internal/runner/destination_catalog.go`, reserve its endpoint type, and add a
  typed Protobuf branch and endpoint-codec round trip. Include Terraform support.
- Declare only guarantees the adapter actually provides. Append acknowledgements,
  transaction commits, replay safety and idempotent recovery are different
  contracts. New adapters start experimental until the required evidence exists.
- Open must validate before side effects and close partially created resources on
  failure. Write must propagate errors and cancellation; never acknowledge or drop
  a rejected record silently. Close must be safe after partial Open and on reuse.
- A transport carrying a DDL event does not execute schema changes. Keep
  `SupportsSchemaChanges` separate from `Delivery.ExecutesDDL`.
- Keep native protocols separate. Do not route a non-Kafka broker through the
  Kafka client just because both systems expose streams or topics.
- Use bounded message sizes, timeouts and retries. Resolve ambiguous writes by
  their original identity when the protocol allows; never invent exactly-once
  guarantees from an in-memory cache or message ID.
- Emit OTEL spans/metrics without credentials, payloads or unbounded labels.
  Test encoding, authentication/TLS selection, failed Open cleanup, replay,
  cancellation, oversize failures and schema evolution where applicable.
- Unit fakes test adapter decisions, not server compatibility. Put real-service
  tests under `tests/` and document required versions and environment variables.
