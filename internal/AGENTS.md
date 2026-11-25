# Engine and control-plane rules

Read the root guidance first. `internal/` owns orchestration, lifecycle,
checkpoints, schema/DDL state and runtime assembly, not stable external APIs.

- Persist progress before source acknowledgement. Keep source feedback,
  destination receipts and authoritative checkpoints distinct.
- Every durable mutation must respect lifecycle generation and fencing. A stale
  worker must not advance checkpoints, publish artifacts or acknowledge a source.
- Preserve explicit recovery states for ambiguous operations. Do not turn an
  unknown outcome into success, absence or an unconditional resend.
- `endpointcodec/` is the single boundary between typed persisted/API endpoints
  and `connector.RuntimeSpec`. Built-ins must not use arbitrary options maps in
  the authoring surface. Reject unknown options and preserve optional presence.
- `runner/destination_catalog.go` owns first-party construction and support
  classification. Capability-affecting configuration needs a closed profile and
  an independent oracle, not a duplicate factory switch elsewhere.
- Honor cancellation and release goroutines, connections and leases on all exit
  paths. New lifecycle paths need recovery/fencing tests and OTEL instrumentation.
- Run focused package tests plus `mise run test-durable-pr` for durable-core
  changes; use the live gates when correctness depends on database/process behavior.
