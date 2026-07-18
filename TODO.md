# WALlaby backlog and context notes

## Production-readiness contracts

- [x] **P0 — Destination capability contract**
  - [x] Declare transactional-batch, idempotent, replay-safe, DDL-executing, and lossy behavior for every destination.
  - [x] Validate acknowledgement and DDL policies before opening connectors or starting execution.
  - [x] Add table-driven capability and invalid-flow contract tests.
- [ ] **P0 — DDL execution receipt/outbox**
  - [x] Persist replay-safe destination DDL execution receipts and immutable destination manifests.
  - [x] Couple receipt persistence, registry transition, and checkpoint advancement through one recoverable protocol.
  - [x] Reject administrative `applied` transitions without an execution receipt.
  - [x] Add receipt-before-checkpoint crash recovery, replay, and integration tests.
  - [x] Preserve per-record source positions and validate immutable manifests before destination execution.
  - [ ] Add destination-specific reconciliation for the external-commit-before-receipt window.
- [x] **P1 — Connector support matrix**
  - [x] Classify every connector as maintained, experimental, deprecated, or placeholder.
  - [x] Require restart, replay, schema-evolution, and integration contracts before maintained status.
  - [x] Generate user-facing support documentation from the executable matrix.
- [x] **P1 — CI execution completeness**
  - [x] Enumerate expected Go tests deterministically.
  - [x] Fail CI when an expected test or package is omitted from machine-readable test results.
  - [x] Keep flaky-test quarantine explicit; never infer success from automatic reruns.
- [x] **P1 — Health contracts**
  - [x] Add startup, readiness, and liveness endpoints and Kubernetes probes.
  - [x] Replace the TCP-only Helm test with a readiness assertion.
  - [x] Support independent OTLP metrics and traces endpoints.
  - [x] Add configuration, server, chart, and deployment tests.

- [x] Config precedence contract confirmed and documented: `--config`/`WALLABY*_CONFIG` uses `config file > env > defaults`.
- [x] `WALLABY_DBOS_MAX_RETRIES` parsing is now fail-fast on malformed values (no silent disable).
  - Return structured validation errors for malformed env/config values (including numeric/range checks and string enums).
- [x] Worker command mutates persisted flow source options in place.
  - Apply CLI overrides via copy-on-write into local flow copies.
- [x] `just check` remains usable in environments where TLC cannot open JMX/RMI sockets.
  - Added the `SKIP_TLA_CHECKS` recipe input for sandboxed checks.
- [x] WAL replication frame handling can panic on malformed/empty payloads (`msg.Data[0]`).
  - Add explicit length checks and robust error accounting/metrics for malformed protocol frames.
- [x] App shutdown teardown is path-dependent and can skip closes.
  - Normalize lifecycle cleanup through a single shutdown path for all runtime modes and error paths.
- [x] Kubernetes job-name derivation can still collide under truncation stress.
  - Use deterministic, collision-resistant suffixing from full flow identity and guard name building rules.
- [x] Worker `--mode` accepts unknown values without validation.
  - Enforce strict enum validation (e.g. `cdc`, `backfill`) with clear error.
- [x] API start semantics differ between immediate-start and run-once paths when dispatcher is missing.
  - Decide and apply one consistent precondition contract; align return codes/messages.

- Backlog follow-up work to implement in one module:
  - [x] Define and export a config precedence contract (file > env > defaults).
  - [x] Add startup validation for numeric/time/string-enum settings using validator tags and startup contracts.
  - [x] Add explicit protocol-frame metrics/counters for parse errors.
  - [x] Complete immutable copy-on-write for flow option overrides across worker and orchestrated paths.
- [x] Normalize resource ownership and teardown boundaries around app shutdown.
  - [x] Standardize `RunFlowOnce` contract and messaging across server and dispatcher modes.
