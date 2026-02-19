# WALlaby backlog and context notes

- [x] Config precedence contract confirmed and documented: `--config`/`WALLABY*_CONFIG` uses `config file > env > defaults`.
- [x] `WALLABY_DBOS_MAX_RETRIES` parsing is now fail-fast on malformed values (no silent disable).
  - Return structured validation errors for malformed env/config values (including numeric/range checks and string enums).
- [x] Worker command mutates persisted flow source options in place.
  - Apply CLI overrides via copy-on-write into local flow copies.
- [x] `make check` remains usable in environments where TLC cannot open JMX/RMI sockets.
  - Added `SKIP_TLA_CHECKS` switch in Makefile for sandboxed checks.
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
