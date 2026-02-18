# WALlaby backlog and context notes

- [P1] Config file path is parsed but ignored at runtime. `--config` is threaded into startup, but active config loading remains env-only.
  - Fix by enforcing a single precedence contract: config file path > environment > defaults.
- [P1] `WALLABY_DBOS_MAX_RETRIES` parsing is silently tolerant and can zero out retries.
  - Return structured validation errors (or keep defaults with explicit warnings) when env/config values are malformed.
- [P1] Worker command mutates persisted flow source options in place.
  - Apply CLI overrides via copy-on-write into local flow copies.
- [P2] WAL replication frame handling can panic on malformed/empty payloads (`msg.Data[0]`).
  - Add explicit length checks and robust error accounting/metrics for malformed protocol frames.
- [P2] App shutdown teardown is path-dependent and can skip closes.
  - Normalize lifecycle cleanup through a single shutdown path for all runtime modes and error paths.
- [P2] Kubernetes job-name derivation can still collide under truncation stress.
  - Use deterministic, collision-resistant suffixing from full flow identity and guard name building rules.
- [P2] Worker `--mode` accepts unknown values without validation.
  - Enforce strict enum validation (e.g. `cdc`, `backfill`) with clear error.
- [P2] API start semantics differ between immediate-start and run-once paths when dispatcher is missing.
  - Decide and apply one consistent precondition contract; align return codes/messages.

- Backlog follow-up work to implement in one module:
  - Define and export a config precedence contract (file > env > defaults).
  - Add startup validation for numeric/time/string-enum settings.
  - Add explicit protocol-frame metrics/counters for parse errors.
  - Complete immutable copy-on-write for flow option overrides across worker and orchestrated paths.
  - Normalize resource ownership and teardown boundaries around app shutdown.
