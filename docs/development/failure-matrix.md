# OS-process failure evidence, model, load, and soak

WALlaby has two credential-free protocol checks which must not be confused with
real destination evidence:

1. `just test-failure-matrix` prebuilds a parent executable and a real child
   executable, then collects **OS-process protocol evidence**.
2. `just test-failure-matrix-model` runs the fast in-process executable model.

Neither command connects to a destination, proves a connector implementation,
claims exactly-once, or replaces the real-service promotion suites.

## OS-process evidence

For every applicable modeled `(profile,boundary)` cell, the required gate runs at least
100 deterministic cycles. Every requested cell for unlinked Streaming is an
unreachable negative fail-closed check because rejection occurs before durable
attempt preparation; none is reported as a synthetic reached boundary. A child computes protocol-model transitions and replaces its state
with expected-generation CAS under an inter-process file lock, file `fsync`,
atomic rename, and directory `fsync`. The parent then applies one of three
process faults:

- `kill`: send `SIGKILL` to the recorded initial PID and verify its wait status;
- `restart`: observe a planned boundary exit and start a distinct recovery PID;
- `overlapping_takeover`: keep the old PID alive while a new PID durably advances
  the generation, release the old PID to attempt a real corrupting mutation
  through that same locked state-store API, require a typed stale-generation
  rejection with an unchanged durable revision and SHA-256, and then let the
  new PID recover.

The prebuilt worker is reused for the full run. Its SHA-256 and platform are in
the summary. Each seed selects a real durable schedule: an expected-generation
CAS probe before the recovery/takeover transition, after it, or on both sides.
Every probe rewrites the fsync-backed protocol state through the same locked
state-store API and increments its durable revision. Evidence records the
ordered operation, generation, revision, and state SHA-256; marker files only
coordinate the children and are not counted as schedule variants. The state is
a credential-free protocol-model state file, **not** a PostgreSQL WAL,
destination receipt store, or connector implementation.

The boundaries are before/after side effect, destination receipt, PostgreSQL
adoption, checkpoint, source ACK, artifact publication, consumer receipt,
retention release, and GC. Modeled protocol profile labels are
`postgres-to-postgres-v1`, `clickhouse-append-v1`, `snowflake-sql-v1`,
`snowpipe-copy-v1`, `snowpipe-streaming-linked-v1`, and
`snowpipe-streaming-v1`. Those names parameterize the protocol model only.
They are not evidence that the named destination implementation ran.

The unlinked `snowpipe-streaming-v1` profile halts before durable attempt
preparation. Every requested boundary still applies a real process fault and
restart, but records `boundary_reached=false`, `fail_closed=true`,
`attempt_prepared=false`, and zero external apply, destination receipt,
adoption, checkpoint, ACK intent, source flush/receipt, publication, consumer
receipt, retention release, and GC. They are reported
under separate negative counts rather than claimed as reached cells. A
non-fail-closed cell that does not reach its requested boundary fails.

## Run

```bash
just test-failure-matrix
```

The default executes 6 modeled profiles × 10 requested boundaries × 100 cycles
= 6000 cycles and 12,000 child starts. All unlinked Streaming requests are
separate negative checks. `coverage_ok=true`
requires:

- the exact expected cycle count and at least 100 cycles in every applicable or
  explicitly negative cell;
- zero skipped or failed cycles;
- nonzero `kill`, `restart`, and `overlapping_takeover` counts plus all three
  fsync-backed CAS schedules in every cell;
- an applied process fault and distinct initial/recovery PIDs in every cycle;
- boundary reach for every linked cell;
- no invariant violations; and
- per-cycle timeout, durable-state-size, and child-count bounds.

Deepen or replay deterministically:

```bash
FAILURE_CYCLES=1000 FAILURE_SEED=20260728 just test-failure-matrix
```

`FAILURE_CYCLES` below 100 is rejected by the required evidence command.
Focused Go tests use `RunProcessCycle` directly.

## Machine-readable evidence

Each run writes `bench/evidence/failure_matrix/os_run_<timestamp>_seed<seed>/`:

- `cycles.ndjson`: raw seed, profile, requested boundary, schedule/observed-order
  hashes, ordered fsync-backed CAS probe and transition revision observations,
  fault mode, PIDs/timing, observed
  SIGKILL/restart/overlap, stale CAS result and before/after durable
  digest/revision, the complete final protocol-model state, and violations;
- `normalized.ndjson`: those stable protocol fields and complete final state with
  raw PID, CPU, and timing data removed; identical seeds and worker semantics
  produce identical bytes;
- `summary.json`: raw worker/platform, timing/resource maxima, applicable and
  negative cell counts, skip/failure/fail-closed counts, and verdicts;
- `normalized-summary.json`: the stable summary fields without wall-clock,
  executable-path, PID, or CPU noise;
- `summary.txt`: the same scope and roll-up for humans.

`bench/evidence/failure_matrix/run_canonical/` is legacy **in-process model-only**
evidence. It is retained for model reproducibility and is not OS-process proof.
Fresh evidence directories are ignored by Git and uploaded by CI.

## Fast model and bounded soak

```bash
just test-failure-matrix-model
just test-soak                 # default SOAK_DURATION=30s
SOAK_DURATION=5m just test-soak
```

These run in-process and are labeled model evidence. `just test-bounded-load`
uses provisioned local services and is the separate destination/load surface.

## Nightly

`just test-durable-nightly` runs 1000 OS-process cycles per cell, a five-minute
in-process model soak, expanded property checks, and the live durable and DBOS
integration suites.

## Formal-model relationship

The protocol state mirrors safety properties from the managed-durability and
managed-PostgreSQL-delivery TLA+ models. TLC model checking, the in-process
executable model, OS-process death/restart evidence, and live-service connector
tests are separate evidence classes; none substitutes for another.
