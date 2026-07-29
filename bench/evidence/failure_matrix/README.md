# Failure-matrix evidence classes

This directory contains credential-free protocol evidence. It does not contain
proof that PostgreSQL, ClickHouse, Snowflake, Snowpipe, or any connector
implementation ran.

## OS-process protocol evidence

`just test-failure-matrix` prebuilds `wallaby-failmatrix` and
`wallaby-failmatrix-worker`, reuses that real child executable, and writes fresh
runs under `os_run_<timestamp>_seed<seed>/`.

For at least 100 deterministic cycles in every applicable modeled
`(profile,boundary)` cell, plus separately counted unlinked-Streaming negative
cells that reject before durable attempt preparation, the parent:

- starts an initial child PID;
- has the child persist protocol-model state with expected-generation CAS under
  an inter-process file lock, file `fsync`, atomic rename, and directory `fsync`;
- applies and verifies actual PID `SIGKILL`, planned restart, or overlapping
  stale/new-generation execution;
- starts a distinct recovery PID and executes seed-selected, expected-generation
  CAS probes before, after, or on both sides of the real recovery/takeover
  durable transition, producing distinct fsync-backed revision orderings;
- reloads the durable state and checks convergence or explicit fail-closed state;
- enforces timeout, state-size, child-count, no-skip, and no-vacuity bounds.

The overlapping mode keeps both PIDs alive, durably advances the new generation,
and requires the old PID's real state-store mutation to receive a typed stale
CAS rejection without changing the durable revision or SHA-256. The unlinked
Streaming profile reports every requested boundary as a negative fail-closed
check with `attempt_prepared=false`, zero advancement, and
`boundary_reached=false`; they are
never silently skipped or presented as reached boundary cells.

Each OS-process run contains:

- `cycles.ndjson` — raw per-cycle evidence including seed-derived schedule and
  observed-order hashes, ordered CAS operation/generation/revision/state-hash
  observations, PIDs/timing, fault
  event, stale CAS before/after digest and revision, complete final protocol
  state, and violations;
- `normalized.ndjson` — stable protocol evidence with complete final state and
  PID/CPU/timing noise removed;
- `summary.json` — raw worker/platform, exact applicable/negative counts,
  skips, failures, resource maxima, and verdicts;
- `normalized-summary.json` — stable summary bytes without wall-clock,
  executable-path, PID, or CPU data;
- `summary.txt` — human-readable scope and roll-up.

Fresh `os_run_*` directories are Git-ignored and uploaded from CI.

## In-process model-only evidence

`run_canonical/` is the committed legacy sample from the fast in-process
executable model (`just test-failure-matrix-model`). Its `cycles.ndjson` schema
has no child PIDs because no child process was involved. It remains useful for
model determinism, but it is not OS-process or destination implementation proof.

The bounded `just test-soak` command is also in-process model evidence and writes
to `bench/evidence/soak`.

## Evidence boundaries

Protocol profile names select model behavior; they do not imply a destination
connection. Real-service integration and promotion commands are documented
separately in the development guide. No failure-matrix evidence claims
exactly-once or comparative performance.
