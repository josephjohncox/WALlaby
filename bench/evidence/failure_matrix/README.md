# Deterministic process-failure matrix evidence

This directory holds machine-readable evidence from the in-process
process-failure matrix (`internal/failmatrix`, driven by `cmd/wallaby-failmatrix`
and `just test-failure-matrix`).

## What the matrix is

The matrix is an **executable specification** of WALlaby's durable delivery
boundary chain: fence acquisition, durable attempt, external destination side
effect (before and after), destination receipt, PostgreSQL adoption,
authoritative checkpoint, source ACK (intent + flush + flush receipt), artifact
publication, consumer receipt, retention release, and garbage collection. For
each boundary it injects a `kill`, `restart`, or `overlapping_takeover` fault,
runs recovery, and asserts the standing safety invariants.

It requires no live services and no credentials, so it runs hundreds of
randomized crash cycles per boundary deterministically and cheaply. It is a
protocol-fake model — it does **not** replace the real local-service integration
harnesses, which remain the promotion evidence for the exact maintained
profiles. Experimental cells (`snowflake-sql-v1`, `snowpipe-copy-v1`,
`snowpipe-streaming-*`) exercise the protocol only; live commercial cells remain
credential-gated and are excluded from promotion evidence.

The matrix never claims exactly-once. Replays converge by deterministic identity
(at-least-once with idempotent dedupe); duplicates are bounded and gaps are
impossible.

## Files

- `cycles.ndjson` — one JSON object per crash cycle (schema below).
- `summary.json` — machine-readable roll-up: pass/fail counts, per-boundary and
  per-profile cycle counts (no-skip accounting), fail-closed cycle count, and a
  coverage verdict.
- `summary.txt` — the same roll-up rendered for humans.

`run_canonical/` is a committed sample produced with `-cycles 100 -seed
20260728` (6 profiles × 10 boundaries × 100 cycles = 6000 cycles). Fresh
`run_*` directories are git-ignored; regenerate them with:

```bash
just test-failure-matrix                 # bounded gate, 100 cycles/boundary
FAILURE_CYCLES=1000 just test-failure-matrix   # deeper nightly sweep
```

## `cycles.ndjson` schema

```json
{
  "cycle": 0,
  "seed": 2744588129839429875,
  "profile": "postgres-to-postgres-v1",
  "kind": "maintained",
  "boundary": "before_side_effect",
  "fault": "kill",
  "injected": true,
  "recovered": true,
  "converged": true,
  "fail_closed": false,
  "external_applies": 1,
  "adoptions": 1,
  "checkpoint_lsn": 100,
  "source_flush_lsn": 100,
  "stale_rejected": true
}
```

`fail_closed` cycles are the unlinked streaming profile (`snowpipe-streaming-v1`)
correctly refusing to advance any durable state without a linked transport. They
are a passing outcome, not a defect, and are counted explicitly rather than
skipped.
