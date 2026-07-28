# Process-failure matrix, load, and soak

WALlaby ships a deterministic, credential-free **process-failure matrix** that is
an executable specification of the durable delivery boundary chain, plus bounded
load and soak gates. None of these gates claim exactly-once, and none emit
comparative "winner" claims.

## What the matrix covers

For every supported protocol profile it injects a `kill`, `restart`, or
`overlapping takeover` fault at each boundary and asserts the standing safety
invariants after recovery. The boundaries are:

- before and after the destination side effect,
- destination receipt,
- PostgreSQL adoption,
- authoritative checkpoint,
- source ACK (intent, flush, flush receipt),
- artifact publication,
- consumer receipt,
- retention release,
- garbage collection.

Profiles: the exact maintained `postgres-to-postgres-v1` and
`clickhouse-append-v1`, plus the experimental `snowflake-sql-v1`,
`snowpipe-copy-v1`, `snowpipe-streaming-linked-v1`, and the unlinked
`snowpipe-streaming-v1`. The unlinked streaming profile must **fail closed** —
it never advances any durable state without a linked transport — and those cycles
are counted explicitly rather than skipped.

The matrix is a protocol fake. It does not replace the real local-service
integration harnesses, which remain the promotion evidence for the exact
maintained profiles. Experimental cells exercise the protocol only; live
commercial cells stay credential-gated and are excluded from promotion evidence.

## Running the matrix

```bash
just test-failure-matrix
```

Expected output (100 cycles per boundary per profile = 6000 cycles):

```
ok  	github.com/josephjohncox/wallaby/internal/failmatrix
failure-matrix evidence: bench/evidence/failure_matrix/run_<timestamp>_seed20260728
total=6000 passed=6000 failed=0 fail_closed=1000 coverage_ok=true
```

`coverage_ok=true` requires every `(profile, boundary)` cell to reach the target
cycle count with zero invariant violations. The gate exits non-zero otherwise, so
a stale run filter cannot pass vacuously.

Deepen the sweep or replay a specific seed:

```bash
FAILURE_CYCLES=1000 FAILURE_SEED=20260728 just test-failure-matrix
```

### Evidence

Each run writes machine-readable evidence under
`bench/evidence/failure_matrix/run_<timestamp>_seed<seed>/`:

- `cycles.ndjson` — one JSON object per crash cycle.
- `summary.json` — pass/fail counts, per-boundary and per-profile cycle counts
  (no-skip accounting), fail-closed count, and a coverage verdict.
- `summary.txt` — the human-readable roll-up.

A canonical sample is committed at
`bench/evidence/failure_matrix/run_canonical/`. Fresh run directories are
git-ignored.

## Bounded soak

The soak gate drives the matrix for a wall-clock budget and asserts bounded
goroutine and heap growth with no invariant violations:

```bash
just test-soak                 # default SOAK_DURATION=30s
SOAK_DURATION=5m just test-soak
```

Expected output:

```
soak evidence: bench/evidence/soak/run_<timestamp>_seed20260728
cycles=<n> passed=<n> failed=0 goroutines=1->1 heap_inuse=<a>-><b> ok=true
```

Goroutine count must return to its baseline band (no leak). Live-service load and
soak for the exact maintained profiles run separately:

```bash
just test-bounded-load         # requires the provisioned PostgreSQL + MinIO harness
```

## Nightly

`just test-durable-nightly` runs the deep matrix (`FAILURE_CYCLES=1000`), a
five-minute soak, expanded rapid property checks, and the live durable and DBOS
integration suites.

## Where it fits

The matrix mirrors the TLA+ managed-durability (ArtifactPublication) and
managed-PostgreSQL-delivery (SourceFeedback) models in executable Go. The models
are model-checked by `just tla`; their coverage is enforced by
`just tla-coverage-check`; and the matrix invariants are the runtime mirror. See
[Formal specifications](../specs.md).
