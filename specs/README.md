# Formal Specs

This directory holds lightweight formal specs for the CDC protocol and flow lifecycle.
The goal is to validate ordering, durability, retries, and DDL gating invariants early,
before we add a deeper model (multi-destination fan-out, backfill vs streaming modes,
and richer DDL workflows).

## CDC Flow Spec (TLA+)

File: `specs/CDCFlow.tla`

What it models:
- Flow state transitions (`Created → Running → Paused/Stopped/FailedHoldingSlot/FailedDroppedSlot`).
- Read → deliver → ack ordering for LSNs.
- Checkpoint monotonicity (no regression).
- DDL gating (pending → approved → applied).
- Source/destination retry attempts (bounded).
- Failure modes (hold slot vs drop slot) and configurable give-up behavior.

### Run with TLC

Run TLC (from the TLA+ tools) for all specs:

```
make tla
```

To run only this module:

```
TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlow.cfg make tla-single
```

The default config lives at `specs/CDCFlow.cfg`. Adjust constants like `MaxLSN`,
`MaxRetries`, `GateDDL`, and `DDLSet` there for deeper exploration.

For a liveness/fairness check, use:

```
TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlowLiveness.cfg make tla-single
```

For a DDL witness run (ensures approval/applied are reachable under fairness):

```
TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlowWitness.cfg make tla-single
```

To produce coverage reports:

```
make tla-coverage
make tla-coverage-check
```

`tla-coverage-check` respects `TLA_COVERAGE_MIN` and writes a JSON report to
`specs/coverage/report.json`.

To tweak TLC runtime flags, use `TLC_ARGS` and `TLC_JAVA_OPTS`. Some TLC builds
do not support `-noJMX`; if you hit sandbox port restrictions, run TLC outside
the sandbox or with a build that supports `-noJMX`.

## Flow State Machine Spec (TLA+)

File: `specs/FlowStateMachine.tla`

What it models:
- CLI-driven flow lifecycle transitions.
- `run-once` does not change state.

Run TLC:

```
TLA_MODULE=specs/FlowStateMachine.tla TLA_CONFIG=specs/FlowStateMachine.cfg make tla-single
```

## Fan-out Spec (TLA+)

File: `specs/CDCFlowFanout.tla`

What it models:
- Per-destination delivery and ack.
- Source ack only after all destinations ack.
- Configurable ack policy (all vs primary destination).

Run TLC:

```
TLA_MODULE=specs/CDCFlowFanout.tla TLA_CONFIG=specs/CDCFlowFanout.cfg make tla-single
```

## Trace Validation

We emit optional JSONL traces from the Go runner and validate them offline against
the same invariants (NoAckWithoutDeliver, AckMonotonic, CheckpointMonotonic, etc.).
See the `wallaby-trace-validate` tool and mirror any new invariants in property tests.

## Coverage Manifest

The coverage manifests are the shared contract between the TLA+ specs and Go traces.
They list allowed spec actions/invariants, minimum witness thresholds, and mark
unreachable items for the trace suite:

- `specs/coverage.json` (CDCFlow)
- `specs/coverage.flow_state.json` (FlowStateMachine)
- `specs/coverage.fanout.json` (CDCFlowFanout)

Regenerate them with:

```
make spec-manifest
```

To ensure the manifests stay in sync with the TLA+ `Next` blocks and config
invariants, run:

```
make spec-sync
```

Static analysis (`make spec-lint`) enforces that `SpecAction` values in code are
constants from the manifest.

To emit traces from a worker run, set `WALLABY_TRACE_PATH` (supports `{flow_id}`
placeholder) and run `wallaby-worker`. For the main server, `{flow_id}` is
replaced with `server`. Then validate (defaults to `specs/coverage.json`):

```
wallaby-trace-validate -input /path/to/trace.jsonl
```

## Next (Deeper Model)

Planned extensions:
- Multi-destination fan-out with per-destination ack.
- Backfill + streaming mode transitions.
- DDL approval gates and checkpoint coupling.
- Failure recovery and retry semantics.

When we add those, they will live in a separate module to keep the lightweight
spec fast to model-check.
