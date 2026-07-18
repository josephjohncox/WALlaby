# Formal Specs

This directory holds lightweight formal specs for the CDC protocol and flow lifecycle.
The goal is to validate ordering, durability, retries, and DDL gating invariants early,
before we add a deeper model (multi-destination fan-out, backfill vs streaming modes,
and richer DDL workflows).

## CDC Flow Spec (TLA+)

File: `specs/CDCFlow.tla`

What it models:

- Flow state transitions (`Created → Running ↔ Paused → Stopping → Stopped`, with active states able to enter `Failed`).
- Read → deliver → durable checkpoint → source acknowledgement ordering.
- Checkpoint and acknowledgement monotonicity, including failure before source acknowledgement.
- Process crash/restart and idempotent restore acknowledgement from the durable position.
- DDL gating (pending → approved → applied).
- Source/destination retry attempts (bounded).
- Failure modes (hold slot vs drop slot) and configurable give-up behavior.

### Run with TLC

Run TLC (from the TLA+ tools) for all specs:

```
just tla
```

To run only this module:

```
TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlow.cfg just tla-single
```

The default config lives at `specs/CDCFlow.cfg` and intentionally uses a small
state space suitable for CI. Override constants in a separate config for deeper
exploration rather than making the default model check unbounded.

For a liveness/fairness check, use:

```
TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlowLiveness.cfg just tla-single
```

For a DDL witness run (ensures approval/applied are reachable under fairness):

```
TLA_MODULE=specs/CDCFlow.tla TLA_CONFIG=specs/CDCFlowWitness.cfg just tla-single
```

To produce coverage reports:

```
just tla-coverage
just tla-coverage-check
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
TLA_MODULE=specs/FlowStateMachine.tla TLA_CONFIG=specs/FlowStateMachine.cfg just tla-single
```

## Fan-out Spec (TLA+)

File: `specs/CDCFlowFanout.tla`

What it models:

- Per-destination delivery and ack.
- Source ack only after all destinations ack.
- Configurable ack policy (all vs primary destination).

Run TLC:

```
TLA_MODULE=specs/CDCFlowFanout.tla TLA_CONFIG=specs/CDCFlowFanout.cfg just tla-single
```

## Trace Validation

We emit optional JSONL traces from the Go runner and validate them offline against
the same invariants (NoAckWithoutDeliver, AckMonotonic, CheckpointMonotonic, etc.).
Validation is per flow and compares PostgreSQL LSNs by their hexadecimal value;
decimal positions are treated as abstract batch ordinals. See the
`wallaby-trace-validate` tool and mirror new invariants in property tests.

## Coverage Manifest

The coverage manifests are the shared contract between the TLA+ specs and Go traces.
They list allowed spec actions/invariants, minimum witness thresholds, and mark
unreachable items for the trace suite:

- `specs/coverage.json` (CDCFlow)
- `specs/coverage.flow_state.json` (FlowStateMachine)
- `specs/coverage.fanout.json` (CDCFlowFanout)

Regenerate them with:

```
just spec-manifest
```

To ensure the manifests stay in sync with the TLA+ `Next` blocks and config
invariants, run:

```
just spec-sync
```

Static analysis (`just spec-lint`) enforces that `SpecAction` values in code are
constants from the manifest.

To emit traces from a worker run, set `WALLABY_TRACE_PATH` (supports `{flow_id}`
placeholder) and run `wallaby-worker`. For the main server, `{flow_id}` is
replaced with `server`. Then validate (defaults to `specs/coverage.json`):

```
wallaby-trace-validate --input /path/to/trace.jsonl
```

## Next (Deeper Model)

The remaining planned extension is a deeper model of backfill-to-stream mode
transitions. Fan-out, DDL gating, retry bounds, checkpoint failure, and crash/restart
recovery are already covered by the current modules.
