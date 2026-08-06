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

## Lifecycle Generation Spec (TLA+)

File: `specs/LifecycleGeneration.tla`

What it models:

- Generation-scoped execution registration and leases.
- Pause intent while the public state remains `Running` until execution quiesces.
- Two-phase stop through `Stopping`.
- Terminal-state quiescence, generation matching, and stopped-state finality.

Run TLC:

```
TLA_MODULE=specs/LifecycleGeneration.tla TLA_CONFIG=specs/LifecycleGeneration.cfg just tla-single
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

## Snapshot Transition Spec (TLA+)

File: `specs/SnapshotTransition.tla`

What it models:

- Partition assignment and durable per-partition coverage.
- Crash loss of volatile rows followed by replay from durable state.
- A snapshot-to-stream transition only after every source row is durable.
- Streaming start at the exported snapshot boundary.

Run TLC:

```
TLA_MODULE=specs/SnapshotTransition.tla TLA_CONFIG=specs/SnapshotTransition.cfg just tla-single
```

## DDL Execution Receipt Spec (TLA+)

File: `specs/DDLExecution.tla`

What it models:

- Session-scoped execution locks around attempt, downstream DDL, batch write, and receipt persistence.
- Durable attempt preparation before any downstream DDL side effect.
- Crashes before and after the downstream commit.
- Destination reconciliation after restart.
- Receipt persistence only after a confirmed external commit.
- Fail-closed indeterminate reconciliation and at-most-once external application.

Run TLC:

```
TLA_MODULE=specs/DDLExecution.tla TLA_CONFIG=specs/DDLExecution.cfg just tla-single
```

## Managed Durability Spec (TLA+)

File: `specs/ManagedDurability.tla`

What it models:

- producer takeover and lease-epoch ownership;
- durable attempts before external target commits;
- atomic destination receipt, checkpoint, and source ACK-intent publication;
- atomic artifact root, delivery rows, checkpoint, and source ACK-intent publication;
- source ACK safety;
- orphan collection that cannot delete an active abstract PostgreSQL root; and
- abstract root release only after source ACK, one modeled delivery-completion bit, and a newer checkpoint.

This model does **not** represent elapsed time or retention eligibility, delivery-row cardinality, multi-object publications, partial object release, exact S3 versions, or the worker startup recovery path. `AuthorizeInitialCut` models only checkpoint/ACK authorization for an object-free cut. `ManagedDurability` is checked by `just check-tla`; its nonzero action coverage is enforced by `just tla-coverage-check` (it is generated into `specs/coverage/ManagedDurability.txt` by `just tla-coverage`); its manifest (`specs/coverage.managed_durability.json`) is registered in `pkg/spec` and validated for Next-block/cfg drift by `just spec-sync`. It is not part of the CDCFlow trace coverage manifests; instead its safety invariants are mirrored as executable runtime checks by the deterministic process-failure matrix in `internal/failmatrix` (see [Process-failure matrix and soak](../docs/development/failure-matrix.md)).

Run TLC:

```
TLA_MODULE=specs/ManagedDurability.tla TLA_CONFIG=specs/ManagedDurability.cfg just tla-single
```

## Managed PostgreSQL Delivery Spec (TLA+)

File: `specs/ManagedPostgresDelivery.tla`

What it models:

- bounded per-logical-batch delivery and reconciliation attempts;
- target-marker reconciliation followed by fenced receipt/checkpoint/ACK intent finalization;
- separate authorized source flush and observed-receipt steps, including takeover or crash between them and idempotent repair;
- stale-owner rejection; and
- retention roots that cannot prune the current checkpoint.

Run TLC:

```
TLA_MODULE=specs/ManagedPostgresDelivery.tla TLA_CONFIG=specs/ManagedPostgresDelivery.cfg just tla-single
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
- `specs/coverage.ddl_execution.json` (DDLExecution)
- `specs/coverage.lifecycle_generation.json` (LifecycleGeneration)
- `specs/coverage.snapshot_transition.json` (SnapshotTransition)
- `specs/coverage.managed_durability.json` (ManagedDurability / ArtifactPublication)
- `specs/coverage.managed_postgres_delivery.json` (ManagedPostgresDelivery / SourceFeedback)

Regenerate them with:

```
just spec-manifest
```

To ensure the manifests stay in sync with the TLA+ `Next` blocks and config
invariants, run:

```
just spec-sync
```

`spec-sync` validates CDCFlow, FlowStateMachine, CDCFlowFanout, DDLExecution, and
both managed models (ManagedDurability, ManagedPostgresDelivery). LifecycleGeneration
and SnapshotTransition use a distinct action-naming layer in their manifests and are
reconciled separately, so they are not synced by this tool yet.

Known formal follow-up: `DDLExecution.tla` defines `IndeterminateFailsClosed` as an
action predicate (it references `UNCHANGED vars`), so it cannot be checked as a TLC
state invariant and is intentionally absent from `DDLExecution.cfg`. The managed
models cover the indeterminate-fail-closed behavior through `ReconcileIndeterminate`
plus bounded retries, and it is mirrored as an executable check
(`adopted_indeterminate_effect`) in `internal/failmatrix`.

Static analysis (`just spec-lint`) enforces that `SpecAction` values in code are
constants from the manifest.

To emit traces from a worker run, set `WALLABY_TRACE_PATH` (supports `{flow_id}`
placeholder) and run `wallaby-worker`. For the main server, `{flow_id}` is
replaced with `server`. Then validate (defaults to `specs/coverage.json`):

```
wallaby-trace-validate --input /path/to/trace.jsonl
```

## Model Boundaries

The current modules cover fan-out, generation-fenced lifecycle leases, DDL gating,
DDL receipt recovery, partitioned snapshot durability, snapshot-to-stream handoff,
retry bounds, checkpoint failure, and crash/restart recovery. Destination-specific
schema semantics remain executable Go contracts rather than TLA+ constants.
