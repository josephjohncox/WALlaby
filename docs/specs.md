# Specs & Verification

WALlaby treats protocol specs and trace validation as first-class artifacts. The TLA+ models and trace validators are used to prevent drift between runtime behavior and the formal model.

## What’s in `specs/`

- **TLA+ models**: `CDCFlow.tla`, `FlowStateMachine.tla`, `CDCFlowFanout.tla`, and liveness/witness configs.
- **Coverage manifests**: `specs/coverage*.json` define the contract between spec actions/invariants and runtime trace actions.
- **Coverage output**: generated into `specs/coverage/` when running TLC with coverage enabled.

## Local workflow

Run a full spec + trace verification pass:

```bash
just tla                 # TLA+ model checks (flow, state machine, fan-out, liveness)
just tla-coverage         # action coverage from TLC
just tla-coverage-check   # enforce minimum coverage
just trace-suite          # randomized trace suite (CI-friendly)
just trace-suite-large    # larger randomized trace suite
```

Regenerate and verify the manifest contracts:

```bash
just spec-verify
just spec-sync
just spec-lint
```

## Trace validation

Runtime traces are emitted as JSON and validated against the spec using:

```bash
wallaby-trace-validate --path <trace.json>
```

The validator rejects unknown actions (not present in `specs/coverage*.json`) and enforces invariants mirrored from the spec. Validation state is isolated per flow. PostgreSQL LSNs use native hexadecimal ordering; decimal values are explicit abstract batch ordinals, and the two forms cannot be mixed within a flow. Durable checkpoints must follow delivery and precede source acknowledgement. Duplicate restore acknowledgements are allowed, while skipped or regressing checkpoints and acknowledgements are rejected.

## CI enforcement

CI runs:

- TLA+ model checks
- TLA+ coverage (and fails on zero-coverage actions)
- Trace suite coverage with a minimum action/invariant threshold

If you add or change behavior, update:

1) TLA+ models
2) `specs/coverage*.json`
3) trace-suite tests (so the behavior is exercised)
