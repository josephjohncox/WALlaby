# Specs & Verification

WALlaby treats protocol specs and trace validation as first-class artifacts. The TLA+ models and trace validators are used to prevent drift between runtime behavior and the formal model.

## What’s in `specs/`
- **TLA+ models**: `CDCFlow.tla`, `FlowStateMachine.tla`, `CDCFlowFanout.tla`, and liveness/witness configs.
- **Coverage manifests**: `specs/coverage*.json` define the contract between spec actions/invariants and runtime trace actions.
- **Coverage output**: generated into `specs/coverage/` when running TLC with coverage enabled.

## Local workflow
Run a full spec + trace verification pass:

```bash
make tla                 # TLA+ model checks (flow, state machine, fan-out, liveness)
make tla-coverage         # action coverage from TLC
make tla-coverage-check   # enforce minimum coverage
make trace-suite          # randomized trace suite (CI-friendly)
make trace-suite-large    # larger randomized trace suite
```

Regenerate and verify the manifest contracts:

```bash
make spec-verify
make spec-sync
make spec-lint
```

## Trace validation
Runtime traces are emitted as JSON and validated against the spec using:

```bash
wallaby-trace-validate -path <trace.json>
```

The validator rejects unknown actions (not present in `specs/coverage*.json`) and enforces invariants mirrored from the spec. This ensures runtime behavior stays aligned with the model.

## CI enforcement
CI runs:
- TLA+ model checks
- TLA+ coverage (and fails on zero-coverage actions)
- Trace suite coverage with a minimum action/invariant threshold

If you add or change behavior, update:
1) TLA+ models
2) `specs/coverage*.json`
3) trace-suite tests (so the behavior is exercised)
