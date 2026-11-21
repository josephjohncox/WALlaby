# Formal verification rules

Read the root guidance first. Models, action manifests and executable traces
must agree; passing a model is not live destination-delivery evidence.

- Keep Next actions, cfg constraints and pkg/spec manifests synchronized.
  Run `mise run spec-sync`, `mise run spec-verify` and `mise run spec-lint`.
- Register new actions/invariants and enforce nonzero TLC action coverage with
  `mise run tla-coverage` and `mise run tla-coverage-check`.
- ManagedDurability and ManagedPostgresDelivery are first-class models. Preserve
  their manifests, coverage gates and managed runtime Spec/SpecAction trace work.
- All eight models participate in spec-sync. DDLExecution explicitly separates
  observations from external truth; its IndeterminateFailsClosed invariant must
  kill permission-granting mutants. Run `mise run test-tla-mutations`.
- Managed-model no-skip/ownership adequacy and live managed trace linkage remain
  open obligations. Never infer them from action-name synchronization alone.
- Failure-matrix evidence exercises modeled protocols across real process death,
  not destination implementations. Keep model-only, OS-process and real-service
  evidence separate; preserve seeds, PID/fault/count/resource evidence and
  no-skip/vacuity checks.
- TLA+ tasks require Java. See README.md here for checked tool downloads and
  bounded runs. Never replace a missing TLC run with an unexplained skip.
