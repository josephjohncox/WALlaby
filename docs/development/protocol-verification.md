# Implementation-linked protocol verification

Run the local, credential-free gate with:

```bash
mise run test-protocol
```

The `lint` CI job and `mise run ci-check-test` run this gate. A missing tool,
checker timeout, surviving mutant, compilation failure or vacuous required
witness fails it. A skipped service test is not a substitute.

## What is checked

| Layer | Subject | Claim |
| --- | --- | --- |
| Go bounded explorer | Production `stream.Runner.Run` and managed loop | Conditional conformance for the enumerated call-boundary fault plans |
| Go overlay mutants | Production Runner and coordinator reconciliation code | Ten deliberate implementation defects fail named properties |
| Porcupine 1.3.1 | Real SQLite checkpoint/outbox implementation | The recorded finite concurrent histories admit the sequential atomic-store contract |
| TLC 1.7.4 | Abstract DDL reconciliation model | Configured bounded state space satisfies its invariants |
| TLC safety mutants | Abstract DDL reconciliation model | Three unsafe changes produce specific safety counterexamples with nonzero unknown-outcome coverage |
| Manifest checks | All eight registered TLA models | Go/JSON action names and cfg invariant names agree; verification does not rewrite inputs |

These are not interchangeable proofs. In particular, checking an abstract model
is not proof that Go or SQL implements it. A finite linearizable history is not
a proof of every schedule, liveness or crash durability.

## Production Runner explorer

`pkg/stream/runner_protocol_explore_test.go` executes the real Runner. It does
not copy its dispatch/retry/feedback loop into another simulator. Go 1.27.4's
`testing/synctest` advances retry timers without wall-clock sleeps; it does not
enumerate all Go scheduler choices.

The fixed input contains three ordered transactions after an explicitly
validated initial slot cut:

1. One-fragment data transaction.
2. A data transaction filtered by the real projection call path, leaving no
   destination fragments and requiring authorization-only acknowledgement.
3. A two-fragment data transaction.

There are nine fault sites: read, dispatch and feedback for each transaction.
Each can inject a definite rejection, unknown-before, unknown-after, crash-after,
takeover or cancellation. The suite enumerates **1,315 plans**: zero, one or two
faults at distinct sites, excluding two-crash plans. At most one worker restart
is performed. Earlier terminal errors can make later planned faults unreachable;
those are not counted as executed faults. Every fault kind, retry, restart,
filtered path and stale rejection must execute at least once across the suite.

The checked properties include:

- **P1:** no next read before prior source feedback completes.
- **P2:** feedback uses the current committed grant, never an uncommitted or
  invented checkpoint.
- **P3:** an indeterminate delivery retries the same immutable identity without
  feedback or another transaction overtaking it.
- **P4:** a definite rejection terminates the run without further protocol calls.
- **P5:** filtered work is authorized without destination delivery; data is not
  authorized without delivery.
- **P6:** lineage, content, logical identity, revision and producer fence agree.
- **P7:** no new feedback after cancellation.
- **P8:** a successful bounded run settles every input, ends with checkpoint and
  flush at the last transaction, and has one target marker per data transaction.

### Assumptions and scope

The authority oracle, source and target are **test contracts**, not production
PostgreSQL or broker implementations:

- **A-SRC-ORDER:** source transactions are ordered and complete. The oracle knows
  the input sequence and rejects skips. LSNs themselves are not dense counters.
- **A-COORD:** the coordinator fences durable state, binds receipt/checkpoint/ACK
  intent atomically, and reconciles an already committed marker.
- **A-DEST:** the target's marker and effect are atomic/idempotent.
- Initial slot-cut validation and restored checkpoint reads obey their contracts.

Negative controls intentionally break authority fencing, source order and target
marker idempotence. Each must trip its own monitor. These controls check the
assertions, not the real adapters.

Faults occur at the exposed call boundaries, not arbitrary machine instructions,
network frames, SQL statements or power-loss points. The explorer does not prove
`delivery.Coordinator`, source-slot behaviour, SQL transaction realization,
retention, external drivers or all goroutine interleavings. The real-service
contract suites remain necessary.

## Production mutation gate

```bash
mise run test-protocol-mutations
```

`tools/protocolmutate` uses exact-once source anchors and Go `-overlay` files in
a temporary directory. It first requires a green unmodified baseline, then
requires each changed implementation to fail a specific property. Syntax/type
errors, tool failures and timeouts do not count as killed mutants. Source hashes
are recorded and a changed source during checking invalidates the evidence.

The ten mutants retry definite errors, accept unknown outcomes as success,
authorize data without delivery, deliver filtered work, ignore lineage, forge an
intent epoch, acknowledge before durable delivery, accept an unknown
reconciliation enum, ignore an error at the coordinator-to-policy call, or ignore
the policy error flag. The enum regression checks every invalid uint8 value
`3..255` against the actual coordinator method. The error-wiring test uses a
failing in-process connection dialer: it exercises the real coordinator's Wait
path without DNS, sockets or a PostgreSQL server.

Evidence is written to `.cache/protocol/mutations.json`. The gate does not edit,
reset or require a clean Git worktree.

## Real-store concurrent histories

`internal/checkpoint/linearizability_test.go` calls the actual SQLite store,
transactions and outbox encoder. Sixteen seeded input histories each contain
34 operations over two flows: a required witness prefix and three concurrent
clients. Operations cover checkpoint writes/reads, atomic checkpoint plus two
outbox entries, reads/deletions, duplicate publication, content conflicts,
regression rejection and rollback.

Invocation/return boundaries use a shared logical event counter, not cross-core
wall-clock timestamps. A barrier is outside those intervals. The independent
sequential model includes the checkpoint and pending outbox rows together.
Unknown storage errors are not treated as effect-free failures. Porcupine's
`Unknown`/timeout verdict fails the test; the checker budget is three seconds
per history. Failed synthetic histories are saved under `.cache/protocol/`.

Negative-control histories expose a checkpoint before its outbox and a regressing
checkpoint read. Both must be rejected. Input generation is seeded; real thread
scheduling is not exhaustive or deterministic. This checks SQLite only, not the
PostgreSQL implementation, filesystem fsync semantics or a complete CDC flow.

## Observation-aware DDL model

`specs/DDLExecution.tla` separates external truth from what the process knows.
Fresh preparation grants one execution permission. Crash loses in-memory
permission. A sound Absent observation may permit another attempt; Applied may
permit a receipt. Indeterminate is possible whether or not the effect committed
and grants neither permission.

**A-OBS-SOUND** assumes definite observations are truthful. `Apply` and
`RecordReceipt` consult knowledge/permission, not hidden destination truth.
`IndeterminateFailsClosed` is a checked state invariant, including a history
monitor for unsafe execution, not an implication whose antecedent already
contains the asserted `UNCHANGED` condition.

```bash
mise run test-tla-mutations
```

The gate requires a green baseline, nonzero `ReconcileIndeterminate` coverage,
and TLC safety exit **12** naming the expected invariant for each mutant.
Granting execution or receipt permission after an unknown observation must fail
`IndeterminateFailsClosed`; recording a receipt without observed application must
fail `ReceiptRequiresExternalCommit`. Counterexample logs and source/JAR hashes
are stored under `.cache/protocol/`.

## Remaining proof obligations

- The managed durability models still need stronger ownership-stamp and no-skip
  adequacy checks. Their current epoch inequalities and monotonic checkpoint
  guards do not independently establish those properties.
- The coordinator enforces non-regression, not predecessor contiguity. A stronger
  no-skip guarantee would need a predecessor identity/source-contract decision,
  not a test that assumes adjacent numeric LSNs.
- Managed runtime traces do not yet expose every prepare/reconcile/finalize,
  owner-stamp, feedback-receipt and retention commit. A coarse successful trace
  is not implementation refinement evidence.
- The OS-process failure matrix remains a detached protocol model. Real child
  death exercises its persistence/CAS mechanics, not destination delivery code.
- Live PostgreSQL/source/broker/storage checks remain separate and credential- or
  service-gated. No local service guarantee follows from this gate.

## Deductive-verification pilots

Two executed pilots target the actual production
`internal/reconcilepolicy/decision.go:Decide`, called by the managed delivery
coordinator. Both contracts cover all uint64 observations and both error flags:
result range, error precedence, indeterminate fail-closed behavior and
soundness/completeness of Wait, Retry, Adopt and Reject.

| Task | Executed evidence | Limits |
| --- | --- | --- |
| `mise run proof-gobra` | Gobra v26.09 / Silicon / Z3 4.16.0 accepted the source-linked contract with `--overflow`; false postcondition and Reject→Wait mutant rejected | Native macOS arm64, pinned local source assembly; frontend/encoding/SMT trust |
| `mise run proof-goose` | Regenerated Goose output matched; Lean 4.34.1 accepted `Decide.wp_proof`; only standard axioms; concrete Retry-versus-Adopt false postcondition rejected | Conditional Iris WP under Goose semantics and generated FuncUnfold; no adequacy or crash theorem |

Prerequisites, exact pins, source linkage, evidence paths, license limits and
reproduction commands are in
[verification/README.md](https://github.com/josephjohncox/WALlaby/blob/main/verification/README.md).
Gobra names the production function's single unnamed result in its generated
input but changes no executable body token. Goose translates the production
package; the theorem calls that generated function, not a handwritten substitute.
The Lean axiom audit allows only `propext`, `Classical.choice` and `Quot.sound`.
Typeclass/semantics hypotheses remain explicit and are not discharged by that
audit.

These are opt-in pilots, not mandatory promotion gates. A requested task fails
on missing tools, drift, wrong revisions, unexpected controls or verifier errors.
It does not skip. Neither theorem covers caller error wiring, SQL fencing,
checkpoint I/O, pgx/gRPC/cgo, external marker truth, Iggy replay or Snowflake.
Caller wiring has separate Go tests and overlay mutation controls. Porcupine
remains a bounded history checker, not a deductive prover.

[Live delivery harnesses](live-delivery.md) provision kind PostgreSQL and Compose
Iggy through Go executors. Their required-test JSON accounting establishes
different, real-service scenarios; it is not an implementation-refinement proof.
