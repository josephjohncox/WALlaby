# Deductive-verification pilots

Both lanes target the **actual** `internal/reconcilepolicy/decision.go:Decide`
called by the managed delivery coordinator. Neither proves the coordinator or
external delivery. See `docs/development/protocol-verification.md` for the other
verification layers and live-service boundaries.

| Lane | Executed result | Boundary |
| --- | --- | --- |
| Gobra v26.09 + Silicon + Z3 4.16.0 | Baseline accepted; false postcondition and generated Reject→Wait mutant rejected | Gobra/Viper encoding, solver, comment lift/result naming; native macOS arm64 only |
| Goose/Perennial Lean at `e9988b5cde5907d9a66177ad7912c4d3f3729651` | Production translation drift check, accepted Iris WP theorem, standard axiom audit, rejected false-postcondition control | Conditional partial correctness under Goose semantics/interpreter and generated `FuncUnfold`; no adequacy or crash theorem |

## Subject and contract

The function quantifies over **every uint64** observation and both error flags.
Failure or observation 0 returns Wait=0; observation 1 without failure returns
Retry=1; observation 2 without failure returns Adopt=2; all other observations
without failure return Reject=3. Contracts state result range, error precedence,
indeterminate fail-closed behavior and soundness/completeness of all four actions.
The production enum is uint8; extending the theorem to uint64 does not establish
an additional reachable production state or any I/O guarantee.

Caller error wiring and distinct Retry/Adopt handling have separate Go tests and
overlay mutation controls. Those tests are not part of either deductive theorem.

## Gobra

```bash
mise run test-proof-runner
mise run proof-gobra-bootstrap
mise run proof-gobra
```

The bootstrap supports **macOS arm64 only**. It fetches checksum-pinned public
archives and exact source/submodule revisions into ignored `.cache/`, builds
Gobra if absent, then checks the recorded local assembly hash. There is no
upstream release-JAR claim. A non-byte-identical rebuild fails; do not update the
hash merely to make a build pass. This has not been validated as a portable CI
build. The runner accepts only mise's pinned `temurin-25.0.5+8.0.LTS` install
under the invoking account's default mise data directory. `JAVA_HOME` and Java
agent options cannot select or alter the verifier subprocess; subprocesses use a
small fixed environment. Z3's binary hash is checked before executing it. A
custom mise data directory fails closed. The Java installation itself remains
trusted user-managed content, checked by version rather than a pinned executable
hash. Java 25 worked here but is outside Gobra's documented Java 11/15 matrix.

`test-proof-runner` runs without Gobra or native solver tools and is enforced by
ordinary CI. It tests tool admission, subprocess environment isolation, tampered
solver rejection, pin consistency and the frozen production contract. These
unit tests do not establish the deductive result.

The runner lifts the source's `// @` contracts and names its single unnamed result
`res`. Executable tokens and the contract list must exactly match the frozen
subject. Generated files are not handwritten implementations. `--overflow` is
mandatory (the function itself performs no arithmetic). Missing tools, drift,
checksums, verifier failures, unexpected control success and tool failures all
fail the command; none is a skip. The negative controls must fail at their
specific postconditions. Evidence is `verification/gobra/out/evidence.json`.

Gobra is MPL-2.0. Its frontend/encoding, Silicon and Z3 are trusted; the pilot does
not verify the Go compiler. SBT/ANTLR pins are build provenance, not extra SMT
proofs. No subject assumptions, admissions or axioms are inserted.

## Goose/Perennial

Prerequisites: Go from mise and a public `elan` installation that supplies the
`lake`/`lean` shims. Install only the pinned local toolchain; do not change elan's
default:

```bash
mise run proof-goose-bootstrap
mise run proof-goose
```

`proof-goose-drift` rebuilds the pinned Goose translator/proof generator,
regenerates the production package into a temporary directory and compares the
result with the checked-in translation. **Translation alone is not proof.**
`proof-goose` additionally checks the source hash, builds the program-logic
`Decide.wp_proof`, audits `#print axioms`, then checks the false-property fixture.
Only `propext`, `Classical.choice`, and `Quot.sound` are allowed. The false call is
the actual generated `Decide(1,false)` with an Adopt=2 postcondition; Lean must
reject exactly its final `rfl` with `W64 1 = W64 2`. Import, tactic, dependency or
compiler failures do not count. The fixture is materialized under ignored
`artifacts/`, not part of the normal source build.

`lean-toolchain` pins Lean 4.34.1; `lake-manifest.json` pins Perennial, Iris,
batteries and Qq. The audit verifies exact dependency revisions and clean tracked
trees and records source/spec/proof/translation hashes. Evidence is
`verification/goose/artifacts/evidence.json` with result
`PROVED_CONDITIONAL_WP`. The native macOS arm64 parent run built 1,478 Lake jobs.
An earlier isolated lane had corrupted build artifacts/native setup crashes; a
fresh parent dependency build succeeded. No upstream tool patch was needed.
Linux and crash recovery were not verified by this pilot.

The theorem explicitly assumes the Perennial Goose semantics/interpreter,
heap/global contexts, `go.Semantics`, and the generated package `FuncUnfold`
linking the named call to its translated body. These are hypotheses, not hidden
axioms, and the axiom report does not discharge them. There is no adequacy theorem
connecting this WP to compiled Go or an external process, and no crash WP.

The translator's `goose/LICENSE` is MIT; Iris, batteries and Qq are Apache-2.0.
The pinned Perennial `lean` tree has no root LICENSE even though its historical
master tree has MIT text. Upstream trees are fetched, not vendored; do not infer
a clean SPDX declaration for that Lean tree from the historical branch.

## CI and scope

These opt-in proof tasks fail closed and are not mandatory promotion gates.
Normal Go CI does not pretend to run them or silently skip an unavailable prover.
Both executions are narrow pilots, not whole-system formal verification. SQL
fencing/atomicity, source ordering, external marker truth, Iggy replay,
retention, checkpoint I/O, Snowflake delivery and unbounded liveness remain
outside both theorems.
