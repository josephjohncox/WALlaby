/-
Specification of the generated `reconcilepolicy.Decide`.

The postcondition is the required decision policy on the whole `uint64` domain
(`w64` = `BitVec 64`), not a bound such as `0 ≤ n ≤ 255` and not equality to a
separately maintained implementation. `Wait` / `Retry` / `Adopt` / `Reject` are
the constants goose emitted from the production package.
-/
module

public import WallabyGoose.Code.github_com.josephjohncox.wallaby.internal.reconcilepolicy
public import Perennial.Golang.Theory

@[expose] public section

noncomputable section

namespace Perennial

open Iris Iris.BI Iris.ProgramLogic Iris.Std

namespace github_com.josephjohncox.wallaby.internal.reconcilepolicy

section spec
variable [GooseExts] [GooseSemantics] [GooseInterp]
variable [go_gctx : GoGlobalContext]
variable {hlc : HasLC} {GF : BundledGFunctors} [hG : HeapGS hlc GF]
variable [sem : go.Semantics]
variable [package_sem : Assumptions]

/-- Soundness and completeness of each result, the result range `0..3`,
error precedence, indeterminate fail-closed behaviour, and rejection of every
other `uint64`. Comparisons are word equality (`GoEquals` on `go.uint64`);
the production body has no arithmetic operator. -/
def DecidePost (observation r : w64) (failed : Bool) : Prop :=
  (r = W64 0 ∨ r = W64 1 ∨ r = W64 2 ∨ r = W64 3) ∧
  (uint.Z r = 0 ∨ uint.Z r = 1 ∨ uint.Z r = 2 ∨ uint.Z r = 3) ∧
  (failed = true → #r = Wait ∧ r ≠ W64 1 ∧ r ≠ W64 2 ∧ r ≠ W64 3) ∧
  (observation = W64 0 → #r = Wait) ∧
  (failed = false →
    observation ≠ W64 0 → observation ≠ W64 1 → observation ≠ W64 2 → #r = Reject) ∧
  (#r = Wait ↔ failed = true ∨ failed = false ∧ observation = W64 0) ∧
  (#r = Retry ↔ failed = false ∧ observation = W64 1) ∧
  (#r = Adopt ↔ failed = false ∧ observation = W64 2) ∧
  (#r = Reject ↔
    failed = false ∧ observation ≠ W64 0 ∧ observation ≠ W64 1 ∧ observation ≠ W64 2)

/-- Partial correctness of the goose translation of `Decide`. The call is the
generated function name; `[Assumptions]` is goose's `FuncUnfold` linking that
name to `Decide.impl`. -/
abbrev Decide.spec : Prop :=
  ∀ (observation : w64) (failed : Bool),
    {{ (True : IProp GF) }}
      (App (App (Val (@! Decide)) (Val #observation)) (Val #failed))
    {{ (r : w64), RET #r; ⌜DecidePost observation r failed⌝ }}

end spec

end github_com.josephjohncox.wallaby.internal.reconcilepolicy

end Perennial
