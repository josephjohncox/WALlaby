/- Program-logic proof candidate for the generated production Decide call.
The checker, not this comment, determines whether the theorem is accepted. -/
module

public import WallabyGoose.Spec.Decide

@[expose] public section
noncomputable section

namespace Perennial
open Iris Iris.BI Iris.ProgramLogic Iris.Std Iris.ProofMode
namespace github_com.josephjohncox.wallaby.internal.reconcilepolicy

section proof
variable [GooseExts] [GooseSemantics] [GooseInterp]
variable [go_gctx : GoGlobalContext]
variable {hlc : HasLC} {GF : BundledGFunctors} [hG : HeapGS hlc GF]
variable [sem : go.Semantics] [package_sem : Assumptions]

private theorem word_val_eq (a b : w64) : (#a : val) = #b ↔ a = b := by
  constructor
  · intro h
    exact GoGlobalContext.intoVal_inj_w64 h
  · intro h
    subst b
    rfl

private theorem post_error (observation : w64) : DecidePost observation (W64 0) true := by
  simp [DecidePost, Wait, Retry, Adopt, Reject, word_val_eq, W64, uint.Z]

private theorem post_retry : DecidePost (W64 1) (W64 1) false := by
  simp [DecidePost, Wait, Retry, Adopt, Reject, word_val_eq, W64, uint.Z]

private theorem post_adopt : DecidePost (W64 2) (W64 2) false := by
  simp [DecidePost, Wait, Retry, Adopt, Reject, word_val_eq, W64, uint.Z]

private theorem post_indeterminate : DecidePost (W64 0) (W64 0) false := by
  simp [DecidePost, Wait, Retry, Adopt, Reject, word_val_eq, W64, uint.Z]

private theorem post_reject (observation : w64)
    (h0 : observation ≠ W64 0) (h1 : observation ≠ W64 1) (h2 : observation ≠ W64 2) :
    DecidePost observation (W64 3) false := by
  simp_all [DecidePost, Wait, Retry, Adopt, Reject, word_val_eq, W64, uint.Z]

/-- Full-word soundness/completeness of the actual generated function call.
The Goose semantics and generated FuncUnfold assumptions remain explicit. -/
theorem Decide.wp_proof : Decide.spec (GF := GF) (hG := hG) := by
  intro observation failed
  cases failed
  · wp_start
    simp only [Wait, Retry, Adopt, Reject]
    wp_auto
    wp_if_destruct
    · wp_end
      ipureintro
      exact post_retry
    · rename_i hNot1
      wp_if_destruct
      · wp_end
        ipureintro
        exact post_adopt
      · rename_i hNot2
        wp_if_destruct
        · wp_end
          ipureintro
          exact post_indeterminate
        · rename_i hNot0
          wp_end
          ipureintro
          exact post_reject observation (by assumption) (by assumption) (by assumption)
  · wp_start
    simp only [Wait, Retry, Adopt, Reject]
    wp_auto
    wp_end
    ipureintro
    exact post_error observation

end proof
end github_com.josephjohncox.wallaby.internal.reconcilepolicy
end Perennial
