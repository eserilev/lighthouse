import EpochProofs.Sanity.SinglePass

/-!
# Pass congruence

If two steps agree on every row of a list, the two passes over that list agree. The row
conditions of the Lighthouse step hold for each row of the state, so this lifts the row theorem
to the whole pass.
-/

namespace EpochProofs.Spec

theorem passM_congr {A R : Type} (f g : A → R → SpecM (A × R)) (rows : List R)
    (h : ∀ a r, r ∈ rows → f a r = g a r) (a : A) : passM f a rows = passM g a rows := by
  induction rows generalizing a with
  | nil => rfl
  | cons r rs ih =>
    simp only [passM_cons]
    rw [h a r List.mem_cons_self]
    congr 1
    funext x
    rw [ih (fun a r hr => h a r (List.mem_cons_of_mem _ hr))]

theorem ejection_lt_activation_mainnet :
    Preset.mainnet.EJECTION_BALANCE < Preset.mainnet.MIN_ACTIVATION_BALANCE := by decide

theorem ejection_lt_activation_minimal :
    Preset.minimal.EJECTION_BALANCE < Preset.minimal.MIN_ACTIVATION_BALANCE := by decide

end EpochProofs.Spec
