import EpochProofs.Spec.Slashings
import EpochProofs.Sanity.InactivityUpdates

/-!
# Sanity theorems for the `process_slashings` reference

Facts about the reference alone. If a transcription error breaks a fact, the build fails.

The Lighthouse proof targets `slashingsPreamble` and `slashingBalanceStep`.
`process_slashings_eq` shows that the loop applies the step to each validator.
-/

namespace EpochProofs.Spec

/-- The values the spec computes before the loop: the adjusted total slashing balance and the
penalty per effective balance increment. -/
def slashingsPreamble (p : Preset) (total_active_balance : Gwei) (slashings : List Gwei) :
    SpecM (Gwei × Gwei) := do
  let adjusted_total_slashing_balance :=
    min (← uint64Mul (← uint64Sum slashings) p.PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX)
      total_active_balance
  let penalty_per_effective_balance_increment ← uint64Div adjusted_total_slashing_balance
    (← uint64Div total_active_balance p.EFFECTIVE_BALANCE_INCREMENT)
  pure (adjusted_total_slashing_balance, penalty_per_effective_balance_increment)

/-- The new balance of one validator, for a given target epoch and penalty per increment. -/
def slashingBalanceStep (p : Preset) (target penalty_per_increment : Uint64)
    (validator : Validator) (balance : Gwei) : SpecM Gwei := do
  if validator.slashed && target == validator.withdrawable_epoch then
    let effective_balance_increments ←
      uint64Div validator.effective_balance p.EFFECTIVE_BALANCE_INCREMENT
    let penalty ← uint64Mul penalty_per_increment effective_balance_increments
    pure (saturating_sub balance penalty)
  else
    pure balance

/-- One iteration of the loop, with the target epoch already known. -/
def slashingLoopStep (p : Preset) (target penalty_per_increment : Uint64)
    (balances : List Gwei) (x : Validator × Nat) : SpecM (List Gwei) := do
  if x.1.slashed && target == x.1.withdrawable_epoch then
    let effective_balance_increments ←
      uint64Div x.1.effective_balance p.EFFECTIVE_BALANCE_INCREMENT
    let penalty ← uint64Mul penalty_per_increment effective_balance_increments
    decrease_balance balances x.2 penalty
  else
    pure balances

theorem process_slashings_eq (p : Preset) (total_active_balance : Gwei) (state : BeaconState)
    (epoch : Epoch) (hepoch : get_current_epoch p state = .ok epoch)
    (htarget : epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2 < UINT64_SIZE) :
    process_slashings p total_active_balance state =
      (do
        let (_, penalty_per_increment) ←
          slashingsPreamble p total_active_balance state.slashings
        let balances ← state.validators.zipIdx.foldlM
          (slashingLoopStep p (epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2) penalty_per_increment)
          state.balances
        pure { state with balances }) := by
  have hdiv : uint64Div p.EPOCHS_PER_SLASHINGS_VECTOR 2 = .ok (p.EPOCHS_PER_SLASHINGS_VECTOR / 2) :=
    by simp [uint64Div, pure, Except.pure]
  have hadd : uint64Add epoch (p.EPOCHS_PER_SLASHINGS_VECTOR / 2) =
      .ok (epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2) := by
    simp [uint64Add, htarget, pure, Except.pure]
  unfold process_slashings slashingsPreamble
  simp only [bind, Except.bind, pure, Except.pure, hepoch]
  cases uint64Sum state.slashings with
  | error e => rfl
  | ok total =>
    simp only
    cases uint64Mul total p.PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX with
    | error e => rfl
    | ok scaled =>
      simp only
      cases uint64Div total_active_balance p.EFFECTIVE_BALANCE_INCREMENT with
      | error e => rfl
      | ok increments =>
        simp only
        cases uint64Div (min scaled total_active_balance) increments with
        | error e => rfl
        | ok per =>
          simp only
          rw [forIn_yield_eq_foldlM
            (step := slashingLoopStep p (epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2) per)]
          rintro ⟨validator, index⟩ balances
          simp only [slashingLoopStep, hdiv, hadd, bind, Except.bind, pure, Except.pure]
          by_cases hs : validator.slashed = true
          · simp only [hs, if_true, Bool.true_and]
            split
            · cases uint64Div validator.effective_balance p.EFFECTIVE_BALANCE_INCREMENT with
              | error e => rfl
              | ok n =>
                simp only
                cases uint64Mul per n with
                | error e => rfl
                | ok penalty =>
                  simp only
                  cases decrease_balance balances index penalty <;> rfl
            · rfl
          · simp [hs]

/-- A validator that is not slashed keeps its balance. -/
theorem slashingBalanceStep_not_slashed (p : Preset) (target per : Uint64)
    (validator : Validator) (balance : Gwei) (h : validator.slashed = false) :
    slashingBalanceStep p target per validator balance = .ok balance := by
  simp [slashingBalanceStep, h, pure, Except.pure]

/-- The penalty never raises a balance. -/
theorem slashingBalanceStep_le (p : Preset) (target per : Uint64) (validator : Validator)
    (balance new_balance : Gwei)
    (h : slashingBalanceStep p target per validator balance = .ok new_balance) :
    new_balance ≤ balance := by
  unfold slashingBalanceStep at h
  split at h
  · simp only [bind, Except.bind, pure, Except.pure] at h
    split at h
    · cases h
    · split at h
      · cases h
      · simp only [Except.ok.injEq] at h
        rw [← h]; unfold saturating_sub; split <;> exact Nat.sub_le _ _
  · simp only [pure, Except.pure, Except.ok.injEq] at h
    rw [h]
    exact Nat.le_refl _

end EpochProofs.Spec
