import EpochProofs.Spec.EffectiveBalanceUpdates

/-!
# Sanity theorems for the `process_effective_balance_updates` reference

Facts about the reference alone. If a transcription error breaks a fact, the build fails.

The Lighthouse proof targets `hysteresisThresholds` and `newEffectiveBalance`.
`process_effective_balance_updates_eq` shows that the loop is a `mapM` of these per validator.
-/

namespace EpochProofs.Spec

/-- The three threshold lines of the loop body. -/
def hysteresisThresholds (p : Preset) : SpecM (Uint64 × Uint64) := do
  let HYSTERESIS_INCREMENT ← uint64Div p.EFFECTIVE_BALANCE_INCREMENT p.HYSTERESIS_QUOTIENT
  let DOWNWARD_THRESHOLD ← uint64Mul HYSTERESIS_INCREMENT p.HYSTERESIS_DOWNWARD_MULTIPLIER
  let UPWARD_THRESHOLD ← uint64Mul HYSTERESIS_INCREMENT p.HYSTERESIS_UPWARD_MULTIPLIER
  pure (DOWNWARD_THRESHOLD, UPWARD_THRESHOLD)

/-- The new effective balance of one validator, for given thresholds. -/
def newEffectiveBalance (p : Preset) (downward_threshold upward_threshold : Uint64)
    (validator : Validator) (balance : Gwei) : SpecM Gwei := do
  let below ← uint64Add balance downward_threshold
  let update ← if below < validator.effective_balance then pure true
    else do
      let above ← uint64Add validator.effective_balance upward_threshold
      pure (decide (above < balance))
  if update then
    let remainder ← uint64Mod balance p.EFFECTIVE_BALANCE_INCREMENT
    let rounded ← uint64Sub balance remainder
    pure (min rounded (get_max_effective_balance p validator))
  else pure validator.effective_balance

/-- One iteration of the loop. -/
def effectiveBalanceStep (p : Preset) (balances : List Gwei) (x : Validator × Nat) :
    SpecM Validator := do
  let balance ← match balances[x.2]? with
    | some balance => pure balance
    | none => throw .indexOutOfRange
  let (downward_threshold, upward_threshold) ← hysteresisThresholds p
  let effective_balance ← newEffectiveBalance p downward_threshold upward_threshold x.1 balance
  pure { x.1 with effective_balance }

theorem forIn_append_eq_mapM {α β : Type} (l : List α) (acc : List β)
    (body : α → List β → SpecM (ForInStep (List β))) (step : α → SpecM β)
    (h : ∀ x r, body x r = (step x).map fun v => ForInStep.yield (r ++ [v])) :
    forIn l acc body = (l.mapM step).map (acc ++ ·) := by
  induction l generalizing acc with
  | nil => simp [forIn, ForIn.forIn, pure, Except.pure, Except.map]
  | cons x xs ih =>
    rw [List.forIn_cons, h]
    cases hs : step x with
    | error e => simp [Except.map, bind, Except.bind, List.mapM_cons, hs]
    | ok v =>
      simp only [Except.map, bind, Except.bind, ih, List.mapM_cons, hs, pure, Except.pure]
      cases xs.mapM step <;> simp

theorem process_effective_balance_updates_eq (p : Preset) (state : BeaconState) :
    process_effective_balance_updates p state =
      (state.validators.zipIdx.mapM (effectiveBalanceStep p state.balances)).map
        fun validators => { state with validators } := by
  unfold process_effective_balance_updates
  simp only [bind, Except.bind, pure, Except.pure]
  rw [forIn_append_eq_mapM (step := effectiveBalanceStep p state.balances)]
  · cases state.validators.zipIdx.mapM (effectiveBalanceStep p state.balances) <;> rfl
  · rintro ⟨validator, index⟩ r
    simp only [effectiveBalanceStep, hysteresisThresholds, newEffectiveBalance, bind,
      Except.bind, pure, Except.pure, Except.map]
    cases state.balances[index]? with
    | none => rfl
    | some balance =>
      simp only
      cases uint64Div p.EFFECTIVE_BALANCE_INCREMENT p.HYSTERESIS_QUOTIENT with
      | error e => rfl
      | ok increment =>
        simp only
        cases uint64Mul increment p.HYSTERESIS_DOWNWARD_MULTIPLIER with
        | error e => rfl
        | ok downward =>
          simp only
          cases uint64Mul increment p.HYSTERESIS_UPWARD_MULTIPLIER with
          | error e => rfl
          | ok upward =>
            simp only
            cases uint64Add balance downward with
            | error e => rfl
            | ok below =>
              simp only
              split
              · simp only [if_true]
                cases uint64Mod balance p.EFFECTIVE_BALANCE_INCREMENT with
                | error e => rfl
                | ok remainder =>
                  simp only
                  cases uint64Sub balance remainder <;> rfl
              · cases uint64Add validator.effective_balance upward with
                | error e => rfl
                | ok above =>
                  simp only
                  split
                  · cases uint64Mod balance p.EFFECTIVE_BALANCE_INCREMENT with
                    | error e => rfl
                    | ok remainder =>
                      simp only
                      cases uint64Sub balance remainder <;> rfl
                  · rfl

/-- Mainnet thresholds: 0.25 ETH down, 1.25 ETH up. -/
theorem hysteresisThresholds_mainnet :
    hysteresisThresholds Preset.mainnet = .ok (250000000, 1250000000) := by
  rfl

/-- A changed effective balance never exceeds `get_max_effective_balance`. -/
theorem newEffectiveBalance_le_max (p : Preset) (downward upward : Uint64)
    (validator : Validator) (balance effective_balance : Gwei)
    (h : newEffectiveBalance p downward upward validator balance = .ok effective_balance)
    (hchanged : effective_balance ≠ validator.effective_balance) :
    effective_balance ≤ get_max_effective_balance p validator := by
  simp only [newEffectiveBalance, uint64Add, uint64Mod, uint64Sub, bind, Except.bind, pure,
    Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  repeat' split at h
  all_goals first
    | (simp at h; done)
    | (simp at h; subst h; exact Nat.min_le_right _ _)
    | (simp at h; exact absurd h.symm hchanged)

/-- Inside the hysteresis band, the effective balance does not change. -/
theorem newEffectiveBalance_in_band (p : Preset) (downward upward : Uint64)
    (validator : Validator) (balance : Gwei)
    (hdown : validator.effective_balance ≤ balance + downward)
    (hup : balance ≤ validator.effective_balance + upward)
    (hfit1 : balance + downward < UINT64_SIZE)
    (hfit2 : validator.effective_balance + upward < UINT64_SIZE) :
    newEffectiveBalance p downward upward validator balance = .ok validator.effective_balance := by
  have h1 : ¬ balance + downward < validator.effective_balance := Nat.not_lt.mpr hdown
  have h2 : ¬ validator.effective_balance + upward < balance := Nat.not_lt.mpr hup
  simp [newEffectiveBalance, uint64Add, hfit1, hfit2, h1, h2, bind, Except.bind, pure,
    Except.pure]

end EpochProofs.Spec
