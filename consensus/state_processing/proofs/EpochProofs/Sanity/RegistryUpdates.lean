import EpochProofs.Spec.RegistryUpdates

/-!
# Sanity theorems for the `process_registry_updates` reference

`exitChurnStep` is `compute_exit_epoch_and_update_churn` on the two churn fields.
`registryStepExclusive` is the loop body for one validator: the spec's `if`/`elif`/`elif`.

Lighthouse runs three independent steps instead (`registryStepIndependent`).
`registryStepIndependent_eq_exclusive` shows that the two agree on valid states.
-/

namespace EpochProofs.Spec

/-- The values that a registry update reads and can change. -/
structure RegistryFields where
  activation_eligibility_epoch : Epoch
  activation_epoch : Epoch
  exit_epoch : Epoch
  withdrawable_epoch : Epoch
  earliest_exit_epoch : Epoch
  exit_balance_to_consume : Gwei
  deriving DecidableEq, Repr

/-- `compute_exit_epoch_and_update_churn` on the two churn fields. Returns the exit epoch, the
new earliest exit epoch and the new exit balance to consume. -/
def exitChurnStep (p : Preset) (total_active_balance current_epoch : Nat) (exit_balance : Gwei)
    (earliest_exit_epoch_state exit_balance_to_consume_state : Nat) :
    SpecM (Epoch × Epoch × Gwei) := do
  let mut earliest_exit_epoch := max earliest_exit_epoch_state
    (← compute_activation_exit_epoch p current_epoch)
  let per_epoch_churn ← get_exit_churn_limit p total_active_balance
  let mut exit_balance_to_consume :=
    if earliest_exit_epoch_state < earliest_exit_epoch then per_epoch_churn
    else exit_balance_to_consume_state
  if exit_balance > exit_balance_to_consume then
    let balance_to_process ← uint64Sub exit_balance exit_balance_to_consume
    let additional_epochs ←
      uint64Add (← uint64Div (← uint64Sub balance_to_process 1) per_epoch_churn) 1
    earliest_exit_epoch ← uint64Add earliest_exit_epoch additional_epochs
    exit_balance_to_consume ←
      uint64Add exit_balance_to_consume (← uint64Mul additional_epochs per_epoch_churn)
  pure (earliest_exit_epoch, earliest_exit_epoch,
    ← uint64Sub exit_balance_to_consume exit_balance)

/-- The exit branch: `initiate_validator_exit` on the fields. -/
def exitStep (p : Preset) (total_active_balance current_epoch : Nat) (effective_balance : Gwei)
    (f : RegistryFields) : SpecM RegistryFields := do
  if f.exit_epoch != FAR_FUTURE_EPOCH then
    return f
  let (exit_epoch, earliest_exit_epoch, exit_balance_to_consume) ←
    exitChurnStep p total_active_balance current_epoch effective_balance f.earliest_exit_epoch
      f.exit_balance_to_consume
  pure { f with
    exit_epoch
    withdrawable_epoch := ← uint64Add exit_epoch p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY
    earliest_exit_epoch
    exit_balance_to_consume }

/-- The spec loop body for one validator. -/
def registryStepExclusive (p : Preset) (total_active_balance current_epoch finalized_epoch
    activation_epoch : Nat) (effective_balance : Gwei) (f : RegistryFields) :
    SpecM RegistryFields := do
  if f.activation_eligibility_epoch == FAR_FUTURE_EPOCH
      && effective_balance ≥ p.MIN_ACTIVATION_BALANCE then
    pure { f with activation_eligibility_epoch := ← uint64Add current_epoch 1 }
  else if (f.activation_epoch ≤ current_epoch && current_epoch < f.exit_epoch)
      && effective_balance ≤ p.EJECTION_BALANCE then
    exitStep p total_active_balance current_epoch effective_balance f
  else if f.activation_eligibility_epoch ≤ finalized_epoch
      && f.activation_epoch == FAR_FUTURE_EPOCH then
    pure { f with activation_epoch }
  else
    pure f

def eligibilityStep (p : Preset) (current_epoch : Nat) (effective_balance : Gwei)
    (f : RegistryFields) : SpecM RegistryFields := do
  if f.activation_eligibility_epoch == FAR_FUTURE_EPOCH
      && effective_balance ≥ p.MIN_ACTIVATION_BALANCE then
    pure { f with activation_eligibility_epoch := ← uint64Add current_epoch 1 }
  else
    pure f

def ejectionStep (p : Preset) (total_active_balance current_epoch : Nat)
    (effective_balance : Gwei) (f : RegistryFields) : SpecM RegistryFields := do
  if (f.activation_epoch ≤ current_epoch && current_epoch < f.exit_epoch)
      && effective_balance ≤ p.EJECTION_BALANCE then
    exitStep p total_active_balance current_epoch effective_balance f
  else
    pure f

def activationStep (p : Preset) (current_epoch finalized_epoch : Nat) (f : RegistryFields) :
    SpecM RegistryFields := do
  if f.activation_eligibility_epoch ≤ finalized_epoch
      && f.activation_epoch == FAR_FUTURE_EPOCH then
    pure { f with activation_epoch := ← compute_activation_exit_epoch p current_epoch }
  else
    pure f

/-- Lighthouse: the three steps one after the other. -/
def registryStepIndependent (p : Preset) (total_active_balance current_epoch finalized_epoch : Nat)
    (effective_balance : Gwei) (f : RegistryFields) : SpecM RegistryFields := do
  let f ← eligibilityStep p current_epoch effective_balance f
  let f ← ejectionStep p total_active_balance current_epoch effective_balance f
  activationStep p current_epoch finalized_epoch f

theorem exitStep_activation (p : Preset) (total_active_balance current_epoch : Nat)
    (effective_balance : Gwei) (f g : RegistryFields)
    (h : exitStep p total_active_balance current_epoch effective_balance f = .ok g) :
    g.activation_epoch = f.activation_epoch ∧
      g.activation_eligibility_epoch = f.activation_eligibility_epoch := by
  unfold exitStep at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · cases h; exact ⟨rfl, rfl⟩
  · split at h
    · cases h
    · split at h
      · cases h
      · cases h; exact ⟨rfl, rfl⟩

/-- On valid states, Lighthouse's three independent steps equal the spec's `if`/`elif`/`elif`.
The conditions: an ejected validator cannot also be eligible for the queue, the finalized epoch
is not in the future, and the epochs fit in a `u64`. -/
theorem registryStepIndependent_eq_exclusive (p : Preset)
    (total_active_balance current_epoch finalized_epoch activation_epoch : Nat)
    (effective_balance : Gwei) (f : RegistryFields)
    (hbalances : p.EJECTION_BALANCE < p.MIN_ACTIVATION_BALANCE)
    (hfinalized : finalized_epoch ≤ current_epoch)
    (hexit : f.exit_epoch ≤ FAR_FUTURE_EPOCH)
    (hcurrent : current_epoch + 1 < UINT64_SIZE)
    (hactivation : compute_activation_exit_epoch p current_epoch = .ok activation_epoch) :
    registryStepIndependent p total_active_balance current_epoch finalized_epoch
        effective_balance f =
      registryStepExclusive p total_active_balance current_epoch finalized_epoch
        activation_epoch effective_balance f := by
  have hinc : uint64Add current_epoch 1 = .ok (current_epoch + 1) := by
    simp [uint64Add, hcurrent, pure, Except.pure]
  unfold registryStepIndependent registryStepExclusive
  by_cases helig : f.activation_eligibility_epoch = FAR_FUTURE_EPOCH
      ∧ p.MIN_ACTIVATION_BALANCE ≤ effective_balance
  · -- The queue step fires. The other two steps do nothing.
    obtain ⟨hfar, hmin⟩ := helig
    have hnej : ¬ effective_balance ≤ p.EJECTION_BALANCE :=
      Nat.not_le.mpr (Nat.lt_of_lt_of_le hbalances hmin)
    have hnfin : ¬ current_epoch + 1 ≤ finalized_epoch := by omega
    simp [eligibilityStep, ejectionStep, activationStep, hfar, hmin, hnej, hnfin, hinc, bind,
      Except.bind, pure, Except.pure]
  · have helig' : (f.activation_eligibility_epoch == FAR_FUTURE_EPOCH
        && decide (p.MIN_ACTIVATION_BALANCE ≤ effective_balance)) = false := by
      by_cases hf : f.activation_eligibility_epoch = FAR_FUTURE_EPOCH
      · have hm : ¬ p.MIN_ACTIVATION_BALANCE ≤ effective_balance := fun h => helig ⟨hf, h⟩
        simp [hf, hm]
      · simp [hf]
    have hstep1 : eligibilityStep p current_epoch effective_balance f = .ok f := by
      simp [eligibilityStep, ge_iff_le, helig', pure, Except.pure]
    simp only [hstep1, bind, Except.bind, ge_iff_le, helig', Bool.false_eq_true, if_false]
    by_cases hej : (f.activation_epoch ≤ current_epoch ∧ current_epoch < f.exit_epoch)
        ∧ effective_balance ≤ p.EJECTION_BALANCE
    · -- The ejection step fires. The activation step does nothing.
      have hej' : ((decide (f.activation_epoch ≤ current_epoch)
          && decide (current_epoch < f.exit_epoch))
          && decide (effective_balance ≤ p.EJECTION_BALANCE)) = true := by
        simp [hej.1.1, hej.1.2, hej.2]
      simp only [ejectionStep, hej', if_true]
      cases hx : exitStep p total_active_balance current_epoch effective_balance f with
      | error e => rfl
      | ok g =>
        obtain ⟨hact, helg⟩ := exitStep_activation p total_active_balance current_epoch
          effective_balance f g hx
        have hnfar : g.activation_epoch ≠ FAR_FUTURE_EPOCH := by
          have hlt : f.activation_epoch < FAR_FUTURE_EPOCH :=
            Nat.lt_of_le_of_lt hej.1.1 (Nat.lt_of_lt_of_le hej.1.2 hexit)
          rw [hact]; exact Nat.ne_of_lt hlt
        simp [activationStep, hnfar, pure, Except.pure]
    · have hej' : ((decide (f.activation_epoch ≤ current_epoch)
          && decide (current_epoch < f.exit_epoch))
          && decide (effective_balance ≤ p.EJECTION_BALANCE)) = false := by
        by_cases h1 : f.activation_epoch ≤ current_epoch
        · by_cases h2 : current_epoch < f.exit_epoch
          · have h3 : ¬ effective_balance ≤ p.EJECTION_BALANCE := fun h => hej ⟨⟨h1, h2⟩, h⟩
            simp [h1, h2, h3]
          · simp [h2]
        · simp [h1]
      simp only [ejectionStep, hej', Bool.false_eq_true, if_false, pure, Except.pure,
        activationStep, hactivation, bind, Except.bind]

end EpochProofs.Spec
