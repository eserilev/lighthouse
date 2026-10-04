import EpochProofs.Sanity.Rows
import EpochProofs.Sanity.RegistryUpdates
import EpochProofs.Sanity.RowsSlashings

/-!
# `process_registry_updates` as a pass over rows

`registryRowStep` runs `registryStepExclusive` on one row. The accumulator is the exit churn:
`(earliest_exit_epoch, exit_balance_to_consume)`. `process_registry_updates_rows` shows that the
spec loop is a `passM` of this step. Each spec iteration changes only the validator at its index
and the two churn fields.
-/

namespace EpochProofs.Spec

/-- `compute_exit_epoch_and_update_churn` is `exitChurnStep` on the two churn fields. -/
theorem compute_exit_epoch_and_update_churn_eq (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (exit_balance : Gwei) (current_epoch : Epoch)
    (hcurrent : get_current_epoch p state = .ok current_epoch) :
    compute_exit_epoch_and_update_churn p total_active_balance state exit_balance =
      (fun x => (x.1,
        { state with
          earliest_exit_epoch := x.2.1
          exit_balance_to_consume := x.2.2 })) <$>
        exitChurnStep p total_active_balance current_epoch exit_balance
          state.earliest_exit_epoch state.exit_balance_to_consume := by
  unfold compute_exit_epoch_and_update_churn exitChurnStep
  simp only [hcurrent, bind, Except.bind, pure, Except.pure, Functor.map, Except.map]
  cases compute_activation_exit_epoch p current_epoch with
  | error e => rfl
  | ok a =>
  cases get_exit_churn_limit p total_active_balance with
  | error e => rfl
  | ok c =>
  dsimp only
  generalize (if state.earliest_exit_epoch < max state.earliest_exit_epoch a then c
    else state.exit_balance_to_consume) = b
  by_cases hb : exit_balance > b
  · simp only [hb, if_true]
    cases uint64Sub exit_balance b with
    | error e => rfl
    | ok d =>
    dsimp only
    cases uint64Sub d 1 with
    | error e => rfl
    | ok d =>
    dsimp only
    cases uint64Div d c with
    | error e => rfl
    | ok d =>
    dsimp only
    cases uint64Add d 1 with
    | error e => rfl
    | ok d =>
    dsimp only
    cases uint64Add (max state.earliest_exit_epoch a) d with
    | error e => rfl
    | ok d' =>
    dsimp only
    cases uint64Mul d c with
    | error e => rfl
    | ok d =>
    dsimp only
    cases uint64Add b d with
    | error e => rfl
    | ok d =>
    dsimp only
    cases uint64Sub d exit_balance <;> rfl
  · simp only [hb, if_false]
    cases uint64Sub b exit_balance <;> rfl

/-- The registry fields of a validator, with the churn accumulator. -/
def registryFieldsOf (validator : Validator) (churn : Epoch × Gwei) : RegistryFields :=
  { activation_eligibility_epoch := validator.activation_eligibility_epoch
    activation_epoch := validator.activation_epoch
    exit_epoch := validator.exit_epoch
    withdrawable_epoch := validator.withdrawable_epoch
    earliest_exit_epoch := churn.1
    exit_balance_to_consume := churn.2 }

/-- Write the four validator epochs of `f` back to the validator. -/
def Validator.withRegistryFields (validator : Validator) (f : RegistryFields) : Validator :=
  { validator with
    activation_eligibility_epoch := f.activation_eligibility_epoch
    activation_epoch := f.activation_epoch
    exit_epoch := f.exit_epoch
    withdrawable_epoch := f.withdrawable_epoch }

/-- The registry update for one row: `registryStepExclusive` with the churn accumulator.
Only `validator` changes in the row. -/
def registryRowStep (p : Preset) (total_active_balance current_epoch finalized_epoch
    activation_epoch : Nat) (churn : Epoch × Gwei) (row : Row) :
    SpecM ((Epoch × Gwei) × Row) := do
  let f ← registryStepExclusive p total_active_balance current_epoch finalized_epoch
    activation_epoch row.validator.effective_balance (registryFieldsOf row.validator churn)
  pure ((f.earliest_exit_epoch, f.exit_balance_to_consume),
    { row with validator := row.validator.withRegistryFields f })

/-- The spec loop state: `state` with new validators and new churn fields. -/
def registryLoopState (state : BeaconState) (validators : List Validator) (churn : Epoch × Gwei) :
    BeaconState :=
  { state with
    validators
    earliest_exit_epoch := churn.1
    exit_balance_to_consume := churn.2 }

/-- The body of the `for` loop in `process_registry_updates`. -/
def registrySpecBody (p : Preset) (total_active_balance : Gwei)
    (current_epoch activation_epoch : Epoch)
    (index : Nat) (state : BeaconState) : SpecM (ForInStep BeaconState) := do
  let mut state := state
  let validator ← listGet state.validators index
  if is_eligible_for_activation_queue p validator then
    let validator := { validator with
      activation_eligibility_epoch := ← uint64Add current_epoch 1 }
    state := { state with validators := ← listSet state.validators index validator }
  else if is_active_validator validator current_epoch
      && validator.effective_balance ≤ p.EJECTION_BALANCE then
    state ← initiate_validator_exit p total_active_balance state index
  else if is_eligible_for_activation state validator then
    let validator := { validator with activation_epoch }
    state := { state with validators := ← listSet state.validators index validator }
  pure (.yield state)

/-- `process_registry_updates` is its `for` loop, once the two epochs are known. -/
theorem process_registry_updates_forIn (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (current_epoch activation_epoch : Epoch)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hactivation : compute_activation_exit_epoch p current_epoch = .ok activation_epoch) :
    process_registry_updates p total_active_balance state =
      forIn (List.range state.validators.length) state
        (registrySpecBody p total_active_balance current_epoch activation_epoch) := by
  unfold process_registry_updates
  rw [hcurrent]
  show (compute_activation_exit_epoch p current_epoch >>= fun activation_epoch =>
    forIn (List.range state.validators.length) state
        (registrySpecBody p total_active_balance current_epoch activation_epoch) >>=
      fun r => pure r) = _
  rw [hactivation]
  exact bind_pure (m := SpecM) _

/-- One spec iteration on a loop state is one `registryRowStep`. The loop reads and writes
only the validator at its index, and `initiate_validator_exit` only changes the churn fields. -/
theorem registrySpecBody_step (p : Preset) (total_active_balance : Gwei) (state : BeaconState)
    (current_epoch activation_epoch : Epoch)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (pre rest : List Validator) (row : Row) (churn : Epoch × Gwei) :
    registrySpecBody p total_active_balance current_epoch activation_epoch pre.length
        (registryLoopState state (pre ++ row.validator :: rest) churn) =
      match registryRowStep p total_active_balance current_epoch
          state.finalized_checkpoint.epoch activation_epoch churn row with
      | .error e => .error e
      | .ok x => .ok (.yield (registryLoopState state (pre ++ x.2.validator :: rest) x.1)) := by
  have hget : ∀ w : Validator, listGet (pre ++ w :: rest) pre.length = .ok w := by
    intro w; simp [listGet, pure, Except.pure]
  have hset : ∀ w : Validator,
      listSet (pre ++ row.validator :: rest) pre.length w = .ok (pre ++ w :: rest) := by
    intro w; simp [listSet, pure, Except.pure]
  have hcurrent' : get_current_epoch p (registryLoopState state (pre ++ row.validator :: rest)
      churn) = .ok current_epoch := hcurrent
  unfold registrySpecBody registryRowStep registryStepExclusive
  simp only [registryLoopState, hget, hset, bind, Except.bind, pure, Except.pure,
    registryFieldsOf, is_eligible_for_activation_queue, is_active_validator,
    is_eligible_for_activation, ge_iff_le, Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq]
  by_cases h1 : row.validator.activation_eligibility_epoch = FAR_FUTURE_EPOCH
      ∧ p.MIN_ACTIVATION_BALANCE ≤ row.validator.effective_balance
  · simp only [h1, and_self, ↓reduceIte]
    cases uint64Add current_epoch 1 <;> rfl
  simp only [h1, ↓reduceIte]
  by_cases h2 : (row.validator.activation_epoch ≤ current_epoch
      ∧ current_epoch < row.validator.exit_epoch)
      ∧ row.validator.effective_balance ≤ p.EJECTION_BALANCE
  · simp only [h2, and_self, ↓reduceIte]
    unfold initiate_validator_exit exitStep
    simp only [hget, bind, Except.bind, pure, Except.pure]
    by_cases h3 : (row.validator.exit_epoch != FAR_FUTURE_EPOCH) = true
    · simp only [h3, ↓reduceIte]
      rfl
    simp only [h3, ↓reduceIte, Bool.false_eq_true]
    have hchurn := compute_exit_epoch_and_update_churn_eq p total_active_balance
      (registryLoopState state (pre ++ row.validator :: rest) churn)
      row.validator.effective_balance current_epoch hcurrent'
    simp only [registryLoopState] at hchurn
    rw [hchurn]
    cases exitChurnStep p total_active_balance current_epoch row.validator.effective_balance
        churn.fst churn.snd with
    | error e => rfl
    | ok x =>
    simp only [Functor.map, Except.map]
    cases uint64Add x.fst p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY with
    | error e => rfl
    | ok w =>
    simp only [hset]
    rfl
  simp only [h2, ↓reduceIte]
  split <;> rfl

/-- The spec loop over the indices from `pre.length` is a `passM` over the rows. -/
theorem registrySpecBody_forIn (p : Preset) (total_active_balance : Gwei) (state : BeaconState)
    (current_epoch activation_epoch : Epoch)
    (hcurrent : get_current_epoch p state = .ok current_epoch) :
    ∀ (rows : List Row) (pre : List Validator) (churn : Epoch × Gwei),
      forIn (List.range' pre.length rows.length)
          (registryLoopState state (pre ++ rows.map (·.validator)) churn)
          (registrySpecBody p total_active_balance current_epoch activation_epoch) =
        (fun x => registryLoopState state (pre ++ x.2.map (·.validator)) x.1) <$>
          passM (registryRowStep p total_active_balance current_epoch
            state.finalized_checkpoint.epoch activation_epoch) churn rows := by
  intro rows
  induction rows with
  | nil =>
    intro pre churn
    simp [passM, forIn, ForIn.forIn, pure, Except.pure, Functor.map, Except.map]
  | cons row rows ih =>
    intro pre churn
    show forIn (pre.length :: List.range' (pre.length + 1) rows.length)
        (registryLoopState state (pre ++ row.validator :: rows.map (·.validator)) churn) _ = _
    rw [List.forIn_cons, registrySpecBody_step p total_active_balance state current_epoch
      activation_epoch hcurrent, passM_cons]
    cases registryRowStep p total_active_balance current_epoch state.finalized_checkpoint.epoch
        activation_epoch churn row with
    | error e => rfl
    | ok x =>
    have h := ih (pre ++ [x.2.validator]) x.1
    simp only [List.length_append, List.length_singleton, List.append_assoc,
      List.singleton_append] at h
    simp only [bind, Except.bind, h]
    cases passM (registryRowStep p total_active_balance current_epoch
        state.finalized_checkpoint.epoch activation_epoch) x.1 rows with
    | error e => rfl
    | ok y => simp [Functor.map, Except.map, pure, Except.pure]

/-- The step changes only `validator` in the row. -/
theorem registryRowStep_preserves (p : Preset) (total_active_balance current_epoch finalized_epoch
    activation_epoch : Nat) (churn : Epoch × Gwei) (row : Row) (x : (Epoch × Gwei) × Row)
    (h : registryRowStep p total_active_balance current_epoch finalized_epoch activation_epoch
      churn row = .ok x) :
    x.2 = { row with validator := x.2.validator } := by
  unfold registryRowStep at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · cases h
  · cases h; rfl

/-- The pass keeps the balances and the inactivity scores of the rows. -/
private theorem passM_registry_preserves (p : Preset) (total_active_balance current_epoch
    finalized_epoch activation_epoch : Nat) :
    ∀ (rows : List Row) (churn : Epoch × Gwei) (y : (Epoch × Gwei) × List Row),
      passM (registryRowStep p total_active_balance current_epoch finalized_epoch
        activation_epoch) churn rows = .ok y →
      y.2.map (·.balance) = rows.map (·.balance)
        ∧ y.2.map (·.inactivity_score) = rows.map (·.inactivity_score) := by
  intro rows
  induction rows with
  | nil =>
    intro churn y h
    cases h
    exact ⟨rfl, rfl⟩
  | cons row rows ih =>
    intro churn y h
    rw [passM_cons] at h
    simp only [bind, Except.bind, pure, Except.pure] at h
    split at h
    · cases h
    · rename_i x hx
      split at h
      · cases h
      · rename_i z hz
        cases h
        obtain ⟨hb, hi⟩ := ih x.1 z hz
        have hrow := registryRowStep_preserves p total_active_balance current_epoch
          finalized_epoch activation_epoch churn row x hx
        rw [hrow]
        simp only [List.map_cons, hb, hi, and_self]

/-- `process_registry_updates` is a `passM` of `registryRowStep` over the rows.

`hrows` makes `withRows` write back the same balances and inactivity scores.
`hcurrent` and `hactivation` give the two epochs that the step takes as parameters.
If one of them fails, the spec fails but the pass does not. -/
theorem process_registry_updates_rows (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (current_epoch activation_epoch : Epoch) (hrows : RowsOk state)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hactivation : compute_activation_exit_epoch p current_epoch = .ok activation_epoch) :
    SameOk (process_registry_updates p total_active_balance state)
      ((fun x => { state.withRows x.2 with
          earliest_exit_epoch := x.1.1
          exit_balance_to_consume := x.1.2 }) <$>
        passM (registryRowStep p total_active_balance current_epoch
          state.finalized_checkpoint.epoch activation_epoch)
          (state.earliest_exit_epoch, state.exit_balance_to_consume) (rowsOf state)) := by
  apply SameOk.of_eq
  have h := registrySpecBody_forIn p total_active_balance state current_epoch activation_epoch
    hcurrent (rowsOf state) [] (state.earliest_exit_epoch, state.exit_balance_to_consume)
  have hstate : registryLoopState state state.validators
      (state.earliest_exit_epoch, state.exit_balance_to_consume) = state := rfl
  simp only [List.length_nil, List.nil_append, rowsOf_length, rowsOf_map_validator, hstate,
    ← List.range_eq_range'] at h
  rw [process_registry_updates_forIn p total_active_balance state current_epoch activation_epoch
    hcurrent hactivation, h]
  cases hy : passM (registryRowStep p total_active_balance current_epoch
      state.finalized_checkpoint.epoch activation_epoch)
      (state.earliest_exit_epoch, state.exit_balance_to_consume) (rowsOf state) with
  | error e => rfl
  | ok y =>
    obtain ⟨hb, hi⟩ := passM_registry_preserves p total_active_balance current_epoch
      state.finalized_checkpoint.epoch activation_epoch (rowsOf state) _ y hy
    simp only [Functor.map, Except.map, registryLoopState, BeaconState.withRows, hb, hi,
      rowsOf_map_balance state hrows.1,
      rowsOf_map_inactivity_score state hrows.2.1]

end EpochProofs.Spec
