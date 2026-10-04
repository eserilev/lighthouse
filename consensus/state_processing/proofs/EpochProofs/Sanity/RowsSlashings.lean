import EpochProofs.Sanity.Rows
import EpochProofs.Sanity.Slashings

/-!
# `process_slashings` as a pass over rows

`slashingsRowStep` applies `slashingBalanceStep` to one row. `process_slashings_rows` shows that
`process_slashings` is a `passM` of this step over `rowsOf state`. This file also holds general
lemmas: rows round-trip to the state lists, and a pass keeps the fields that its step keeps.
-/

namespace EpochProofs.Spec

/-- The validators of the rows are the validators of the state. -/
theorem rowsOf_map_validator (state : BeaconState) :
    (rowsOf state).map (·.validator) = state.validators := by
  apply List.ext_getElem
  · simp [rowsOf_length]
  · intro i h1 h2
    simp [rowsOf_getElem]

/-- The balances of the rows are the balances of the state, if the lengths match. -/
theorem rowsOf_map_balance (state : BeaconState)
    (h : state.balances.length = state.validators.length) :
    (rowsOf state).map (·.balance) = state.balances := by
  apply List.ext_getElem
  · simp [rowsOf_length, h]
  · intro i h1 h2
    simp [rowsOf_getElem, List.getD_eq_getElem?_getD, h2]

/-- The inactivity scores of the rows are the scores of the state, if the lengths match. -/
theorem rowsOf_map_inactivity_score (state : BeaconState)
    (h : state.inactivity_scores.length = state.validators.length) :
    (rowsOf state).map (·.inactivity_score) = state.inactivity_scores := by
  apply List.ext_getElem
  · simp [rowsOf_length, h]
  · intro i h1 h2
    simp [rowsOf_getElem, List.getD_eq_getElem?_getD, h2]

/-- The indices of the rows are `0, 1, 2, ...`. -/
theorem rowsOf_map_index (state : BeaconState) :
    (rowsOf state).map (·.index) = List.range' 0 (rowsOf state).length := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp [rowsOf_getElem]

/-- The rows give back the `(validator, index)` pairs of the state. -/
theorem rowsOf_map_pair (state : BeaconState) :
    (rowsOf state).map (fun r => (r.validator, r.index)) = state.validators.zipIdx := by
  apply List.ext_getElem
  · simp [rowsOf_length]
  · intro i h1 h2
    simp [rowsOf_getElem]

/-- If each step keeps `g` of its row, the pass keeps `g` of all rows. -/
theorem passM_map_eq {A R B : Type} (f : A → R → SpecM (A × R)) (g : R → B)
    (hf : ∀ a r x, f a r = .ok x → g x.2 = g r) :
    ∀ (a : A) (rows : List R) (y : A × List R), passM f a rows = .ok y →
      y.2.map g = rows.map g := by
  intro a rows
  induction rows generalizing a with
  | nil =>
    intro y h
    simp only [passM, pure, Except.pure, Except.ok.injEq] at h
    subst h
    rfl
  | cons r rs ih =>
    intro y h
    rw [passM_cons] at h
    cases hx : f a r with
    | error e => simp [hx, bind, Except.bind] at h
    | ok x =>
      simp only [hx, bind, Except.bind] at h
      cases hy : passM f x.1 rs with
      | error e => simp [hy] at h
      | ok z =>
        simp only [hy, pure, Except.pure, Except.ok.injEq] at h
        subst h
        simp [hf a r x hx, ih x.1 z hy]

/-- One row of `process_slashings`. Only `balance` changes. -/
def slashingsRowStep (p : Preset) (target penalty_per_increment : Uint64) :
    Unit → Row → SpecM (Unit × Row) := fun _ r => do
  let balance ← slashingBalanceStep p target penalty_per_increment r.validator r.balance
  pure ((), { r with balance })

/-- The step changes only `balance`. -/
theorem slashingsRowStep_preserves (p : Preset) (target per : Uint64) (u : Unit) (r : Row)
    (x : Unit × Row) (h : slashingsRowStep p target per u r = .ok x) :
    x.2 = { r with balance := x.2.balance } := by
  simp only [slashingsRowStep, bind, Except.bind] at h
  cases hb : slashingBalanceStep p target per r.validator r.balance with
  | error e => simp [hb] at h
  | ok b =>
    simp only [hb, pure, Except.pure, Except.ok.injEq] at h
    subst h
    rfl

/-- One loop step on the full balance list is `slashingBalanceStep` at that index. -/
theorem slashingLoopStep_row (p : Preset) (target per : Uint64) (pre rest : List Gwei)
    (validator : Validator) (balance : Gwei) :
    slashingLoopStep p target per (pre ++ balance :: rest) (validator, pre.length) =
      (fun b => pre ++ b :: rest) <$> slashingBalanceStep p target per validator balance := by
  simp only [slashingLoopStep, slashingBalanceStep]
  split
  · simp only [bind, Except.bind]
    cases uint64Div validator.effective_balance p.EFFECTIVE_BALANCE_INCREMENT with
    | error e => rfl
    | ok n =>
      simp only
      cases uint64Mul per n with
      | error e => rfl
      | ok penalty =>
        simp [decrease_balance, listGet, listSet, bind, Except.bind, pure, Except.pure,
          Functor.map, Except.map]
  · rfl

/-- The loop over `(validator, index)` pairs is the pass over the rows. `pre` holds the balances
before the rows. -/
theorem foldlM_slashingLoopStep (p : Preset) (target per : Uint64) :
    ∀ (rows : List Row) (pre : List Gwei),
      rows.map (·.index) = List.range' pre.length rows.length →
      (rows.map fun r => (r.validator, r.index)).foldlM (slashingLoopStep p target per)
          (pre ++ rows.map (·.balance)) =
        (fun x => pre ++ x.2.map (·.balance)) <$> passM (slashingsRowStep p target per) () rows
  | [], pre, _ => by simp [passM, pure, Except.pure, Functor.map, Except.map]
  | r :: rs, pre, hidx => by
    simp only [List.map_cons, List.length_cons, List.range'_succ, List.cons.injEq] at hidx
    obtain ⟨hr, hrs⟩ := hidx
    simp only [List.map_cons, List.foldlM_cons, passM_cons, hr, slashingLoopStep_row]
    simp only [slashingsRowStep, bind, Except.bind, Functor.map, Except.map]
    cases slashingBalanceStep p target per r.validator r.balance with
    | error e => rfl
    | ok b =>
      simp only [pure, Except.pure]
      have ih := foldlM_slashingLoopStep p target per rs (pre ++ [b])
        (by simpa using hrs)
      simp only [List.append_assoc, List.cons_append, List.nil_append] at ih
      rw [ih]
      simp only [Functor.map, Except.map]
      cases passM (slashingsRowStep p target per) () rs <;> simp

/-- `process_slashings` is a pass of `slashingsRowStep` over the rows, once the preamble
succeeds. -/
theorem process_slashings_rows (p : Preset) (total_active_balance : Gwei) (state : BeaconState)
    (epoch : Epoch) (hepoch : get_current_epoch p state = .ok epoch)
    (htarget : epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2 < UINT64_SIZE)
    (adjusted per : Gwei)
    (hpre : slashingsPreamble p total_active_balance state.slashings = .ok (adjusted, per))
    (hrows : RowsOk state) :
    process_slashings p total_active_balance state =
      (fun x => state.withRows x.2) <$>
        passM (slashingsRowStep p (epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2) per) ()
          (rowsOf state) := by
  obtain ⟨hbal, hscores, -, -⟩ := hrows
  rw [process_slashings_eq p total_active_balance state epoch hepoch htarget, hpre]
  have hfold := foldlM_slashingLoopStep p (epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2) per
    (rowsOf state) [] (by simpa using rowsOf_map_index state)
  rw [rowsOf_map_pair, List.nil_append, rowsOf_map_balance state hbal] at hfold
  simp only [bind, Except.bind, hfold]
  cases hpass : passM (slashingsRowStep p (epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2) per) ()
      (rowsOf state) with
  | error e => rfl
  | ok y =>
    have hv := passM_map_eq _ (·.validator) (fun a r x hx => by
      rw [slashingsRowStep_preserves _ _ _ a r x hx]) () (rowsOf state) y hpass
    have hs := passM_map_eq _ (·.inactivity_score) (fun a r x hx => by
      rw [slashingsRowStep_preserves _ _ _ a r x hx]) () (rowsOf state) y hpass
    rw [rowsOf_map_validator] at hv
    rw [rowsOf_map_inactivity_score state hscores] at hs
    simp only [Functor.map, Except.map, pure, Except.pure, List.nil_append,
      BeaconState.withRows, hv, hs]

/-- `process_slashings_rows` in `SameOk` form, for the single pass proof. -/
theorem process_slashings_rows_sameOk (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (epoch : Epoch) (hepoch : get_current_epoch p state = .ok epoch)
    (htarget : epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2 < UINT64_SIZE)
    (adjusted per : Gwei)
    (hpre : slashingsPreamble p total_active_balance state.slashings = .ok (adjusted, per))
    (hrows : RowsOk state) :
    SameOk (process_slashings p total_active_balance state)
      ((fun x => state.withRows x.2) <$>
        passM (slashingsRowStep p (epoch + p.EPOCHS_PER_SLASHINGS_VECTOR / 2) per) ()
          (rowsOf state)) :=
  SameOk.of_eq (process_slashings_rows p total_active_balance state epoch hepoch htarget
    adjusted per hpre hrows)

end EpochProofs.Spec
