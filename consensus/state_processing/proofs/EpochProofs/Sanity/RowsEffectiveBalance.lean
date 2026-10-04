import EpochProofs.Sanity.RowsSlashings
import EpochProofs.Sanity.EffectiveBalanceUpdates

/-!
# `process_effective_balance_updates` as a pass over rows

`effectiveBalanceRowStep` applies `newEffectiveBalance` to one row, with the thresholds already
known. `process_effective_balance_updates_rows` shows that the spec loop is a `passM` of this
step over `rowsOf state`.
-/

namespace EpochProofs.Spec

/-- One row of `process_effective_balance_updates`. Only `validator` changes. -/
def effectiveBalanceRowStep (p : Preset) (downward_threshold upward_threshold : Uint64) :
    Unit → Row → SpecM (Unit × Row) := fun _ r => do
  let effective_balance ←
    newEffectiveBalance p downward_threshold upward_threshold r.validator r.balance
  pure ((), { r with validator := { r.validator with effective_balance } })

/-- The step changes only `validator`. -/
theorem effectiveBalanceRowStep_preserves (p : Preset) (downward upward : Uint64) (u : Unit)
    (r : Row) (x : Unit × Row) (h : effectiveBalanceRowStep p downward upward u r = .ok x) :
    x.2 = { r with validator := x.2.validator } := by
  simp only [effectiveBalanceRowStep, bind, Except.bind] at h
  cases hb : newEffectiveBalance p downward upward r.validator r.balance with
  | error e => simp [hb] at h
  | ok b =>
    simp only [hb, pure, Except.pure, Except.ok.injEq] at h
    subst h
    rfl

/-- The loop over `(validator, index)` pairs is the pass over the rows, if each row holds the
balance at its index. -/
theorem mapM_effectiveBalanceStep (p : Preset) (balances : List Gwei)
    (downward upward : Uint64) (hthresholds : hysteresisThresholds p = .ok (downward, upward)) :
    ∀ (rows : List Row), (∀ r ∈ rows, balances[r.index]? = some r.balance) →
      (rows.map fun r => (r.validator, r.index)).mapM (effectiveBalanceStep p balances) =
        (fun x => x.2.map (·.validator)) <$>
          passM (effectiveBalanceRowStep p downward upward) () rows
  | [], _ => by simp [passM, pure, Except.pure, Functor.map, Except.map]
  | r :: rs, hbal => by
    have hr := hbal r (List.mem_cons_self ..)
    have ih := mapM_effectiveBalanceStep p balances downward upward hthresholds rs
      (fun r' h => hbal r' (List.mem_cons_of_mem _ h))
    simp only [List.map_cons, List.mapM_cons, passM_cons, effectiveBalanceStep,
      effectiveBalanceRowStep, hr, hthresholds, ih, bind, Except.bind, pure, Except.pure,
      Functor.map, Except.map]
    cases newEffectiveBalance p downward upward r.validator r.balance with
    | error e => rfl
    | ok b =>
      simp only
      cases passM (effectiveBalanceRowStep p downward upward) () rs <;> rfl

/-- Each row of a state holds the balance at its index, if the lengths match. -/
theorem rowsOf_balance_getElem? (state : BeaconState)
    (h : state.balances.length = state.validators.length) :
    ∀ r ∈ rowsOf state, state.balances[r.index]? = some r.balance := by
  intro r hr
  obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hr
  have hv : i < state.balances.length := by
    rw [h, ← rowsOf_length state]; exact hi
  simp [rowsOf_getElem, List.getD_eq_getElem?_getD, hv]

/-- `process_effective_balance_updates` is a pass of `effectiveBalanceRowStep` over the rows, once
the thresholds are known. -/
theorem process_effective_balance_updates_rows (p : Preset) (state : BeaconState)
    (downward upward : Uint64) (hthresholds : hysteresisThresholds p = .ok (downward, upward))
    (hrows : RowsOk state) :
    process_effective_balance_updates p state =
      (fun x => state.withRows x.2) <$>
        passM (effectiveBalanceRowStep p downward upward) () (rowsOf state) := by
  obtain ⟨hbal, hscores, -, -⟩ := hrows
  rw [process_effective_balance_updates_eq, ← rowsOf_map_pair,
    mapM_effectiveBalanceStep p state.balances downward upward hthresholds (rowsOf state)
      (rowsOf_balance_getElem? state hbal)]
  cases hpass : passM (effectiveBalanceRowStep p downward upward) () (rowsOf state) with
  | error e => rfl
  | ok y =>
    have hb := passM_map_eq _ (·.balance) (fun a r x hx => by
      rw [effectiveBalanceRowStep_preserves _ _ _ a r x hx]) () (rowsOf state) y hpass
    have hs := passM_map_eq _ (·.inactivity_score) (fun a r x hx => by
      rw [effectiveBalanceRowStep_preserves _ _ _ a r x hx]) () (rowsOf state) y hpass
    rw [rowsOf_map_balance state hbal] at hb
    rw [rowsOf_map_inactivity_score state hscores] at hs
    simp only [Functor.map, Except.map, BeaconState.withRows, hb, hs]

/-- `process_effective_balance_updates_rows` in `SameOk` form, for the single pass proof. -/
theorem process_effective_balance_updates_rows_sameOk (p : Preset) (state : BeaconState)
    (downward upward : Uint64) (hthresholds : hysteresisThresholds p = .ok (downward, upward))
    (hrows : RowsOk state) :
    SameOk (process_effective_balance_updates p state)
      ((fun x => state.withRows x.2) <$>
        passM (effectiveBalanceRowStep p downward upward) () (rowsOf state)) :=
  SameOk.of_eq (process_effective_balance_updates_rows p state downward upward hthresholds hrows)

end EpochProofs.Spec
