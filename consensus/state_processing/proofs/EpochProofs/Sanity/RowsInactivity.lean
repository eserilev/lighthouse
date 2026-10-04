import EpochProofs.Sanity.RowsSlashings
import EpochProofs.Sanity.InactivityUpdates

/-!
# `process_inactivity_updates` as a pass over rows

`inactivityRowStep` updates the inactivity score of one row. It reads only that row.
`process_inactivity_updates_rows` shows that the spec gives the same `ok` values as a `passM` of
this step over all rows. The spec fails before its loop when `get_eligible_validator_indices`
fails. The pass fails on the first bad row instead, so the theorem states `SameOk`.
-/

namespace EpochProofs.Spec

/-- The test of `get_eligible_validator_indices` for one validator. Like Python, it computes
`previous_epoch + 1` only for a slashed validator that is not active. -/
def validatorEligible (previous_epoch : Epoch) (v : Validator) : SpecM Bool :=
  if is_active_validator v previous_epoch then pure true
  else if v.slashed then do pure (decide ((← uint64Add previous_epoch 1) < v.withdrawable_epoch))
  else pure false

/-- `has_flag flags TIMELY_TARGET_FLAG_INDEX`. This flag index never raises. -/
def targetHit (flags : ParticipationFlags) : Bool :=
  flags &&& UInt8.ofNat (2 ^ TIMELY_TARGET_FLAG_INDEX) == UInt8.ofNat (2 ^ TIMELY_TARGET_FLAG_INDEX)

/-- The row is in `get_unslashed_participating_indices` for the target flag at
`previous_epoch`. -/
def rowHitsTarget (previous_epoch : Epoch) (r : Row) : Bool :=
  is_active_validator r.validator previous_epoch && targetHit r.previous_participation
    && !r.validator.slashed

/-- One row of `process_inactivity_updates`. An eligible row gets a new inactivity score from
`inactivityScoreStep`. The step changes no other field. -/
def inactivityRowStep (p : Preset) (previous_epoch : Epoch) (in_leak : Bool) :
    Unit → Row → SpecM (Unit × Row) := fun _ r => do
  if (← validatorEligible previous_epoch r.validator) then
    let score ← inactivityScoreStep p (rowHitsTarget previous_epoch r) in_leak r.inactivity_score
    pure ((), { r with inactivity_score := score })
  else
    pure ((), r)

/-- `targetHit` is `has_flag` for the target flag. -/
theorem has_flag_target (flags : ParticipationFlags) :
    has_flag flags TIMELY_TARGET_FLAG_INDEX = .ok (targetHit flags) := by
  simp [has_flag, targetHit, TIMELY_TARGET_FLAG_INDEX, pure, Except.pure]

/-- A filtering fold over a list with a monadic test. -/
def filterStep {α β : Type} (P : α → SpecM Bool) (out : α → β) (acc : List β) (x : α) :
    SpecM (List β) := do
  let b ← P x
  pure (if b then acc ++ [out x] else acc)

/-- If the test never fails, the fold is `List.filter`. -/
theorem foldlM_filterStep_ok {α β : Type} (P : α → SpecM Bool) (Q : α → Bool) (out : α → β)
    (l : List α) (acc : List β) (h : ∀ x ∈ l, P x = .ok (Q x)) :
    l.foldlM (filterStep P out) acc = .ok (acc ++ (l.filter Q).map out) := by
  induction l generalizing acc with
  | nil => simp [pure, Except.pure]
  | cons x xs ih =>
    rw [List.foldlM_cons]
    simp only [filterStep, h x (by simp), bind, Except.bind, pure, Except.pure]
    rw [ih _ (fun y hy => h y (by simp [hy]))]
    cases hq : Q x <;> simp [hq]

/-- If the test fails on some element, the fold fails. -/
theorem foldlM_filterStep_error {α β : Type} (P : α → SpecM Bool) (out : α → β)
    (l : List α) (acc : List β) (h : ∃ x ∈ l, ∃ e, P x = .error e) :
    ∃ e, l.foldlM (filterStep P out) acc = .error e := by
  induction l generalizing acc with
  | nil => simp at h
  | cons x xs ih =>
    rw [List.foldlM_cons]
    cases hp : P x with
    | error e => exact ⟨e, by simp [filterStep, hp, bind, Except.bind]⟩
    | ok b =>
      obtain ⟨y, hy, e, he⟩ := h
      have hy' : y ∈ xs := by
        rcases List.mem_cons.mp hy with hyx | hyx
        · subst hyx; rw [hp] at he; cases he
        · exact hyx
      obtain ⟨e', he'⟩ := ih _ ⟨y, hy', e, he⟩
      exact ⟨e', by simp only [filterStep, hp, bind, Except.bind, pure, Except.pure]; exact he'⟩

/-- The loop of `get_eligible_validator_indices` is a filtering fold. -/
theorem get_eligible_validator_indices_eq (p : Preset) (state : BeaconState)
    (previous_epoch : Epoch) (hprevious : get_previous_epoch p state = .ok previous_epoch) :
    get_eligible_validator_indices p state =
      state.validators.zipIdx.foldlM
        (filterStep (fun x => validatorEligible previous_epoch x.1) (·.2)) [] := by
  unfold get_eligible_validator_indices
  simp only [bind, Except.bind, hprevious]
  rw [forIn_yield_eq_foldlM
    (step := filterStep (fun x => validatorEligible previous_epoch x.1) (·.2))]
  · cases (state.validators.zipIdx.foldlM
      (filterStep (fun x => validatorEligible previous_epoch x.1) (·.2)) [] : SpecM (List Nat)) <;>
      rfl
  · intro x acc
    simp only [filterStep, validatorEligible, bind, Except.bind, pure, Except.pure]
    by_cases ha : is_active_validator x.1 previous_epoch = true
    · simp [ha]
    · by_cases hs : x.1.slashed = true
      · cases uint64Add previous_epoch 1 with
        | error e => simp [ha, hs]
        | ok v => by_cases hv : v < x.1.withdrawable_epoch <;> simp [ha, hs, hv]
      · simp [ha, hs]

/-- Each row of `rowsOf` comes from one index of the state. -/
theorem mem_rowsOf {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    ∃ i, ∃ hi : i < state.validators.length, r =
      { validator := state.validators[i]
        balance := state.balances.getD i 0
        inactivity_score := state.inactivity_scores.getD i 0
        previous_participation := state.previous_epoch_participation.getD i 0
        current_participation := state.current_epoch_participation.getD i 0
        index := i } := by
  obtain ⟨i, hi, hr⟩ := List.getElem_of_mem h
  rw [rowsOf_getElem] at hr
  exact ⟨i, by simpa [rowsOf_length] using hi, hr.symm⟩

/-- Two rows of `rowsOf` with the same index are equal. -/
theorem rowsOf_index_inj {state : BeaconState} {r r' : Row} (h : r ∈ rowsOf state)
    (h' : r' ∈ rowsOf state) (hidx : r.index = r'.index) : r = r' := by
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf h
  obtain ⟨j, hj, rfl⟩ := mem_rowsOf h'
  simp only at hidx
  subst hidx
  rfl

/-- `get_active_validator_indices` in terms of rows. -/
theorem get_active_validator_indices_rows (state : BeaconState) (epoch : Epoch) :
    get_active_validator_indices state epoch =
      ((rowsOf state).filter fun r => is_active_validator r.validator epoch).map (·.index) := by
  simp [get_active_validator_indices, rowsOf, List.filter_map, Function.comp_def]

/-- If the eligibility test never fails, `get_eligible_validator_indices` gives the indices
of the eligible rows, in order. -/
theorem eligible_rows (p : Preset) (state : BeaconState) (previous_epoch : Epoch)
    (hprevious : get_previous_epoch p state = .ok previous_epoch) (Ev : Validator → Bool)
    (hok : ∀ r ∈ rowsOf state,
      validatorEligible previous_epoch r.validator = .ok (Ev r.validator)) :
    get_eligible_validator_indices p state =
      .ok (((rowsOf state).filter fun r => Ev r.validator).map (·.index)) := by
  rw [get_eligible_validator_indices_eq p state previous_epoch hprevious,
    foldlM_filterStep_ok _ (fun x => Ev x.1)]
  · simp [rowsOf, List.filter_map, Function.comp_def]
  · intro x hx
    exact hok _ (List.mem_map.mpr ⟨x, hx, rfl⟩)

/-- If the eligibility test fails on one row, `get_eligible_validator_indices` fails. -/
theorem eligible_error (p : Preset) (state : BeaconState) (previous_epoch : Epoch)
    (hprevious : get_previous_epoch p state = .ok previous_epoch)
    (hbad : ∃ r ∈ rowsOf state, ∃ e, validatorEligible previous_epoch r.validator = .error e) :
    ∃ e, get_eligible_validator_indices p state = .error e := by
  rw [get_eligible_validator_indices_eq p state previous_epoch hprevious]
  apply foldlM_filterStep_error
  obtain ⟨r, hr, e, he⟩ := hbad
  obtain ⟨x, hx, rfl⟩ := List.mem_map.mp hr
  exact ⟨x, hx, e, he⟩

/-- A filtering fold over mapped elements. -/
theorem foldlM_filterStep_map {α β : Type} (P : β → SpecM Bool) (f : α → β) (l : List α)
    (acc : List β) :
    (l.map f).foldlM (filterStep P id) acc = l.foldlM (filterStep (fun x => P (f x)) f) acc := by
  rw [List.foldlM_map]
  rfl

/-- `state.validators[r.index]` is the validator of the row. -/
theorem listGet_rows_validator {state : BeaconState} {r : Row} (h : r ∈ rowsOf state) :
    listGet state.validators r.index = .ok r.validator := by
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf h
  simp [listGet, hi, pure, Except.pure]

/-- `state.previous_epoch_participation[r.index]` is the flags of the row. -/
theorem listGet_rows_previous {state : BeaconState} (hrows : RowsOk state) {r : Row}
    (h : r ∈ rowsOf state) :
    listGet state.previous_epoch_participation r.index = .ok r.previous_participation := by
  obtain ⟨i, hi, rfl⟩ := mem_rowsOf h
  have hi' : i < state.previous_epoch_participation.length := by rw [hrows.2.2.1]; exact hi
  simp [listGet, hi', pure, Except.pure]

/-- The target participants at the previous epoch, in terms of rows. After genesis the
previous epoch is not the current epoch, so the spec reads `previous_epoch_participation`. -/
theorem get_unslashed_participating_indices_rows (p : Preset) (state : BeaconState)
    (hrows : RowsOk state) (current_epoch previous_epoch : Epoch)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hprevious : get_previous_epoch p state = .ok previous_epoch)
    (hne : (previous_epoch == current_epoch) = false) :
    get_unslashed_participating_indices p state TIMELY_TARGET_FLAG_INDEX previous_epoch =
      .ok (((((rowsOf state).filter fun r => is_active_validator r.validator previous_epoch).filter
        fun r => targetHit r.previous_participation).filter fun r => !r.validator.slashed).map
          (·.index)) := by
  unfold get_unslashed_participating_indices
  simp only [bind, Except.bind, hprevious, hcurrent, pure, Except.pure, hne, BEq.rfl,
    Bool.true_or, Bool.not_true, Bool.false_eq_true, if_false, get_active_validator_indices_rows]
  rw [forIn_yield_eq_foldlM (step := filterStep (fun i => do
      has_flag (← listGet state.previous_epoch_participation i) TIMELY_TARGET_FLAG_INDEX) id)]
  rotate_left
  · intro i acc
    simp only [filterStep, bind, Except.bind, pure, Except.pure]
    cases listGet state.previous_epoch_participation i with
    | error e => rfl
    | ok f =>
      simp only [has_flag_target]
      cases targetHit f <;> rfl
  rw [foldlM_filterStep_map, foldlM_filterStep_ok _ (fun r => targetHit r.previous_participation)]
  rotate_left
  · intro r hr
    have hr' := (List.mem_filter.mp hr).1
    simp only [bind, Except.bind, listGet_rows_previous hrows hr', has_flag_target]
  simp only [List.nil_append]
  rw [forIn_yield_eq_foldlM (step := filterStep (fun i => do
      pure !(← listGet state.validators i).slashed) id)]
  rotate_left
  · intro i acc
    simp only [filterStep, bind, Except.bind, pure, Except.pure]
    cases listGet state.validators i with
    | error e => rfl
    | ok v =>
      dsimp only
      cases v.slashed <;> rfl
  rw [foldlM_filterStep_map, foldlM_filterStep_ok _ (fun r => !r.validator.slashed)]
  · rfl
  · intro r hr
    have hr' := (List.mem_filter.mp (List.mem_filter.mp hr).1).1
    simp only [bind, Except.bind, listGet_rows_validator hr', pure, Except.pure]

/-- A row is in the target participants exactly when `rowHitsTarget` holds. -/
theorem mem_participating_rows (state : BeaconState) (previous_epoch : Epoch) {r : Row}
    (hr : r ∈ rowsOf state) :
    decide (r.index ∈ ((((rowsOf state).filter fun r => is_active_validator r.validator
      previous_epoch).filter fun r => targetHit r.previous_participation).filter
        fun r => !r.validator.slashed).map (·.index)) = rowHitsTarget previous_epoch r := by
  generalize hF : (((rowsOf state).filter fun r => is_active_validator r.validator
      previous_epoch).filter fun r => targetHit r.previous_participation).filter
        (fun r => !r.validator.slashed) = F
  have hsub : ∀ r' ∈ F, r' ∈ rowsOf state := by
    intro r' h
    rw [← hF] at h
    exact (List.mem_filter.mp (List.mem_filter.mp (List.mem_filter.mp h).1).1).1
  have key : r.index ∈ F.map (·.index) ↔ r ∈ F := by
    constructor
    · intro h
      obtain ⟨r', hr', hidx⟩ := List.mem_map.mp h
      rw [rowsOf_index_inj (hsub r' hr') hr hidx] at hr'
      exact hr'
    · intro h
      exact List.mem_map.mpr ⟨r, h, rfl⟩
  have hmem : r ∈ F ↔ rowHitsTarget previous_epoch r = true := by
    subst hF
    simp only [List.mem_filter, hr, rowHitsTarget, true_and, Bool.and_eq_true]
  rw [Bool.eq_iff_iff, decide_eq_true_iff, key, hmem]

/-- Read the element after a prefix. -/
theorem listGet_middle {α : Type} (pre rest : List α) (a : α) :
    listGet (pre ++ a :: rest) pre.length = .ok a := by
  simp [listGet, pure, Except.pure]

/-- Write the element after a prefix. -/
theorem listSet_middle {α : Type} (pre rest : List α) (a b : α) :
    listSet (pre ++ a :: rest) pre.length b = .ok (pre ++ b :: rest) := by
  simp [listSet, pure, Except.pure]

/-- The spec loop over the eligible indices of a block of rows is the pass over that block.
The rows of the block have consecutive indices, and their scores sit at those indices. -/
theorem foldlM_inactivityLoopStep_rows (p : Preset) (previous_epoch : Epoch) (in_leak : Bool)
    (participating : List ValidatorIndex) (Eb : Row → Bool) :
    ∀ (rs : List Row) (pre post : List Uint64),
    rs.map (·.index) = List.range' pre.length rs.length →
    (∀ r ∈ rs, validatorEligible previous_epoch r.validator = .ok (Eb r)) →
    (∀ r ∈ rs, decide (r.index ∈ participating) = rowHitsTarget previous_epoch r) →
    ((rs.filter Eb).map (·.index)).foldlM (inactivityLoopStep p participating in_leak)
        (pre ++ rs.map (·.inactivity_score) ++ post) =
      (fun x => pre ++ x.2.map (·.inactivity_score) ++ post) <$>
        passM (inactivityRowStep p previous_epoch in_leak) () rs
  | [], pre, post, _, _, _ => by
    simp [passM, pure, Except.pure, Functor.map, Except.map]
  | r :: rs, pre, post, hidx, helig, hpart => by
    simp only [List.map_cons, List.length_cons, List.range'_succ, List.cons.injEq] at hidx
    obtain ⟨hr, hrs⟩ := hidx
    have ih := fun pre' (h : pre'.length = pre.length + 1) =>
      foldlM_inactivityLoopStep_rows p previous_epoch in_leak participating Eb rs pre' post
        (by rw [h]; exact hrs) (fun r' h' => helig r' (by simp [h']))
        (fun r' h' => hpart r' (by simp [h']))
    rw [passM_cons]
    simp only [inactivityRowStep, helig r (by simp), bind, Except.bind, pure, Except.pure]
    cases hE : Eb r
    · simp only [List.filter_cons, hE, Bool.false_eq_true, if_false]
      have h := ih (pre ++ [r.inactivity_score]) (by simp)
      simp only [List.map_cons, List.append_assoc, List.cons_append, List.nil_append] at h ⊢
      rw [h]
      cases passM (inactivityRowStep p previous_epoch in_leak) () rs <;>
        simp [Functor.map, Except.map]
    · simp only [List.filter_cons, hE, if_true, List.map_cons, List.foldlM_cons]
      have hp : decide (pre.length ∈ participating) = rowHitsTarget previous_epoch r := by
        rw [← hr]; exact hpart r (by simp)
      simp only [inactivityLoopStep, bind, Except.bind, hr, List.append_assoc, List.cons_append,
        listGet_middle, hp]
      cases hs : inactivityScoreStep p (rowHitsTarget previous_epoch r) in_leak
          r.inactivity_score with
      | error e => rfl
      | ok s =>
        simp only [listSet_middle]
        have h := ih (pre ++ [s]) (by simp)
        simp only [List.append_assoc, List.cons_append, List.nil_append] at h
        rw [h]
        cases passM (inactivityRowStep p previous_epoch in_leak) () rs <;>
          simp [Functor.map, Except.map]

/-- The row step changes only `inactivity_score`. -/
theorem inactivityRowStep_preserves (p : Preset) (previous_epoch : Epoch) (in_leak : Bool)
    (a u : Unit) (r r' : Row) (h : inactivityRowStep p previous_epoch in_leak a r = .ok (u, r')) :
    r' = { r with inactivity_score := r'.inactivity_score } := by
  simp only [inactivityRowStep, bind, Except.bind, pure, Except.pure] at h
  cases he : validatorEligible previous_epoch r.validator with
  | error e => simp [he] at h
  | ok b =>
    cases b
    · simp only [he, Bool.false_eq_true, if_false, Except.ok.injEq, Prod.mk.injEq] at h
      rw [← h.2]
    · cases hs : inactivityScoreStep p (rowHitsTarget previous_epoch r) in_leak
          r.inactivity_score with
      | error e => simp [he, hs] at h
      | ok s =>
        simp only [he, hs, if_true, Except.ok.injEq, Prod.mk.injEq] at h
        rw [← h.2]

/-- If the step fails on some row, the pass fails. -/
theorem passM_error {R : Type} (f : Unit → R → SpecM (Unit × R)) :
    ∀ (rs : List R), (∃ r ∈ rs, ∃ e, f () r = .error e) → ∃ e, passM f () rs = .error e
  | [], h => by simp at h
  | r :: rs, h => by
    rw [passM_cons]
    cases hr : f () r with
    | error e => exact ⟨e, rfl⟩
    | ok y =>
      obtain ⟨r', hr', e, he⟩ := h
      have hr'' : r' ∈ rs := by
        rcases List.mem_cons.mp hr' with h | h
        · subst h; rw [hr] at he; cases he
        · exact h
      obtain ⟨e', he'⟩ := passM_error f rs ⟨r', hr'', e, he⟩
      exact ⟨e', by simp only [bind, Except.bind, he']⟩

/-- After genesis, `process_inactivity_updates` is a pass of `inactivityRowStep` over the
rows. -/
theorem process_inactivity_updates_rows (p : Preset) (state : BeaconState)
    (current_epoch previous_epoch : Epoch) (in_leak : Bool) (hrows : RowsOk state)
    (hcurrent : get_current_epoch p state = .ok current_epoch)
    (hgenesis : current_epoch ≠ GENESIS_EPOCH)
    (hprevious : get_previous_epoch p state = .ok previous_epoch)
    (hleak : is_in_inactivity_leak p state = .ok in_leak) :
    SameOk (process_inactivity_updates p state)
      ((fun x => state.withRows x.2) <$>
        passM (inactivityRowStep p previous_epoch in_leak) () (rowsOf state)) := by
  have hne : (previous_epoch == current_epoch) = false := by
    simp only [get_previous_epoch, bind, Except.bind, hcurrent, pure, Except.pure,
      Except.ok.injEq] at hprevious
    subst hprevious
    simp only [GENESIS_EPOCH] at hgenesis
    simp only [saturating_sub, beq_eq_false_iff_ne]
    split
    · exact Nat.ne_of_lt (Nat.sub_one_lt hgenesis)
    · rw [Nat.sub_self]; exact fun h => hgenesis h.symm
  by_cases hbad : ∃ r ∈ rowsOf state, ∃ e, validatorEligible previous_epoch r.validator = .error e
  · obtain ⟨e1, he1⟩ := eligible_error p state previous_epoch hprevious hbad
    have hg : (current_epoch == GENESIS_EPOCH) = false := by simpa using hgenesis
    have hspec : process_inactivity_updates p state = .error e1 := by
      unfold process_inactivity_updates
      simp only [bind, Except.bind, hcurrent, hg, he1, Bool.false_eq_true, if_false, pure,
        Except.pure]
    obtain ⟨e2, he2⟩ := passM_error (inactivityRowStep p previous_epoch in_leak) (rowsOf state)
      (by
        obtain ⟨r, hr, e, he⟩ := hbad
        exact ⟨r, hr, e, by simp [inactivityRowStep, he, bind, Except.bind]⟩)
    rw [hspec, he2]
    exact SameOk.errors
  · let Ev : Validator → Bool := fun v => match validatorEligible previous_epoch v with
      | .ok b => b
      | .error _ => false
    have hok : ∀ r ∈ rowsOf state,
        validatorEligible previous_epoch r.validator = .ok (Ev r.validator) := by
      intro r hr
      cases h : validatorEligible previous_epoch r.validator with
      | error e => exact absurd ⟨r, hr, e, h⟩ hbad
      | ok b => simp [Ev, h]
    rw [process_inactivity_updates_eq p state current_epoch previous_epoch _ _ in_leak hcurrent
      hgenesis (eligible_rows p state previous_epoch hprevious Ev hok) hprevious
      (get_unslashed_participating_indices_rows p state hrows current_epoch previous_epoch
        hcurrent hprevious hne) hleak]
    have h := foldlM_inactivityLoopStep_rows p previous_epoch in_leak _ (fun r => Ev r.validator)
      (rowsOf state) [] [] (by simp [rowsOf_map_index, rowsOf_length]) hok
      (fun r hr => mem_participating_rows state previous_epoch hr)
    simp only [List.nil_append, List.append_nil, rowsOf_map_inactivity_score state hrows.2.1] at h
    rw [h]
    apply SameOk.of_eq
    cases hpass : passM (inactivityRowStep p previous_epoch in_leak) () (rowsOf state) with
    | error e => rfl
    | ok x =>
      have hv := passM_map_eq _ (·.validator)
        (fun a r y h => by rw [inactivityRowStep_preserves p previous_epoch in_leak a y.1 r y.2 h])
        () _ x hpass
      have hb := passM_map_eq _ (·.balance)
        (fun a r y h => by rw [inactivityRowStep_preserves p previous_epoch in_leak a y.1 r y.2 h])
        () _ x hpass
      rw [rowsOf_map_validator] at hv
      rw [rowsOf_map_balance state hrows.1] at hb
      simp only [Functor.map, Except.map, BeaconState.withRows, hv, hb]

/-- At the genesis epoch the spec returns the state unchanged. -/
theorem process_inactivity_updates_genesis (p : Preset) (state : BeaconState)
    (hcurrent : get_current_epoch p state = .ok GENESIS_EPOCH) :
    process_inactivity_updates p state = .ok state := by
  unfold process_inactivity_updates
  simp [bind, Except.bind, hcurrent, pure, Except.pure]

end EpochProofs.Spec
