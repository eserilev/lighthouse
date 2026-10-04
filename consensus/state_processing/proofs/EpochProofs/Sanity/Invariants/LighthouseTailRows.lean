import EpochProofs.Sanity.Invariants.LighthouseTailTopups

/-!
# The second half of the Lighthouse row step

`lhRowStepFull` is `lhRowStep` followed by the top-up and the effective balance update of the
row. One pass of it equals a pass of `lhRowStep`, then a pass of the second half. The second pass
over the rows of a state equals one fold over the validator indices, and that fold equals all
top-ups first, then the effective balance updates.
-/

namespace EpochProofs.Spec

/-- The second half of `lhRowStepFull`: the top-up, then the effective balance update unless a
consolidation names the validator. -/
def lhRowStep2 (downward upward : Uint64) (topup : Gwei) (named : Bool) (r : Row) :
    SpecM Row := do
  let balance ← uint64Add r.balance topup
  let r := { r with balance }
  if named then pure r
  else do
    let effective_balance ←
      newEffectiveBalance Preset.mainnet downward upward r.validator r.balance
    pure { r with validator := { r.validator with effective_balance } }

/-- `lhRowStep` keeps the index of the row. -/
theorem lhRowStep_index (ctx : LhStepContext) (base : Gwei) (churn : Epoch × Gwei) (r : Row)
    (x : (Epoch × Gwei) × Row) (h : lhRowStep Preset.mainnet ctx base churn r = .ok x) :
    x.2.index = r.index := by
  unfold lhRowStep at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; rfl)

/-- One row of `lhRowStepFull` is `lhRowStep`, then `lhRowStep2` on the result. -/
theorem lhRowStepFull_eq (ctx : LhStepContext) (downward upward : Uint64)
    (bf : Row → Gwei) (td : Nat → Gwei) (nm : Nat → Bool) (c : Epoch × Gwei) (r : Row) :
    lhRowStepFull Preset.mainnet ctx downward upward ⟨bf r, td r.index, nm r.index⟩ c r =
      (fun x => (x.1.1, x.2)) <$> bothSteps (fun c r => lhRowStep Preset.mainnet ctx (bf r) c r)
        (fun _ r => (fun r' => ((), r')) <$>
          lhRowStep2 downward upward (td r.index) (nm r.index) r) (c, ()) r := by
  unfold lhRowStepFull bothSteps
  dsimp only
  cases hx : lhRowStep Preset.mainnet ctx (bf r) c r with
  | error e => rfl
  | ok x =>
    have hi := lhRowStep_index ctx (bf r) c r x hx
    obtain ⟨c', r'⟩ := x
    simp only at hi
    simp only [bind, Except.bind]
    rw [← hi]
    unfold lhRowStep2
    cases uint64Add r'.balance (td r'.index) with
    | error e => rfl
    | ok b =>
      simp only [bind, Except.bind, Functor.map, Except.map, pure, Except.pure]
      cases nm r'.index with
      | true => rfl
      | false =>
        simp only [Bool.false_eq_true, if_false]
        cases newEffectiveBalance Preset.mainnet downward upward r'.validator b <;> rfl

/-- One pass of `lhRowStepFull` equals a pass of `lhRowStep`, then a pass of `lhRowStep2`. -/
theorem lhRowStepFull_split (ctx : LhStepContext) (downward upward : Uint64)
    (bf : Row → Gwei) (td : Nat → Gwei) (nm : Nat → Bool) (c0 : Epoch × Gwei) (rows : List Row) :
    SameOk (passM (fun c r => lhRowStepFull Preset.mainnet ctx downward upward
        ⟨bf r, td r.index, nm r.index⟩ c r) c0 rows)
      (do
        let x ← passM (fun c r => lhRowStep Preset.mainnet ctx (bf r) c r) c0 rows
        let y ← passM (fun _ r => (fun r' => ((), r')) <$>
          lhRowStep2 downward upward (td r.index) (nm r.index) r) () x.2
        pure (x.1, y.2)) := by
  let f := fun c r => lhRowStep Preset.mainnet ctx (bf r) c r
  let g := fun (_ : Unit) r => (fun r' => ((), r')) <$>
    lhRowStep2 downward upward (td r.index) (nm r.index) r
  have h1 : passM (fun c r => lhRowStepFull Preset.mainnet ctx downward upward
      ⟨bf r, td r.index, nm r.index⟩ c r) c0 rows =
      (fun y => (y.1.1, y.2)) <$> passM (bothSteps f g) (c0, ()) rows := by
    rw [← passM_proj (bothSteps f g) (fun c => (c, ())) (fun b => b.1) (fun _ => rfl)
      (fun _ => rfl)]
    exact passM_congr _ _ rows (fun c r _ => lhRowStepFull_eq ctx downward upward bf td nm c r) c0
  rw [h1]
  refine SameOk.trans (SameOk.map (two_passes_eq_one_pass f g c0 () rows).flip _) ?_
  apply SameOk.of_eq
  unfold twoPasses
  cases passM f c0 rows with
  | error e => rfl
  | ok x =>
    show (fun y => (y.1.1, y.2)) <$> (passM g () x.2 >>= fun y => pure ((x.1, y.1), y.2)) =
      (passM g () x.2 >>= fun y => pure (x.1, y.2))
    cases passM g () x.2 <;> rfl

/-! ## The second pass as a fold over indices -/

/-- One fold step: the top-up of validator `i`, then its update unless it is named. -/
def rowsStep (td : Nat → Gwei) (nm : Nat → Bool) (t : BeaconState) (i : Nat) :
    SpecM BeaconState := do
  let t ← increaseAt i (td i) t
  if nm i then pure t else ebUpdateAt t i

/-- `withRows` of the rows of a state is the state, if the row lists have the right lengths. -/
theorem withRows_rowsOf (S : BeaconState) (hrows : RowsOk S) : S.withRows (rowsOf S) = S := by
  obtain ⟨hb, hi, -, -⟩ := hrows
  have hv := rowsOf_map_validator S
  have hbal : (rowsOf S).map (·.balance) = S.balances := by
    apply List.ext_getElem
    · simp [rowsOf_length, hb]
    · intro i h1 h2
      simp only [List.getElem_map, rowsOf_getElem, List.getD_eq_getElem?_getD,
        List.getElem?_eq_getElem h2, Option.getD_some]
  have hin : (rowsOf S).map (·.inactivity_score) = S.inactivity_scores := by
    apply List.ext_getElem
    · simp [rowsOf_length, hi]
    · intro i h1 h2
      simp only [List.getElem_map, rowsOf_getElem, List.getD_eq_getElem?_getD,
        List.getElem?_eq_getElem h2, Option.getD_some]
  simp only [BeaconState.withRows, hv, hbal, hin]

/-- Writing back the entry that a list already holds changes nothing. -/
theorem map_set_self {α β : Type} (f : α → β) (L : List α) (k : Nat) (r : α)
    (hk : L[k]? = some r) : (L.map f).set k (f r) = L.map f := by
  apply List.ext_getElem?
  intro j
  rw [List.getElem?_set]
  by_cases h : k = j
  · subst h
    have hkl : k < L.length := (List.getElem?_eq_some_iff.mp hk).1
    simp [hkl, (List.getElem?_eq_some_iff.mp hk).2]
  · simp [h]

/-- One fold step on the state of a list of rows is `lhRowStep2` on the row at that index. -/
theorem rowsStep_withRows (S : BeaconState) (downward upward : Uint64)
    (hthr : hysteresisThresholds Preset.mainnet = .ok (downward, upward))
    (td : Nat → Gwei) (nm : Nat → Bool) (L : List Row) (k : Nat) (r : Row)
    (hk : L[k]? = some r) :
    rowsStep td nm (S.withRows L) k =
      (fun r' => S.withRows (L.set k r')) <$> lhRowStep2 downward upward (td k) (nm k) r := by
  have hkl : k < L.length := (List.getElem?_eq_some_iff.mp hk).1
  have hbal : (S.withRows L).balances[k]? = some r.balance := by
    simp [BeaconState.withRows, hk]
  have hval : (S.withRows L).validators[k]? = some r.validator := by
    simp [BeaconState.withRows, hk]
  unfold rowsStep lhRowStep2
  rw [increaseAt_char, hbal]
  have hadd : uint64Add r.balance (td k) =
      if r.balance + td k < UINT64_SIZE then .ok (r.balance + td k) else .error .overflow := by
    unfold uint64Add
    split <;> rfl
  rw [hadd]
  by_cases hfit : r.balance + td k < UINT64_SIZE
  · simp only [hfit, if_true]
    have hset : setBal (S.withRows L) k (r.balance + td k) =
        S.withRows (L.set k { r with balance := r.balance + td k }) := by
      simp only [setBal, BeaconState.withRows, List.map_set]
      rw [map_set_self (·.validator) L k r hk, map_set_self (·.inactivity_score) L k r hk]
    rw [hset]
    show (Except.ok (S.withRows (L.set k { r with balance := r.balance + td k })) >>=
      fun t => if nm k = true then pure t else ebUpdateAt t k) = _
    cases nm k with
    | true => rfl
    | false =>
      simp only [Bool.false_eq_true, if_false]
      have hv' : (S.withRows (L.set k { r with balance := r.balance + td k })).validators[k]? =
          some r.validator := by
        simp [BeaconState.withRows, hkl]
      have hb' : (S.withRows (L.set k { r with balance := r.balance + td k })).balances[k]? =
          some (r.balance + td k) := by
        simp [BeaconState.withRows, hkl]
      show ebUpdateAt _ k = _
      rw [ebUpdateAt_some hv' hb']
      unfold ebNew
      rw [hthr]
      simp only [Functor.map, Except.map, bind, Except.bind, pure, Except.pure]
      cases newEffectiveBalance Preset.mainnet downward upward r.validator (r.balance + td k) with
      | error e => rfl
      | ok e =>
        simp only [Except.ok.injEq]
        rw [setEB_some e hv']
        simp only [BeaconState.withRows, List.map_set, List.set_set]
  · simp only [hfit, if_false]
    rfl

/-- The fold over the indices of a list of rows equals a pass of `lhRowStep2` over the rows. -/
theorem rowsStep_fold (S : BeaconState) (downward upward : Uint64)
    (hthr : hysteresisThresholds Preset.mainnet = .ok (downward, upward))
    (td : Nat → Gwei) (nm : Nat → Bool) :
    ∀ (rs pre : List Row), (∀ j (h : j < rs.length), rs[j].index = pre.length + j) →
      (List.range' pre.length rs.length).foldlM (rowsStep td nm) (S.withRows (pre ++ rs)) =
        (fun y => S.withRows (pre ++ y.2)) <$> passM (fun _ r => (fun r' => ((), r')) <$>
          lhRowStep2 downward upward (td r.index) (nm r.index) r) () rs
  | [], pre, _ => by
    simp [passM, pure, Except.pure, Functor.map, Except.map]
  | r :: rs, pre, hidx => by
    have hr : r.index = pre.length := by
      have h0 := hidx 0 (by simp)
      rw [List.getElem_cons_zero] at h0
      simpa using h0
    rw [List.length_cons, List.range'_succ, List.foldlM_cons]
    have hk : (pre ++ r :: rs)[pre.length]? = some r := by simp
    rw [rowsStep_withRows S downward upward hthr td nm _ _ r hk, passM_cons, hr]
    have hset : ∀ r', (pre ++ r :: rs).set pre.length r' = (pre ++ [r']) ++ rs := by
      intro r'
      simp [List.set_append_right]
    cases lhRowStep2 downward upward (td pre.length) (nm pre.length) r with
    | error e => rfl
    | ok r' =>
      have ih := rowsStep_fold S downward upward hthr td nm rs (pre ++ [r']) (by
        intro j hj
        have := hidx (j + 1) (by simp; omega)
        simp only [List.getElem_cons_succ] at this
        simp only [List.length_append, List.length_singleton]
        omega)
      simp only [List.length_append, List.length_singleton] at ih
      simp only [Functor.map, Except.map, bind, Except.bind] at ih ⊢
      rw [hset r', ih]
      split <;> simp [pure, Except.pure]

/-- The second pass over the rows of a state equals the fold of `rowsStep` over the indices. -/
theorem rows_pass2_fold (S : BeaconState) (hrows : RowsOk S) (downward upward : Uint64)
    (hthr : hysteresisThresholds Preset.mainnet = .ok (downward, upward))
    (td : Nat → Gwei) (nm : Nat → Bool) :
    SameOk ((fun y => S.withRows y.2) <$> passM (fun _ r => (fun r' => ((), r')) <$>
        lhRowStep2 downward upward (td r.index) (nm r.index) r) () (rowsOf S))
      ((List.range S.validators.length).foldlM (fun t i => do
        let t ← increaseAt i (td i) t
        if nm i then pure t else ebUpdateAt t i) S) := by
  apply SameOk.of_eq
  have h := rowsStep_fold S downward upward hthr td nm (rowsOf S) [] (by
    intro j hj
    simp [rowsOf_getElem])
  simp only [List.length_nil, List.nil_append, rowsOf_length, withRows_rowsOf S hrows] at h
  rw [List.range_eq_range']
  exact (h.symm.trans rfl)

/-! ## Top-ups first, then the updates -/

/-- A fold of top-ups commutes with `setEB`. -/
theorem increaseFold_hF1 (td : Nat → Gwei) (i : Nat) (e : Gwei) :
    ∀ (l : List Nat) (t : BeaconState),
      l.foldlM (fun t j => increaseAt j (td j) t) (setEB t i e) =
        (fun t' => setEB t' i e) <$> l.foldlM (fun t j => increaseAt j (td j) t) t
  | [], t => rfl
  | j :: l, t => by
    rw [List.foldlM_cons, List.foldlM_cons, increaseAt_hF1]
    cases increaseAt j (td j) t with
    | error err => rfl
    | ok t1 => exact increaseFold_hF1 td i e l t1

/-- A fold of top-ups at other indices keeps validator `i` and its balance. -/
theorem increaseFold_hF2 (td : Nat → Gwei) (i : Nat) :
    ∀ (l : List Nat), i ∉ l → ∀ (t t' : BeaconState),
      l.foldlM (fun t j => increaseAt j (td j) t) t = .ok t' →
        t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]?
  | [], _, t, t', h => by cases h; exact ⟨rfl, rfl⟩
  | j :: l, hi, t, t', h => by
    rw [List.foldlM_cons] at h
    obtain ⟨t1, h1, h⟩ := specM_bind_ok h
    have hij : i ≠ j := fun e => hi (e ▸ List.mem_cons_self)
    have a := increaseAt_hF2 j (td j) i hij t t1 h1
    have b := increaseFold_hF2 td i l (fun hm => hi (List.mem_cons_of_mem _ hm)) t1 t' h
    exact ⟨b.1.trans a.1, b.2.trans a.2⟩

/-- Over a list of distinct indices, the interleaved fold equals all top-ups first, then the
updates of the validators that are not named. -/
theorem fold_interleave_gen (td : Nat → Gwei) (nm : Nat → Bool) :
    ∀ (l : List Nat), l.Nodup → ∀ S : BeaconState,
      SameOk (l.foldlM (rowsStep td nm) S)
        (do
          let t ← l.foldlM (fun t i => increaseAt i (td i) t) S
          (l.filter (fun i => !nm i)).foldlM ebUpdateAt t)
  | [], _, S => by
    apply SameOk.of_eq
    rfl
  | i :: l, hnd, S => by
    have hil : i ∉ l := (List.nodup_cons.mp hnd).1
    have ih := fold_interleave_gen td nm l (List.nodup_cons.mp hnd).2
    rw [List.foldlM_cons, List.foldlM_cons]
    unfold rowsStep
    simp only [bind_assoc]
    refine SameOk.bind (SameOk.refl _) (fun t1 _ => ?_)
    cases hn : nm i with
    | true =>
      have hf : (i :: l).filter (fun i => !nm i) = l.filter (fun i => !nm i) := by
        simp [hn]
      rw [hf]
      exact ih t1
    | false =>
      have hf : (i :: l).filter (fun i => !nm i) = i :: l.filter (fun i => !nm i) := by
        simp [hn]
      rw [hf]
      simp only [Bool.false_eq_true, if_false, List.foldlM_cons]
      refine SameOk.trans (y := ebUpdateAt t1 i >>= fun t2 =>
        l.foldlM (fun t i => increaseAt i (td i) t) t2 >>= fun t3 =>
          (l.filter (fun i => !nm i)).foldlM ebUpdateAt t3)
        (SameOk.bind (SameOk.refl _) (fun t2 _ => ih t2)) ?_
      refine SameOk.trans (SameOk.of_eq (by rw [← bind_assoc])) ?_
      refine SameOk.trans (SameOk.bind (ebUpdateAt_commute
        (fun t => l.foldlM (fun t i => increaseAt i (td i) t) t) i (fun _ => True)
        (fun t e _ => increaseFold_hF1 td i e l t)
        (fun t t' _ h => increaseFold_hF2 td i l hil t t' h) t1 trivial)
        (fun _ _ => SameOk.refl _)) ?_
      apply SameOk.of_eq
      rw [bind_assoc]

/-- The interleaved fold over `0, …, n - 1` equals all top-ups first, then the updates of the
validators that are not named. -/
theorem fold_interleave (td : Nat → Gwei) (nm : Nat → Bool) (n : Nat) (S : BeaconState) :
    SameOk ((List.range n).foldlM (fun t i => do
        let t ← increaseAt i (td i) t
        if nm i then pure t else ebUpdateAt t i) S)
      (do
        let t ← (List.range n).foldlM (fun t i => increaseAt i (td i) t) S
        ((List.range n).filter (fun i => !nm i)).foldlM ebUpdateAt t) :=
  fold_interleave_gen td nm (List.range n) List.nodup_range S

end EpochProofs.Spec
