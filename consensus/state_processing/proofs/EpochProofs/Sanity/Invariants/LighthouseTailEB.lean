import EpochProofs.Sanity.Invariants.LighthouseTailModel

/-!
# Moving effective balance updates

An effective balance update of one validator reads that validator and its balance, and writes
only that validator's effective balance. So it commutes with every step that keeps them and does
not read the effective balance. The update of all validators equals one update per index, in any
order.
-/

namespace EpochProofs.Spec

/-- An `ok` value followed by more code. -/
private theorem ok_bind' {α β : Type} (a : α) (f : α → SpecM β) :
    (Except.ok a >>= f) = f a := rfl

/-- `setEB` writes the effective balance of a validator in range. -/
theorem setEB_some {s : BeaconState} {i : Nat} {v : Validator} (e : Gwei)
    (h : s.validators[i]? = some v) :
    setEB s i e =
      { s with validators := s.validators.set i { v with effective_balance := e } } := by
  simp [setEB, h]

/-- `setEB` does nothing out of range. -/
theorem setEB_none {s : BeaconState} {i : Nat} (e : Gwei) (h : s.validators[i]? = none) :
    setEB s i e = s := by
  simp [setEB, h]

/-- `setEB` keeps the length of `validators`. -/
theorem setEB_length (s : BeaconState) (i : Nat) (e : Gwei) :
    (setEB s i e).validators.length = s.validators.length := by
  unfold setEB
  split <;> simp

/-- `setEB` keeps every field but `validators`. -/
theorem setEB_eq_with (s : BeaconState) (i : Nat) (e : Gwei) :
    ∃ vs, setEB s i e = { s with validators := vs } := by
  unfold setEB
  split
  · exact ⟨_, rfl⟩
  · exact ⟨s.validators, rfl⟩

/-- The new effective balance of one validator on mainnet. -/
def ebNew (v : Validator) (b : Gwei) : SpecM Gwei := do
  let (downward, upward) ← hysteresisThresholds Preset.mainnet
  newEffectiveBalance Preset.mainnet downward upward v b

/-- `listGet` of a present entry. -/
theorem listGet_of_some {α : Type} {l : List α} {i : Nat} {a : α} (h : l[i]? = some a) :
    listGet l i = .ok a := by
  simp only [listGet, h, pure, Except.pure]

/-- `listGet` of a missing entry. -/
theorem listGet_of_none {α : Type} {l : List α} {i : Nat} (h : l[i]? = none) :
    listGet l i = .error .indexOutOfRange := by
  simp only [listGet, h, throw, throwThe, MonadExceptOf.throw]

/-- `ebUpdateAt` when the validator and the balance exist. -/
theorem ebUpdateAt_some {s : BeaconState} {i : Nat} {v : Validator} {b : Gwei}
    (hv : s.validators[i]? = some v) (hb : s.balances[i]? = some b) :
    ebUpdateAt s i = (setEB s i ·) <$> ebNew v b := by
  have hlt : i < s.validators.length := (List.getElem?_eq_some_iff.mp hv).1
  unfold ebUpdateAt ebNew
  rw [listGet_of_some hv, listGet_of_some hb]
  generalize hysteresisThresholds Preset.mainnet = H
  cases H with
  | error err => rfl
  | ok du =>
    obtain ⟨d, u⟩ := du
    show (newEffectiveBalance Preset.mainnet d u v b >>= fun e => _) =
      (setEB s i ·) <$> newEffectiveBalance Preset.mainnet d u v b
    cases newEffectiveBalance Preset.mainnet d u v b with
    | error err => rfl
    | ok e =>
      show (listSet s.validators i _ >>= fun vs => _) = _
      rw [show listSet s.validators i { v with effective_balance := e } =
        .ok (s.validators.set i { v with effective_balance := e }) from by
          simp only [listSet, hlt, if_true, pure, Except.pure]]
      show _ = Except.ok (setEB s i e)
      rw [setEB_some e hv]
      rfl

/-- `ebUpdateAt` fails without the validator. -/
theorem ebUpdateAt_of_none_v {s : BeaconState} {i : Nat} (hv : s.validators[i]? = none) :
    ebUpdateAt s i = .error .indexOutOfRange := by
  unfold ebUpdateAt
  rw [listGet_of_none hv]
  rfl

/-- `ebUpdateAt` fails without the balance. -/
theorem ebUpdateAt_of_none_b {s : BeaconState} {i : Nat} {v : Validator}
    (hv : s.validators[i]? = some v) (hb : s.balances[i]? = none) :
    ebUpdateAt s i = .error .indexOutOfRange := by
  unfold ebUpdateAt
  rw [listGet_of_some hv, listGet_of_none hb]
  rfl

/-- `ebUpdateAt` succeeds exactly when the validator and the balance exist and the new effective
balance computes. Then it is `setEB`. -/
theorem ebUpdateAt_ok (s s' : BeaconState) (i : Nat) :
    ebUpdateAt s i = .ok s' ↔ ∃ v b e, s.validators[i]? = some v ∧ s.balances[i]? = some b ∧
      ebNew v b = .ok e ∧ s' = setEB s i e := by
  cases hv : s.validators[i]? with
  | none => simp [ebUpdateAt_of_none_v hv]
  | some v =>
    cases hb : s.balances[i]? with
    | none => simp [ebUpdateAt_of_none_b hv hb]
    | some b =>
      rw [ebUpdateAt_some hv hb]
      cases he : ebNew v b with
      | error err =>
        simp only [Functor.map, Except.map, reduceCtorEq, false_iff]
        rintro ⟨_, _, _, ⟨⟩, ⟨⟩, he', -⟩
        rw [he] at he'
        cases he'
      | ok e =>
        simp only [Functor.map, Except.map, Except.ok.injEq]
        constructor
        · rintro rfl
          exact ⟨v, b, e, rfl, rfl, he, rfl⟩
        · rintro ⟨_, _, _, ⟨⟩, ⟨⟩, he', rfl⟩
          rw [he] at he'
          cases he'
          rfl

/-! ## Commuting one update with a step -/

/-- An update of validator `i` commutes with a step `F` that commutes with `setEB` at `i` and
keeps validator `i` and its balance. -/
theorem ebUpdateAt_commute (F : BeaconState → SpecM BeaconState) (i : Nat)
    (P : BeaconState → Prop)
    (hF1 : ∀ t e, P t → F (setEB t i e) = (fun t' => setEB t' i e) <$> F t)
    (hF2 : ∀ t t', P t → F t = .ok t' →
      t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]?)
    (s : BeaconState) (hs : P s) :
    SameOk (ebUpdateAt s i >>= F) (F s >>= fun t => ebUpdateAt t i) := by
  intro w
  constructor
  · intro h
    obtain ⟨s1, h1, h2⟩ := specM_bind_ok h
    obtain ⟨v, b, e, hv, hb, he, rfl⟩ := (ebUpdateAt_ok s s1 i).mp h1
    rw [hF1 s e hs] at h2
    cases ht : F s with
    | error err => rw [ht] at h2; cases h2
    | ok t =>
      rw [ht] at h2
      cases h2
      obtain ⟨hvt, hbt⟩ := hF2 s t hs ht
      show (Except.ok t >>= fun t => ebUpdateAt t i) = _
      exact (ebUpdateAt_ok t _ i).mpr ⟨v, b, e, hvt.trans hv, hbt.trans hb, he, rfl⟩
  · intro h
    obtain ⟨t, ht, h2⟩ := specM_bind_ok h
    obtain ⟨v, b, e, hv, hb, he, rfl⟩ := (ebUpdateAt_ok t w i).mp h2 |>.imp fun _ x => x
    obtain ⟨hvt, hbt⟩ := hF2 s t hs ht
    have h1 : ebUpdateAt s i = .ok (setEB s i e) :=
      (ebUpdateAt_ok s _ i).mpr ⟨v, b, e, hvt ▸ hv, hbt ▸ hb, he, rfl⟩
    rw [h1]
    show F (setEB s i e) = _
    rw [hF1 s e hs, ht]
    rfl

/-- Updates of a list of validators commute with a step that commutes with each of them. -/
theorem ebFold_commute (F : BeaconState → SpecM BeaconState) (P : BeaconState → Prop)
    (l : List Nat) (hP : ∀ i ∈ l, ∀ t e, P t → P (setEB t i e))
    (hF1 : ∀ i ∈ l, ∀ t e, P t → F (setEB t i e) = (fun t' => setEB t' i e) <$> F t)
    (hF2 : ∀ i ∈ l, ∀ t t', P t → F t = .ok t' →
      t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]?)
    (s : BeaconState) (hs : P s) :
    SameOk (l.foldlM ebUpdateAt s >>= F) (F s >>= fun t => l.foldlM ebUpdateAt t) := by
  induction l generalizing s with
  | nil =>
    apply SameOk.of_eq
    show F s = (F s >>= pure)
    rw [bind_pure]
  | cons i l ih =>
    have hPi := hP i (by simp)
    refine SameOk.trans (SameOk.of_eq (by rw [List.foldlM_cons, bind_assoc])) ?_
    refine SameOk.trans
      (y := ebUpdateAt s i >>= fun s1 => F s1 >>= fun t' => l.foldlM ebUpdateAt t')
      (SameOk.bind (SameOk.refl (ebUpdateAt s i)) (fun s1 h1 => ?_)) ?_
    · obtain ⟨v, b, e, -, -, -, rfl⟩ := (ebUpdateAt_ok s s1 i).mp h1
      exact ih (fun j hj => hP j (by simp [hj])) (fun j hj => hF1 j (by simp [hj]))
        (fun j hj => hF2 j (by simp [hj])) _ (hPi s e hs)
    · refine SameOk.trans (SameOk.of_eq (by rw [← bind_assoc])) ?_
      refine SameOk.trans (SameOk.bind (ebUpdateAt_commute F i P (hF1 i (by simp))
        (hF2 i (by simp)) s hs) (fun _ _ => SameOk.refl _)) ?_
      apply SameOk.of_eq
      rw [bind_assoc]
      rfl


/-! ## Steps that do not read the validators -/

/-- A step that passes any `validators` list through and does not change it commutes with
`setEB`. -/
theorem setEB_blind (F : BeaconState → SpecM BeaconState) (i : Nat)
    (hB : ∀ t vs, F { t with validators := vs } = (fun t' => { t' with validators := vs }) <$> F t)
    (hV : ∀ t t', F t = .ok t' → t'.validators = t.validators) (t : BeaconState) (e : Gwei) :
    F (setEB t i e) = (fun t' => setEB t' i e) <$> F t := by
  cases hv : t.validators[i]? with
  | none =>
    rw [setEB_none e hv]
    cases hF : F t with
    | error err => rfl
    | ok t' =>
      show Except.ok t' = Except.ok (setEB t' i e)
      rw [setEB_none e (by rw [hV t t' hF]; exact hv)]
  | some v =>
    rw [setEB_some e hv, hB]
    cases hF : F t with
    | error err => rfl
    | ok t' =>
      have h := hV t t' hF
      show Except.ok { t' with validators := _ } = Except.ok (setEB t' i e)
      rw [setEB_some e (by rw [h]; exact hv), h]

/-- `process_builder_pending_payments` passes `validators` through. -/
theorem payments_blind (tab : Gwei) (t : BeaconState) (vs : List Validator) :
    process_builder_pending_payments Preset.mainnet tab { t with validators := vs } =
      (fun t' => { t' with validators := vs }) <$>
        process_builder_pending_payments Preset.mainnet tab t := by
  unfold process_builder_pending_payments
  simp only [bind, Except.bind, pure, Except.pure, Functor.map, Except.map]
  repeat' split
  all_goals rfl


/-- `process_builder_pending_payments` keeps `validators` and `balances`. -/
theorem payments_frame (tab : Gwei) (t t' : BeaconState)
    (h : process_builder_pending_payments Preset.mainnet tab t = .ok t') :
    t'.validators = t.validators ∧ t'.balances = t.balances := by
  unfold process_builder_pending_payments at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨rfl, rfl⟩)

/-- Instance a: `process_builder_pending_payments` commutes with `setEB`. -/
theorem payments_hF1 (tab : Gwei) (i : Nat) (t : BeaconState) (e : Gwei) :
    process_builder_pending_payments Preset.mainnet tab (setEB t i e) =
      (fun t' => setEB t' i e) <$> process_builder_pending_payments Preset.mainnet tab t :=
  setEB_blind _ i (payments_blind tab) (fun t t' h => (payments_frame tab t t' h).1) t e

/-- Instance a: `process_builder_pending_payments` keeps validator `i` and its balance. -/
theorem payments_hF2 (tab : Gwei) (i : Nat) (t t' : BeaconState)
    (h : process_builder_pending_payments Preset.mainnet tab t = .ok t') :
    t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]? := by
  obtain ⟨hv, hb⟩ := payments_frame tab t t' h
  rw [hv, hb]
  exact ⟨rfl, rfl⟩

/-- `process_eth1_data_reset` passes `validators` through. -/
theorem eth1_blind (t : BeaconState) (vs : List Validator) :
    process_eth1_data_reset Preset.mainnet { t with validators := vs } =
      (fun t' => { t' with validators := vs }) <$> process_eth1_data_reset Preset.mainnet t := by
  unfold process_eth1_data_reset
  have hc : get_current_epoch Preset.mainnet { t with validators := vs } =
      get_current_epoch Preset.mainnet t := rfl
  rw [hc]
  simp only [bind, Except.bind, pure, Except.pure, Functor.map, Except.map]
  repeat' split
  all_goals first
    | rfl
    | (rename_i h; cases h; rfl)
    | (rename_i h; cases h)

/-- `process_eth1_data_reset` keeps `validators` and `balances`. -/
theorem eth1_frame (t t' : BeaconState) (h : process_eth1_data_reset Preset.mainnet t = .ok t') :
    t'.validators = t.validators ∧ t'.balances = t.balances := by
  unfold process_eth1_data_reset at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨rfl, rfl⟩)

/-- Instance b: `process_eth1_data_reset` commutes with `setEB`. -/
theorem eth1_hF1 (i : Nat) (t : BeaconState) (e : Gwei) :
    process_eth1_data_reset Preset.mainnet (setEB t i e) =
      (fun t' => setEB t' i e) <$> process_eth1_data_reset Preset.mainnet t :=
  setEB_blind _ i eth1_blind (fun t t' h => (eth1_frame t t' h).1) t e

/-- Instance b: `process_eth1_data_reset` keeps validator `i` and its balance. -/
theorem eth1_hF2 (i : Nat) (t t' : BeaconState)
    (h : process_eth1_data_reset Preset.mainnet t = .ok t') :
    t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]? := by
  obtain ⟨hv, hb⟩ := eth1_frame t t' h
  rw [hv, hb]
  exact ⟨rfl, rfl⟩

/-- The pending deposit queue write of Lighthouse. -/
def writeQueue (q : List PendingDeposit) (d : Gwei) (t : BeaconState) : SpecM BeaconState :=
  pure { t with pending_deposits := q, deposit_balance_to_consume := d }

/-- Instance c: the queue write commutes with `setEB`. -/
theorem writeQueue_hF1 (q : List PendingDeposit) (d : Gwei) (i : Nat) (t : BeaconState)
    (e : Gwei) :
    writeQueue q d (setEB t i e) = (fun t' => setEB t' i e) <$> writeQueue q d t :=
  setEB_blind _ i (fun _ _ => rfl) (fun _ _ h => by cases h; rfl) t e

/-- Instance c: the queue write keeps validator `i` and its balance. -/
theorem writeQueue_hF2 (q : List PendingDeposit) (d : Gwei) (i : Nat) (t t' : BeaconState)
    (h : writeQueue q d t = .ok t') :
    t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]? := by
  cases h
  exact ⟨rfl, rfl⟩

/-- A balance increase as a state step. -/
def increaseAt (j : Nat) (a : Gwei) (t : BeaconState) : SpecM BeaconState := do
  pure { t with balances := ← increase_balance t.balances j a }

/-- `increase_balance` changes only the entry at its index. -/
theorem increase_balance_other {bs bs' : List Gwei} {j : Nat} {a : Gwei}
    (h : increase_balance bs j a = .ok bs') (i : Nat) (hij : i ≠ j) : bs'[i]? = bs[i]? := by
  unfold increase_balance at h
  obtain ⟨x, -, h⟩ := specM_bind_ok h
  obtain ⟨y, -, h⟩ := specM_bind_ok h
  unfold listSet at h
  split at h
  · cases h
    exact List.getElem?_set_ne (Ne.symm hij)
  · cases h

/-- Instance d: a balance increase commutes with `setEB`. -/
theorem increaseAt_hF1 (j : Nat) (a : Gwei) (i : Nat) (t : BeaconState) (e : Gwei) :
    increaseAt j a (setEB t i e) = (fun t' => setEB t' i e) <$> increaseAt j a t := by
  refine setEB_blind _ i (fun t vs => ?_) (fun t t' h => ?_) t e
  · unfold increaseAt
    cases increase_balance t.balances j a <;> rfl
  · unfold increaseAt at h
    obtain ⟨_, -, h⟩ := specM_bind_ok h
    cases h
    rfl

/-- Instance d: a balance increase at another index keeps validator `i` and its balance. -/
theorem increaseAt_hF2 (j : Nat) (a : Gwei) (i : Nat) (hij : i ≠ j) (t t' : BeaconState)
    (h : increaseAt j a t = .ok t') :
    t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]? := by
  unfold increaseAt at h
  obtain ⟨bs, hbs, h⟩ := specM_bind_ok h
  cases h
  exact ⟨rfl, increase_balance_other hbs i hij⟩


/-! ## Two updates commute -/

/-- `setEB` at `i` keeps the other validators. -/
theorem setEB_get_ne (t : BeaconState) {i j : Nat} (e : Gwei) (hij : j ≠ i) :
    (setEB t i e).validators[j]? = t.validators[j]? := by
  unfold setEB
  split
  · exact List.getElem?_set_ne (Ne.symm hij)
  · rfl

/-- `setEB` keeps the balances. -/
theorem setEB_balances (t : BeaconState) (i : Nat) (e : Gwei) :
    (setEB t i e).balances = t.balances := by
  obtain ⟨vs, h⟩ := setEB_eq_with t i e
  rw [h]

/-- `setEB` at two different indices commute. -/
theorem setEB_comm (t : BeaconState) {i j : Nat} (e e' : Gwei) (hij : i ≠ j) :
    setEB (setEB t i e) j e' = setEB (setEB t j e') i e := by
  cases hi : t.validators[i]? with
  | none =>
    rw [setEB_none e hi, setEB_none e (by rw [setEB_get_ne t e' hij]; exact hi)]
  | some v =>
    cases hj : t.validators[j]? with
    | none =>
      rw [setEB_none e' hj, setEB_none e' (by rw [setEB_get_ne t e (Ne.symm hij)]; exact hj)]
    | some w =>
      rw [setEB_some e hi, setEB_some e' hj,
        setEB_some e' (v := w) (by rw [List.getElem?_set_ne hij]; exact hj),
        setEB_some e (v := v) (by rw [List.getElem?_set_ne (Ne.symm hij)]; exact hi)]
      simp only [List.set_comm _ _ hij]

/-- Instance g: an update at `j` commutes with `setEB` at `i ≠ j`. -/
theorem ebUpdateAt_hF1 (j i : Nat) (hij : i ≠ j) (t : BeaconState) (e : Gwei) :
    ebUpdateAt (setEB t i e) j = (fun t' => setEB t' i e) <$> ebUpdateAt t j := by
  have hv' := setEB_get_ne t e (Ne.symm hij)
  have hb' := setEB_balances t i e
  cases hv : t.validators[j]? with
  | none =>
    rw [ebUpdateAt_of_none_v hv, ebUpdateAt_of_none_v (by rw [hv']; exact hv)]
    rfl
  | some v =>
    cases hb : t.balances[j]? with
    | none =>
      rw [ebUpdateAt_of_none_b hv hb, ebUpdateAt_of_none_b (by rw [hv']; exact hv)
        (by rw [hb']; exact hb)]
      rfl
    | some b =>
      rw [ebUpdateAt_some hv hb, ebUpdateAt_some (by rw [hv']; exact hv) (by rw [hb']; exact hb)]
      cases ebNew v b with
      | error err => rfl
      | ok e' =>
        show Except.ok (setEB (setEB t i e) j e') = Except.ok (setEB (setEB t j e') i e)
        rw [setEB_comm t e e' hij]

/-- Instance g: an update at `j` keeps validator `i ≠ j` and its balance. -/
theorem ebUpdateAt_hF2 (j i : Nat) (hij : i ≠ j) (t t' : BeaconState)
    (h : ebUpdateAt t j = .ok t') :
    t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]? := by
  obtain ⟨_, _, e, -, -, -, rfl⟩ := (ebUpdateAt_ok t t' j).mp h
  exact ⟨setEB_get_ne t e hij, by rw [setEB_balances]⟩

/-- Two updates at different indices commute. -/
theorem ebUpdateAt_swap (s : BeaconState) (i j : Nat) :
    SameOk (ebUpdateAt s i >>= fun t => ebUpdateAt t j)
      (ebUpdateAt s j >>= fun t => ebUpdateAt t i) := by
  by_cases hij : i = j
  · subst hij
    exact SameOk.refl _
  · exact ebUpdateAt_commute (fun t => ebUpdateAt t j) i (fun _ => True)
      (fun t e _ => ebUpdateAt_hF1 j i hij t e) (fun t t' _ h => ebUpdateAt_hF2 j i hij t t' h)
      s trivial

/-- Updates over two permuted lists give the same `ok` results. -/
theorem ebFold_perm {l l' : List Nat} (h : l.Perm l') (s : BeaconState) :
    SameOk (l.foldlM ebUpdateAt s) (l'.foldlM ebUpdateAt s) := by
  induction h generalizing s with
  | nil => exact SameOk.refl _
  | cons x _ ih =>
    rw [List.foldlM_cons, List.foldlM_cons]
    exact SameOk.bind (SameOk.refl _) (fun t _ => ih t)
  | swap x y l =>
    simp only [List.foldlM_cons]
    refine SameOk.trans (SameOk.of_eq (bind_assoc _ _ _).symm) ?_
    refine SameOk.trans (SameOk.bind (ebUpdateAt_swap s y x) (fun _ _ => SameOk.refl _)) ?_
    exact SameOk.of_eq (bind_assoc _ _ _)
  | trans _ _ ih1 ih2 => exact (ih1 s).trans (ih2 s)


/-! ## All updates as one update per index -/

/-- One spec loop step when the balance exists. -/
theorem effectiveBalanceStep_some {bals : List Gwei} {k : Nat} {b : Gwei} (v : Validator)
    (hb : bals[k]? = some b) :
    effectiveBalanceStep Preset.mainnet bals (v, k) =
      (fun e => { v with effective_balance := e }) <$> ebNew v b := by
  unfold effectiveBalanceStep ebNew
  simp only [hb]
  generalize hysteresisThresholds Preset.mainnet = H
  cases H with
  | error err => rfl
  | ok du =>
    obtain ⟨d, u⟩ := du
    show (newEffectiveBalance Preset.mainnet d u v b >>= fun e => _) =
      (fun e => { v with effective_balance := e }) <$> newEffectiveBalance Preset.mainnet d u v b
    cases newEffectiveBalance Preset.mainnet d u v b <;> rfl

/-- One spec loop step fails without the balance. -/
theorem effectiveBalanceStep_none {bals : List Gwei} {k : Nat} (v : Validator)
    (hb : bals[k]? = none) :
    effectiveBalanceStep Preset.mainnet bals (v, k) = .error .indexOutOfRange := by
  unfold effectiveBalanceStep
  simp only [hb]
  rfl

/-- The updates of the indices from `done.length` on equal the spec loop over the rest. -/
theorem ebFold_range_eq (s : BeaconState) :
    ∀ (vs done : List Validator),
      (List.range' done.length vs.length).foldlM ebUpdateAt
          { s with validators := done ++ vs } =
        ((vs.zipIdx done.length).mapM (effectiveBalanceStep Preset.mainnet s.balances)).map
          fun ys => { s with validators := done ++ ys }
  | [], done => by
    simp [pure, Except.pure, Except.map]
  | v :: vs, done => by
    rw [List.length_cons, List.range'_succ, List.foldlM_cons, List.zipIdx_cons, List.mapM_cons]
    have hv : ({ s with validators := done ++ v :: vs } : BeaconState).validators[done.length]? =
        some v := by simp
    cases hb : s.balances[done.length]? with
    | none =>
      rw [ebUpdateAt_of_none_b hv hb, effectiveBalanceStep_none v hb]
      rfl
    | some b =>
      rw [ebUpdateAt_some hv hb, effectiveBalanceStep_some v hb]
      cases ebNew v b with
      | error err => rfl
      | ok e =>
        have hset : setEB { s with validators := done ++ v :: vs } done.length e =
            { s with validators := (done ++ [{ v with effective_balance := e }]) ++ vs } := by
          rw [setEB_some e hv]
          simp
        show ((List.range' (done.length + 1) vs.length).foldlM ebUpdateAt
          (setEB { s with validators := done ++ v :: vs } done.length e)) = _
        rw [hset]
        have ih := ebFold_range_eq s vs (done ++ [{ v with effective_balance := e }])
        simp only [List.length_append, List.length_singleton] at ih
        rw [ih]
        cases (vs.zipIdx (done.length + 1)).mapM (effectiveBalanceStep Preset.mainnet s.balances)
        · rfl
        · simp [Except.map, bind, Except.bind, pure, Except.pure, Functor.map]

/-- The update of all validators equals one update per index, in index order. -/
theorem process_effective_balance_updates_fold (s : BeaconState) :
    SameOk (process_effective_balance_updates Preset.mainnet s)
      ((List.range s.validators.length).foldlM ebUpdateAt s) := by
  apply SameOk.of_eq
  have h := ebFold_range_eq s s.validators []
  simp only [List.length_nil, List.nil_append] at h
  rw [process_effective_balance_updates_eq, List.range_eq_range', ← h]


/-! ## Loops that pass a part of the state through -/

/-- `φ` on the value of a loop step. -/
def stepMap {β : Type} (φ : β → β) : ForInStep β → ForInStep β
  | .done b => .done (φ b)
  | .yield b => .yield (φ b)

/-- A loop from `φ b` is `φ` of the loop from `b`, if each step commutes with `φ` on states
with `Q`, and the steps keep `Q`. -/
theorem forIn_sim_eq {α β : Type} (l : List α) (f : α → β → SpecM (ForInStep β)) (φ : β → β)
    (Q : β → Prop) (hg : ∀ x ∈ l, ∀ b, Q b → f x (φ b) = stepMap φ <$> f x b)
    (hQ : ∀ x ∈ l, ∀ b r, Q b → f x b = .ok r → Q r.value) :
    ∀ (b b' : β), b' = φ b → Q b → forIn l b' f = φ <$> forIn l b f := by
  induction l with
  | nil =>
    intro b b' hb' _
    subst hb'
    rfl
  | cons x xs ih =>
    intro b b' hb' hQb
    subst hb'
    rw [List.forIn_cons, List.forIn_cons, hg x (by simp) b hQb]
    cases hfx : f x b with
    | error err => rfl
    | ok r =>
      have hQr := hQ x (by simp) b r hQb hfx
      cases r with
      | done c => rfl
      | yield c =>
        show forIn xs (φ c) f = φ <$> forIn xs c f
        exact ih (fun y hy => hg y (by simp [hy])) (fun y hy => hQ y (by simp [hy])) c _ rfl hQr


/-- A map then a bind is a bind then a map, if the continuations commute with the maps. -/
theorem map_bind_eq {β γ : Type} {φ : β → β} {g : γ → γ} {k1 k2 : β → SpecM γ} (x : SpecM β)
    (h : ∀ r, k1 (φ r) = g <$> k2 r) : ((φ <$> x) >>= k1) = g <$> (x >>= k2) := by
  cases x with
  | error err => rfl
  | ok r => exact h r

/-- `process_pending_consolidations` passes `validators` through when the new list agrees at
each source index. -/
theorem consolidations_blind (t : BeaconState) (vs : List Validator)
    (hvs : ∀ c ∈ t.pending_consolidations, vs[c.source_index]? = t.validators[c.source_index]?) :
    process_pending_consolidations Preset.mainnet { t with validators := vs } =
      (fun t' => { t' with validators := vs }) <$>
        process_pending_consolidations Preset.mainnet t := by
  unfold process_pending_consolidations
  have hc : get_current_epoch Preset.mainnet { t with validators := vs } =
      get_current_epoch Preset.mainnet t := rfl
  rw [hc]
  cases get_current_epoch Preset.mainnet t with
  | error err => rfl
  | ok ce =>
    simp only [ok_bind']
    cases uint64Add ce 1 with
    | error err => rfl
    | ok ne =>
      simp only [ok_bind']

      rw [forIn_sim_eq t.pending_consolidations _
        (fun b : MProd Nat BeaconState => ⟨b.fst, { b.snd with validators := vs }⟩)
        (fun b => b.snd.validators = t.validators) ?_ ?_ ⟨0, t⟩ _ rfl rfl]
      · refine map_bind_eq _ (fun r => ?_)
        rfl
      · intro x hx ⟨n, st⟩ hst
        simp only at hst
        have hsrc : listGet vs x.source_index = listGet st.validators x.source_index := by
          simp only [listGet, hvs x hx, hst]
        simp only [hsrc]
        simp only [bind, Except.bind, pure, Except.pure, Functor.map, Except.map]
        repeat' split
        all_goals first
          | rfl
          | (rename_i h; cases h; rfl)
          | (rename_i h; cases h)
      · intro x _ ⟨n, st⟩ r hst h
        simp only at hst
        simp only [bind, Except.bind, pure, Except.pure] at h
        repeat' split at h
        all_goals first
          | contradiction
          | (cases h; exact hst)


/-- A validator outside `consolidationIndices` is neither a source nor a target. -/
theorem not_mem_consolidationIndices {t : BeaconState} {i : Nat}
    (h : i ∉ consolidationIndices t) :
    ∀ c ∈ t.pending_consolidations, c.source_index ≠ i ∧ c.target_index ≠ i := by
  intro c hc
  unfold consolidationIndices at h
  rw [List.mem_mergeSort, List.mem_eraseDups, List.mem_flatMap] at h
  constructor
  · intro hs
    exact h ⟨c, hc, by simp [hs]⟩
  · intro ht
    exact h ⟨c, hc, by simp [ht]⟩

/-- A `for` loop in `SpecM` keeps every property that each successful pass keeps. -/
theorem forIn_ok_inv {α β : Type} (P : β → Prop) (f : α → β → SpecM (ForInStep β)) :
    ∀ (l : List α), (∀ a ∈ l, ∀ b r, P b → f a b = .ok r → P r.value) →
      ∀ (b b' : β), P b → forIn l b f = .ok b' → P b' := by
  intro l
  induction l with
  | nil => intro _ b b' hb h; cases h; exact hb
  | cons a as ih =>
    intro hf b b' hb h
    rw [List.forIn_cons] at h
    obtain ⟨r, hr, h⟩ := specM_bind_ok h
    have := hf a (by simp) b r hb hr
    cases r with
    | done c => cases h; exact this
    | yield c => exact ih (fun x hx => hf x (by simp [hx])) c b' this h

/-- `decrease_balance` changes only the entry at its index. -/
theorem decrease_balance_other {bs bs' : List Gwei} {j : Nat} {a : Gwei}
    (h : decrease_balance bs j a = .ok bs') (i : Nat) (hij : i ≠ j) : bs'[i]? = bs[i]? := by
  unfold decrease_balance at h
  obtain ⟨x, -, h⟩ := specM_bind_ok h
  unfold listSet at h
  split at h
  · cases h
    exact List.getElem?_set_ne (Ne.symm hij)
  · cases h

/-- `process_pending_consolidations` keeps `validators`, and the balance of a validator that no
consolidation names. -/
theorem consolidations_frame (t t' : BeaconState)
    (h : process_pending_consolidations Preset.mainnet t = .ok t') :
    t'.validators = t.validators ∧ ∀ i, i ∉ consolidationIndices t →
      t'.balances[i]? = t.balances[i]? := by
  let Fr : BeaconState → Prop := fun st => st.validators = t.validators ∧
    ∀ i, (∀ c ∈ t.pending_consolidations, c.source_index ≠ i ∧ c.target_index ≠ i) →
      st.balances[i]? = t.balances[i]?
  have key : Fr t' := by
    unfold process_pending_consolidations at h
    obtain ⟨_, -, h⟩ := specM_bind_ok h
    obtain ⟨_, -, h⟩ := specM_bind_ok h
    obtain ⟨⟨n, st⟩, hr, h⟩ := specM_bind_ok h
    have hP := forIn_ok_inv (fun b : MProd Nat BeaconState => Fr b.snd) _ _ ?_
      ⟨0, t⟩ _ ⟨rfl, fun _ _ => rfl⟩ hr
    · cases h
      exact hP
    · intro x hx ⟨n0, st0⟩ r ⟨hv0, hb0⟩ hb
      simp only [bind, Except.bind, pure, Except.pure] at hb
      repeat' split at hb
      all_goals first
        | contradiction
        | (cases hb; exact ⟨hv0, hb0⟩)
        | (cases hb
           refine ⟨hv0, fun i hi => ?_⟩
           rw [increase_balance_other ‹increase_balance _ _ _ = _› i (hi x hx).2.symm,
             decrease_balance_other ‹decrease_balance _ _ _ = _› i (hi x hx).1.symm]
           exact hb0 i hi)
  exact ⟨key.1, fun i hi => key.2 i (not_mem_consolidationIndices hi)⟩


/-- `setEB` keeps `consolidationIndices`. -/
theorem consolidationIndices_setEB (t : BeaconState) (i : Nat) (e : Gwei) :
    consolidationIndices (setEB t i e) = consolidationIndices t := by
  obtain ⟨vs, h⟩ := setEB_eq_with t i e
  rw [h]
  rfl

/-- Instance f: `process_pending_consolidations` commutes with `setEB` at a validator that no
consolidation names. -/
theorem consolidations_hF1 (i : Nat) (t : BeaconState) (e : Gwei)
    (hi : i ∉ consolidationIndices t) :
    process_pending_consolidations Preset.mainnet (setEB t i e) =
      (fun t' => setEB t' i e) <$> process_pending_consolidations Preset.mainnet t := by
  cases hv : t.validators[i]? with
  | none =>
    rw [setEB_none e hv]
    cases hF : process_pending_consolidations Preset.mainnet t with
    | error err => rfl
    | ok t' =>
      show Except.ok t' = Except.ok (setEB t' i e)
      rw [setEB_none e (by rw [(consolidations_frame t t' hF).1]; exact hv)]
  | some v =>
    rw [setEB_some e hv, consolidations_blind t _ (fun c hc =>
      List.getElem?_set_ne (not_mem_consolidationIndices hi c hc).1.symm)]
    cases hF : process_pending_consolidations Preset.mainnet t with
    | error err => rfl
    | ok t' =>
      have h := (consolidations_frame t t' hF).1
      show Except.ok { t' with validators := _ } = Except.ok (setEB t' i e)
      rw [setEB_some e (by rw [h]; exact hv), h]

/-- Instance f: `process_pending_consolidations` keeps a validator that no consolidation names,
and its balance. -/
theorem consolidations_hF2 (i : Nat) (t t' : BeaconState) (hi : i ∉ consolidationIndices t)
    (h : process_pending_consolidations Preset.mainnet t = .ok t') :
    t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]? := by
  obtain ⟨hv, hb⟩ := consolidations_frame t t' h
  exact ⟨by rw [hv], hb i hi⟩


/-! ## Pending deposits for other pubkeys -/

/-- `idxOf` finds a member. -/
theorem getElem?_idxOf {α : Type} [BEq α] [LawfulBEq α] {a : α} :
    ∀ {l : List α}, a ∈ l → l[l.idxOf a]? = some a
  | [], h => by simp at h
  | b :: l, h => by
    rw [List.idxOf_cons]
    by_cases hb : b = a
    · subst hb; simp
    · have hm : a ∈ l := by
        rcases List.mem_cons.mp h with h | h
        · exact absurd h.symm hb
        · exact h
      have hbe : (b == a) = false := by simpa using hb
      rw [hbe]
      simp only [cond_false, List.getElem?_cons_succ]
      exact getElem?_idxOf hm

/-- `set_or_append_list` at the length appends. -/
theorem set_or_append_list_len {α : Type} (l : List α) (a : α) :
    set_or_append_list l l.length a = .ok (l ++ [a]) := by
  simp [set_or_append_list, pure, Except.pure]

/-- `add_validator_to_registry` on another `validators` list of the same length appends the same
validator to that list. -/
theorem add_validator_blind (t : BeaconState) (vs : List Validator)
    (hlen : vs.length = t.validators.length) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei) :
    add_validator_to_registry Preset.mainnet { t with validators := vs } pubkey
        withdrawal_credentials amount =
      (fun t' => { t' with validators := vs ++ t'.validators.drop t.validators.length }) <$>
        add_validator_to_registry Preset.mainnet t pubkey withdrawal_credentials amount := by
  unfold add_validator_to_registry get_index_for_new_validator
  simp only [set_or_append_list_len]
  simp only [hlen, bind, Except.bind, pure, Except.pure, Functor.map, Except.map]
  repeat' split
  all_goals first
    | rfl
    | simp
    | (rename_i h; cases h; rfl)
    | (rename_i h; cases h)


/-- `apply_pending_deposit` on another `validators` list with the same pubkeys acts on that list
the same way. -/
theorem apply_blind (o : Oracle) (d : PendingDeposit) (t : BeaconState) (vs : List Validator)
    (hpk : vs.map (·.pubkey) = t.validators.map (·.pubkey)) :
    apply_pending_deposit Preset.mainnet o { t with validators := vs } d =
      (fun t' => { t' with validators := vs ++ t'.validators.drop t.validators.length }) <$>
        apply_pending_deposit Preset.mainnet o t d := by
  have hlen : vs.length = t.validators.length := by
    rw [← List.length_map (f := (·.pubkey)), hpk, List.length_map]
  unfold apply_pending_deposit
  dsimp only
  rw [hpk]
  split
  · split
    · exact add_validator_blind t vs hlen _ _ _
    · simp [pure, Except.pure, Functor.map, Except.map]
  · cases increase_balance t.balances ((t.validators.map (·.pubkey)).idxOf d.pubkey) d.amount with
    | error err => rfl
    | ok bs => simp [bind, Except.bind, pure, Except.pure, Functor.map, Except.map]


/-- `add_validator_to_registry` appends to `validators` and keeps the balances of the existing
validators. -/
theorem add_validator_frame (t t' : BeaconState) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei)
    (h : add_validator_to_registry Preset.mainnet t pubkey withdrawal_credentials amount =
      .ok t') :
    (∃ ys, t'.validators = t.validators ++ ys) ∧
      ∀ i, i < t.validators.length → t'.balances[i]? = t.balances[i]? := by
  unfold add_validator_to_registry get_index_for_new_validator at h
  simp only [set_or_append_list_len, bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals try contradiction
  cases h
  refine ⟨⟨_, rfl⟩, fun i hi => ?_⟩
  have hbal := ‹set_or_append_list t.balances t.validators.length amount = _›
  unfold set_or_append_list at hbal
  split at hbal
  · rename_i heq
    simp only [pure, Except.pure, Except.ok.injEq] at hbal
    subst hbal
    have he : t.validators.length = t.balances.length := by simpa using heq
    exact List.getElem?_append_left (he ▸ hi)
  · unfold listSet at hbal
    split at hbal
    · cases hbal
      exact List.getElem?_set_ne (Nat.ne_of_gt hi)
    · cases hbal


/-- `apply_pending_deposit` appends to `validators`, and keeps the balance of an existing
validator with another pubkey. -/
theorem apply_frame (o : Oracle) (d : PendingDeposit) (t t' : BeaconState)
    (h : apply_pending_deposit Preset.mainnet o t d = .ok t') :
    (∃ ys, t'.validators = t.validators ++ ys) ∧
      ∀ i, i < t.validators.length → (∀ v, t.validators[i]? = some v → v.pubkey ≠ d.pubkey) →
        t'.balances[i]? = t.balances[i]? := by
  unfold apply_pending_deposit at h
  by_cases hm : d.pubkey ∈ t.validators.map (·.pubkey)
  · simp only [hm, not_true_eq_false, if_false] at h
    obtain ⟨bs, hbs, h⟩ := specM_bind_ok h
    cases h
    refine ⟨⟨[], by simp⟩, fun i hi hpk => increase_balance_other hbs i ?_⟩
    intro heq
    have hg := getElem?_idxOf hm
    rw [← heq, List.getElem?_map] at hg
    obtain ⟨v, hv⟩ : ∃ v, t.validators[i]? = some v := ⟨_, List.getElem?_eq_getElem hi⟩
    rw [hv] at hg
    exact hpk v hv (Option.some.inj hg)
  · simp only [hm, not_false_eq_true, if_true] at h
    by_cases hs : is_valid_deposit_signature Preset.mainnet o d.pubkey d.withdrawal_credentials
        d.amount d.signature = true
    · simp only [hs, if_true] at h
      obtain ⟨hv, hb⟩ := add_validator_frame t t' _ _ _ h
      exact ⟨hv, fun i hi _ => hb i hi⟩
    · simp only [hs, Bool.false_eq_true, if_false, pure, Except.pure, Except.ok.injEq] at h
      subst h
      exact ⟨⟨[], by simp⟩, fun _ _ _ => rfl⟩

/-- The condition of instance e: validator `i` exists and has another pubkey than `d`. -/
def ApplyOther (i : Nat) (d : PendingDeposit) (t : BeaconState) : Prop :=
  i < t.validators.length ∧ ∀ v, t.validators[i]? = some v → v.pubkey ≠ d.pubkey

/-- `setEB` keeps `ApplyOther`. -/
theorem ApplyOther.setEB {i : Nat} {d : PendingDeposit} {t : BeaconState} (e : Gwei)
    (h : ApplyOther i d t) : ApplyOther i d (setEB t i e) := by
  refine ⟨by rw [setEB_length]; exact h.1, fun v hv => ?_⟩
  obtain ⟨w, hw⟩ : ∃ w, t.validators[i]? = some w := ⟨_, List.getElem?_eq_getElem h.1⟩
  rw [setEB_some e hw] at hv
  simp only [List.getElem?_set_self h.1, Option.some.injEq] at hv
  subst hv
  exact h.2 w hw

/-- Instance e: `apply_pending_deposit` commutes with `setEB` at a validator with another
pubkey. -/
theorem apply_hF1 (o : Oracle) (d : PendingDeposit) (i : Nat) (t : BeaconState) (e : Gwei)
    (hP : ApplyOther i d t) :
    apply_pending_deposit Preset.mainnet o (setEB t i e) d =
      (fun t' => setEB t' i e) <$> apply_pending_deposit Preset.mainnet o t d := by
  obtain ⟨v, hv⟩ : ∃ v, t.validators[i]? = some v := ⟨_, List.getElem?_eq_getElem hP.1⟩
  have hpk : (t.validators.set i { v with effective_balance := e }).map (·.pubkey) =
      t.validators.map (·.pubkey) := by
    have hvi : t.validators[i]'hP.1 = v := by
      rw [List.getElem?_eq_getElem hP.1] at hv
      exact Option.some.inj hv
    have hi' : i < (t.validators.map (·.pubkey)).length := by simpa using hP.1
    have : ({ v with effective_balance := e } : Validator).pubkey =
        (t.validators.map (·.pubkey))[i] := by
      rw [List.getElem_map, hvi]
    rw [List.map_set, this, List.set_getElem_self hi']
  rw [setEB_some e hv, apply_blind o d t _ hpk]
  cases hF : apply_pending_deposit Preset.mainnet o t d with
  | error err => rfl
  | ok t' =>
    obtain ⟨⟨ys, hys⟩, -⟩ := apply_frame o d t t' hF
    have hv' : t'.validators[i]? = some v := by
      rw [hys, List.getElem?_append_left hP.1]; exact hv
    show Except.ok { t' with validators := _ } = Except.ok (setEB t' i e)
    rw [setEB_some e hv', hys, List.drop_left, List.set_append_left _ _ hP.1]

/-- Instance e: `apply_pending_deposit` keeps a validator with another pubkey and its
balance. -/
theorem apply_hF2 (o : Oracle) (d : PendingDeposit) (i : Nat) (t t' : BeaconState)
    (hP : ApplyOther i d t) (h : apply_pending_deposit Preset.mainnet o t d = .ok t') :
    t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]? := by
  obtain ⟨⟨ys, hys⟩, hb⟩ := apply_frame o d t t' h
  exact ⟨by rw [hys, List.getElem?_append_left hP.1], hb i hP.1 hP.2⟩

end EpochProofs.Spec
