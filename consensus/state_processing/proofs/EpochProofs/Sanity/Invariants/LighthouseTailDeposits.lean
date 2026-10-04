import EpochProofs.Sanity.Invariants.LighthouseTailModel

/-!
# The spec's pending deposits: decide first, then apply

The spec's `process_pending_deposits` reads the status of each deposit's validator from the
state that the earlier deposits of the loop changed. The earlier deposits only top up balances
or append new validators that have not exited, so each status equals the status in the input
state. So the spec equals: decide all deposits with `depositDecisions` on the input state, then
apply the handled deposits that are not postponed, in queue order.
-/

namespace EpochProofs.Spec

/-- The accumulator of the spec's deposit loop. -/
abbrev DepAcc := MProd (List PendingDeposit) (MProd Bool (MProd Uint64 (MProd Uint64 BeaconState)))

/-- The part of the spec's loop body after the status: apply, postpone, or check the churn. -/
def depCont (o : Oracle) (avail : Nat) (d : PendingDeposit) (r : DepAcc) (ex wd : Bool) :
    SpecM (ForInStep DepAcc) :=
  if wd = true then do
    let T ← apply_pending_deposit Preset.mainnet o r.2.2.2.2 d
    pure (.yield ⟨r.1, r.2.1, r.2.2.1 + 1, r.2.2.2.1, T⟩)
  else if ex = true then pure (.yield ⟨r.1 ++ [d], r.2.1, r.2.2.1 + 1, r.2.2.2.1, r.2.2.2.2⟩)
  else do
    let x ← uint64Add r.2.2.2.1 d.amount
    if decide (x > avail) = true then
      pure (.done ⟨r.1, decide (x > avail), r.2.2.1, r.2.2.2.1, r.2.2.2.2⟩)
    else do
      let proc ← uint64Add r.2.2.2.1 d.amount
      let T ← apply_pending_deposit Preset.mainnet o r.2.2.2.2 d
      pure (.yield ⟨r.1, decide (x > avail), r.2.2.1 + 1, proc, T⟩)

/-- The body of the spec's deposit loop, with the status read from the loop state. -/
def depBody (o : Oracle) (fs avail next : Nat) (d : PendingDeposit) (r : DepAcc) :
    SpecM (ForInStep DepAcc) :=
  if d.slot > fs then pure (.done r)
  else if r.2.2.1 ≥ Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH then pure (.done r)
  else if d.pubkey ∈ r.2.2.2.2.validators.map (·.pubkey) then do
    let v ← listGet r.2.2.2.2.validators
      ((r.2.2.2.2.validators.map (·.pubkey)).idxOf d.pubkey)
    depCont o avail d r (decide (v.exit_epoch < FAR_FUTURE_EPOCH))
      (decide (v.withdrawable_epoch < next))
  else depCont o avail d r false false

/-- The end of the spec's deposit function: write the queue and the balance to consume. -/
def depFinish (avail : Nat) (r : DepAcc) : SpecM BeaconState :=
  let T := { r.2.2.2.2 with
    pending_deposits := r.2.2.2.2.pending_deposits.drop r.2.2.1 ++ r.1 }
  if r.2.1 = true then do
    pure { T with deposit_balance_to_consume := ← uint64Sub avail r.2.2.2.1 }
  else pure { T with deposit_balance_to_consume := 0 }

/-- The spec's `process_pending_deposits` with its loop body and its end named. -/
theorem process_pending_deposits_eq (o : Oracle) (total_active_balance : Gwei)
    (s : BeaconState) :
    process_pending_deposits Preset.mainnet total_active_balance
        (apply_pending_deposit Preset.mainnet o) s = (do
      let c ← get_current_epoch Preset.mainnet s
      let next_epoch ← uint64Add c 1
      let cl ← get_activation_churn_limit Preset.mainnet total_active_balance
      let avail ← uint64Add s.deposit_balance_to_consume cl
      let fs ← compute_start_slot_at_epoch Preset.mainnet s.finalized_checkpoint.epoch
      let r ← forIn s.pending_deposits ⟨[], false, 0, 0, s⟩ (depBody o fs avail next_epoch)
      depFinish avail r) := by
  rfl

/-! ## Applying deposits keeps the validators of the input state -/

/-- `T` has the validators of `s`, then new validators that have not exited, and the deposit
queue of `s`. -/
def ValExt (s T : BeaconState) : Prop :=
  ∃ extra : List Validator, T.validators = s.validators ++ extra ∧
    (∀ v ∈ extra, v.exit_epoch = FAR_FUTURE_EPOCH ∧ v.withdrawable_epoch = FAR_FUTURE_EPOCH) ∧
    T.pending_deposits = s.pending_deposits

/-- A state has `ValExt` to itself. -/
theorem ValExt.refl (s : BeaconState) : ValExt s s :=
  ⟨[], by simp, by simp, rfl⟩

/-- A new validator from a deposit has not exited. -/
theorem get_validator_from_deposit_far (p : Preset) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei) (v : Validator)
    (h : get_validator_from_deposit p pubkey withdrawal_credentials amount = .ok v) :
    v.exit_epoch = FAR_FUTURE_EPOCH ∧ v.withdrawable_epoch = FAR_FUTURE_EPOCH := by
  unfold get_validator_from_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨rfl, rfl⟩)

/-- `apply_pending_deposit` keeps `ValExt`. -/
theorem apply_pending_deposit_valExt (o : Oracle) (s T T' : BeaconState) (d : PendingDeposit)
    (hT : ValExt s T) (h : apply_pending_deposit Preset.mainnet o T d = .ok T') :
    ValExt s T' := by
  obtain ⟨extra, hv, hextra, hpd⟩ := hT
  unfold apply_pending_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · split at h
    · unfold add_validator_to_registry get_index_for_new_validator at h
      obtain ⟨nv, hnv, h⟩ := specM_bind_ok h
      obtain ⟨l1, hl1, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      cases h
      simp only [set_or_append_list, beq_self_eq_true, if_true, pure, Except.pure,
        Except.ok.injEq] at hl1
      subst hl1
      refine ⟨extra ++ [nv], by simp [hv], ?_, hpd⟩
      intro v hv'
      rcases List.mem_append.mp hv' with h1 | h1
      · exact hextra v h1
      · rw [List.mem_singleton.mp h1]
        exact get_validator_from_deposit_far _ _ _ _ _ hnv
    · cases h
      exact ⟨extra, hv, hextra, hpd⟩
  · split at h
    · contradiction
    · cases h
      exact ⟨extra, hv, hextra, hpd⟩

/-- Under `ValExt`, the status that the spec loop reads in `T` is the status in `s`. -/
theorem status_eq {β : Type} (s T : BeaconState) (hT : ValExt s T) (next : Nat)
    (hnext : next ≤ FAR_FUTURE_EPOCH) (d : PendingDeposit) (k : Bool → Bool → SpecM β) :
    (if d.pubkey ∈ T.validators.map (·.pubkey) then do
      let v ← listGet T.validators ((T.validators.map (·.pubkey)).idxOf d.pubkey)
      k (decide (v.exit_epoch < FAR_FUTURE_EPOCH)) (decide (v.withdrawable_epoch < next))
    else k false false) =
      k (specDepositView s next d).is_exited (specDepositView s next d).is_withdrawn := by
  obtain ⟨extra, hv, hextra, -⟩ := hT
  rw [hv]
  unfold specDepositView
  by_cases hs : d.pubkey ∈ s.validators.map (·.pubkey)
  · have hlt := List.idxOf_lt_length_of_mem hs
    rw [List.length_map] at hlt
    have hmem : d.pubkey ∈ (s.validators ++ extra).map (·.pubkey) := by
      rw [List.map_append]; exact List.mem_append_left _ hs
    rw [if_pos hmem, List.map_append, List.idxOf_append, if_pos hs,
      List.getElem?_eq_getElem hlt]
    simp [listGet, List.getElem?_append_left hlt, List.getElem?_eq_getElem hlt, bind,
      Except.bind, pure, Except.pure]
  · rw [List.idxOf_eq_length hs, List.length_map, List.getElem?_eq_none (Nat.le_refl _)]
    by_cases ht : d.pubkey ∈ (s.validators ++ extra).map (·.pubkey)
    · rw [if_pos ht, List.map_append, List.idxOf_append, if_neg hs]
      rw [List.map_append] at ht
      have he : d.pubkey ∈ extra.map (·.pubkey) := (List.mem_append.mp ht).resolve_left hs
      have hlt := List.idxOf_lt_length_of_mem he
      rw [List.length_map] at hlt
      have hget : (s.validators ++ extra)[(extra.map (·.pubkey)).idxOf d.pubkey +
          s.validators.length]? =
          some extra[(extra.map (·.pubkey)).idxOf d.pubkey] := by
        rw [List.getElem?_append_right (Nat.le_add_left _ _),
          Nat.add_sub_cancel, List.getElem?_eq_getElem hlt]
      obtain ⟨h1, h2⟩ := hextra _ (List.getElem_mem hlt)
      have hn : ¬ FAR_FUTURE_EPOCH < next := Nat.not_lt.mpr hnext
      simp [listGet, hget, bind, Except.bind, h1, h2, hn, pure, Except.pure]
    · rw [if_neg ht]

/-- The loop body reads the status of `s` when the loop state has `ValExt`. -/
theorem depBody_view (o : Oracle) (fs avail next : Nat) (s : BeaconState)
    (hnext : next ≤ FAR_FUTURE_EPOCH) (d : PendingDeposit) (r : DepAcc)
    (hT : ValExt s r.2.2.2.2) :
    depBody o fs avail next d r =
      if d.slot > fs then pure (.done r)
      else if r.2.2.1 ≥ Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH then pure (.done r)
      else depCont o avail d r (specDepositView s next d).is_exited
        (specDepositView s next d).is_withdrawn := by
  unfold depBody
  rw [status_eq s r.2.2.2.2 hT next hnext d (depCont o avail d r)]

/-- A deposit view keeps the slot of the deposit. -/
theorem specDepositView_slot (s : BeaconState) (next : Nat) (d : PendingDeposit) :
    (specDepositView s next d).slot = d.slot := by
  unfold specDepositView; split <;> rfl

/-- A deposit view keeps the amount of the deposit. -/
theorem specDepositView_amount (s : BeaconState) (next : Nat) (d : PendingDeposit) :
    (specDepositView s next d).amount = d.amount := by
  unfold specDepositView; split <;> rfl

/-- A deposit view is never blocked by the eth1 bridge. -/
theorem specDepositView_eth1 (s : BeaconState) (next : Nat) (d : PendingDeposit) :
    (specDepositView s next d).eth1_bridge_blocked = false := by
  unfold specDepositView; split <;> rfl

/-! ## The loop with decisions and applies side by side -/

/-- An `ok` value followed by more code. -/
theorem ok_bind' {α β : Type} (a : α) (f : α → SpecM β) : (Except.ok a >>= f) = f a := rfl

/-- A bind succeeds when both parts succeed. -/
theorem specM_bind_ok_intro {α β : Type} {x : SpecM α} {f : α → SpecM β} {a : α} {b : β}
    (hx : x = .ok a) (hf : f a = .ok b) : (x >>= f) = .ok b := by
  rw [hx]; exact hf

/-- Apply a deposit unless it is postponed. -/
def applyIf (o : Oracle) (pp : Bool) (T : BeaconState) (d : PendingDeposit) :
    SpecM BeaconState :=
  if pp = true then pure T else apply_pending_deposit Preset.mainnet o T d

/-- The spec loop on views: one `depositStep`, then the apply of a deposit that is not
postponed. It also collects the postponed deposits. -/
def decideApplyLoop (o : Oracle) (fs avail : Nat) (view : PendingDeposit → DepositSpecView) :
    DepositLoopState → List PendingDeposit → BeaconState → List PendingDeposit →
      SpecM (DepositLoopResult × List PendingDeposit × BeaconState)
  | ls, post, T, [] => pure (⟨ls, false, false⟩, post, T)
  | ls, post, T, d :: l => do
    let r ← depositStep Preset.mainnet fs avail ls (view d)
    if r.stopped = true then pure (r, post, T)
    else do
      let pp := !(view d).is_withdrawn && (view d).is_exited
      let T ← applyIf o pp T d
      decideApplyLoop o fs avail view r.state (if pp = true then post ++ [d] else post) T l

/-- The accumulator of the spec loop that a result of `decideApplyLoop` stands for. -/
def decideApplyAcc (x : DepositLoopResult × List PendingDeposit × BeaconState) : DepAcc :=
  ⟨x.2.1, x.1.churn_reached, x.1.state.next_deposit_index, x.1.state.processed_amount, x.2.2⟩

/-- The spec loop equals `decideApplyLoop` on the views of `s`. -/
theorem forIn_depBody_decideApply (o : Oracle) (fs avail next : Nat) (s : BeaconState)
    (hnext : next ≤ FAR_FUTURE_EPOCH) :
    ∀ (l : List PendingDeposit) (ls : DepositLoopState) (post : List PendingDeposit)
      (T : BeaconState), ValExt s T →
      forIn l (⟨post, false, ls.next_deposit_index, ls.processed_amount, T⟩ : DepAcc)
          (depBody o fs avail next) =
        decideApplyAcc <$> decideApplyLoop o fs avail (specDepositView s next) ls post T l
  | [], ls, post, T, _ => rfl
  | d :: l, ls, post, T, hT => by
    rw [List.forIn_cons, depBody_view o fs avail next s hnext d _ hT]
    simp only [decideApplyLoop, depositStep, specDepositView_slot, specDepositView_eth1,
      specDepositView_amount, Bool.false_or, applyIf]
    by_cases h1 : d.slot > fs
    · simp [h1, pure, Except.pure, bind, Except.bind, Functor.map, Except.map, decideApplyAcc]
    by_cases h2 : ls.next_deposit_index ≥ Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH
    · simp [h1, h2, pure, Except.pure, bind, Except.bind, Functor.map, Except.map, decideApplyAcc]
    simp only [h1, h2, decide_false, Bool.false_or, if_false, Bool.false_eq_true]
    unfold depCont
    cases hwd : (specDepositView s next d).is_withdrawn
    · cases hex : (specDepositView s next d).is_exited
      · simp only [Bool.false_eq_true, if_false, Bool.not_false, Bool.true_and]
        cases ha : uint64Add ls.processed_amount d.amount with
        | error e => rfl
        | ok x =>
          by_cases h3 : x > avail
          · simp [h3, bind, Except.bind, pure, Except.pure, Functor.map, Except.map, decideApplyAcc]
          · simp only [h3, decide_false, Bool.false_eq_true, if_false, bind, Except.bind,
              pure, Except.pure]
            cases hap : apply_pending_deposit Preset.mainnet o T d with
            | error e => rfl
            | ok T' =>
              exact forIn_depBody_decideApply o fs avail next s hnext l
                ⟨x, ls.next_deposit_index + 1, ls.postponed ++ [false]⟩ post T'
                (apply_pending_deposit_valExt o s T T' d hT hap)
      · simp only [Bool.false_eq_true, if_false, if_true, Bool.not_false, Bool.true_and,
          pure, Except.pure, bind, Except.bind]
        exact forIn_depBody_decideApply o fs avail next s hnext l
          ⟨ls.processed_amount, ls.next_deposit_index + 1, ls.postponed ++ [true]⟩ _ T hT
    · simp only [if_true, Bool.not_true, Bool.false_and, Bool.false_eq_true, if_false, pure,
        Except.pure, bind, Except.bind]
      cases hap : apply_pending_deposit Preset.mainnet o T d with
      | error e => rfl
      | ok T' =>
        exact forIn_depBody_decideApply o fs avail next s hnext l
          ⟨ls.processed_amount, ls.next_deposit_index + 1, ls.postponed ++ [false]⟩ post T'
          (apply_pending_deposit_valExt o s T T' d hT hap)

/-- One `depositStep` either stops and keeps the loop state, or handles the deposit: it adds
one to the index and appends the postponed flag. -/
theorem depositStep_cases (fs avail : Nat) (ls : DepositLoopState) (v : DepositSpecView)
    (r : DepositLoopResult) (h : depositStep Preset.mainnet fs avail ls v = .ok r) :
    (r.stopped = true ∧ r.state = ls) ∨
      (r.stopped = false ∧ r.churn_reached = false ∧
        r.state.next_deposit_index = ls.next_deposit_index + 1 ∧
        r.state.postponed = ls.postponed ++ [!v.is_withdrawn && v.is_exited]) := by
  unfold depositStep at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact .inl ⟨rfl, rfl⟩)
    | (cases h; exact .inr ⟨rfl, rfl, rfl, by simp_all⟩)

/-- The loop only appends postponed flags, one for each handled deposit. -/
theorem depositLoop_growth (fs avail : Nat) :
    ∀ (vs : List DepositSpecView) (ls : DepositLoopState) (r : DepositLoopResult),
      depositLoop Preset.mainnet fs avail ls vs = .ok r →
      ∃ flags : List Bool, r.state.postponed = ls.postponed ++ flags ∧
        r.state.next_deposit_index = ls.next_deposit_index + flags.length
  | [], ls, r, h => by
    simp only [depositLoop, pure, Except.pure, Except.ok.injEq] at h
    subst h
    exact ⟨[], by simp, rfl⟩
  | v :: vs, ls, r, h => by
    simp only [depositLoop] at h
    obtain ⟨r1, h1, h⟩ := specM_bind_ok h
    rcases depositStep_cases fs avail ls v r1 h1 with ⟨hs, hst⟩ | ⟨hs, -, hn1, hp1⟩
    · simp only [hs, if_true, pure, Except.pure, Except.ok.injEq] at h
      subst h
      exact ⟨[], by rw [hst]; simp, by rw [hst]; rfl⟩
    · simp only [hs, Bool.false_eq_true, if_false] at h
      obtain ⟨flags, hp, hn⟩ := depositLoop_growth fs avail vs r1.state r h
      rw [hp1] at hp
      rw [hn1] at hn
      refine ⟨(!v.is_withdrawn && v.is_exited) :: flags, by simp [hp], by simp [hn]; omega⟩

/-- Applying handled deposits keeps `ValExt`. -/
theorem applyHandled_valExt (o : Oracle) (s : BeaconState) :
    ∀ (hd : List (PendingDeposit × Bool)) (T T' : BeaconState), ValExt s T →
      applyHandled o T hd = .ok T' → ValExt s T'
  | [], T, T', hT, h => by
    simp only [applyHandled, List.foldlM_nil, pure, Except.pure, Except.ok.injEq] at h
    subst h
    exact hT
  | x :: hd, T, T', hT, h => by
    simp only [applyHandled, List.foldlM_cons] at h
    obtain ⟨T1, h1, h⟩ := specM_bind_ok h
    have hT1 : ValExt s T1 := by
      split at h1
      · simp only [pure, Except.pure, Except.ok.injEq] at h1
        subst h1
        exact hT
      · exact apply_pending_deposit_valExt o s T T1 x.1 hT h1
    exact applyHandled_valExt o s hd T1 T' hT1 h

/-- The handled deposits of a loop run from `ls` to `r` over `l`. -/
def handledOf (l : List PendingDeposit) (ls : DepositLoopState) (r : DepositLoopResult) :
    List (PendingDeposit × Bool) :=
  (l.take (r.state.next_deposit_index - ls.next_deposit_index)).zip
    (r.state.postponed.drop ls.postponed.length)

/-- The postponed deposits among handled deposits. -/
def postponedOf (hd : List (PendingDeposit × Bool)) : List PendingDeposit :=
  (hd.filter (·.2)).map (·.1)

/-- `decideApplyLoop` succeeds exactly when the decisions succeed and the handled deposits apply. -/
theorem decideApplyLoop_iff (o : Oracle) (fs avail : Nat) (view : PendingDeposit → DepositSpecView) :
    ∀ (l : List PendingDeposit) (ls : DepositLoopState) (post : List PendingDeposit)
      (T : BeaconState) (r : DepositLoopResult) (post' : List PendingDeposit)
      (T' : BeaconState),
      decideApplyLoop o fs avail view ls post T l = .ok (r, post', T') ↔
        depositLoop Preset.mainnet fs avail ls (l.map view) = .ok r ∧
          applyHandled o T (handledOf l ls r) = .ok T' ∧
          post' = post ++ postponedOf (handledOf l ls r)
  | [], ls, post, T, r, post', T' => by
    simp only [decideApplyLoop, depositLoop, List.map_nil, pure, Except.pure, Except.ok.injEq,
      Prod.mk.injEq]
    constructor
    · rintro ⟨rfl, rfl, rfl⟩
      simp [handledOf, applyHandled, postponedOf, pure, Except.pure]
    · rintro ⟨rfl, h2, h3⟩
      simp [handledOf, applyHandled, postponedOf, pure, Except.pure] at h2 h3
      exact ⟨rfl, by simp_all, by simp_all⟩
  | d :: l, ls, post, T, r, post', T' => by
    simp only [decideApplyLoop, depositLoop, List.map_cons]
    cases h1 : depositStep Preset.mainnet fs avail ls (view d) with
    | error e => simp [bind, Except.bind]
    | ok r1 =>
      rcases depositStep_cases fs avail ls (view d) r1 h1 with ⟨hs, hst⟩ | ⟨hs, -, hn1, hp1⟩
      · simp only [bind, Except.bind, hs, if_true, pure, Except.pure, Except.ok.injEq,
          Prod.mk.injEq]
        constructor
        · rintro ⟨rfl, rfl, rfl⟩
          simp [handledOf, applyHandled, postponedOf, hst, pure, Except.pure]
        · rintro ⟨rfl, h2, h3⟩
          simp [handledOf, applyHandled, postponedOf, hst, pure, Except.pure] at h2 h3
          exact ⟨rfl, by simp_all, by simp_all⟩
      · rw [ok_bind', ok_bind']
        simp only [hs, Bool.false_eq_true, if_false]
        have hh : ∀ flags : List Bool, r.state.postponed = r1.state.postponed ++ flags →
            r.state.next_deposit_index = r1.state.next_deposit_index + flags.length →
            handledOf (d :: l) ls r =
              (d, !(view d).is_withdrawn && (view d).is_exited) :: handledOf l r1.state r := by
          intro flags hpf hnf
          have hk : r.state.next_deposit_index - ls.next_deposit_index = flags.length + 1 := by
            rw [hnf, hn1]; omega
          have hk1 : r.state.next_deposit_index - r1.state.next_deposit_index = flags.length := by
            rw [hnf]; omega
          simp only [handledOf, hk, hk1, hpf, hp1, List.take_succ_cons, List.append_assoc,
            List.singleton_append, List.drop_left', List.zip_cons_cons, List.length_append,
            List.length_singleton]
          simp
        constructor
        · intro h
          obtain ⟨T1, hT1, h⟩ := specM_bind_ok h
          obtain ⟨hl, ha, hp⟩ := (decideApplyLoop_iff o fs avail view l r1.state _ T1 r post' T').mp h
          obtain ⟨flags, hpf, hnf⟩ := depositLoop_growth fs avail _ _ r hl
          rw [hh flags hpf hnf]
          refine ⟨hl, ?_, ?_⟩
          · simp only [applyHandled, List.foldlM_cons]
            simp only [applyHandled] at ha
            exact specM_bind_ok_intro hT1 ha
          · rw [hp]
            split <;> simp_all [postponedOf]
        · rintro ⟨hl, ha, hp⟩
          obtain ⟨flags, hpf, hnf⟩ := depositLoop_growth fs avail _ _ r hl
          rw [hh flags hpf hnf] at ha hp
          simp only [applyHandled, List.foldlM_cons] at ha
          obtain ⟨T1, hT1, ha⟩ := specM_bind_ok ha
          refine specM_bind_ok_intro hT1 ?_
          refine (decideApplyLoop_iff o fs avail view l r1.state _ T1 r post' T').mpr ⟨hl, ha, ?_⟩
          rw [hp]
          split <;> simp_all [postponedOf]

/-! ## Decide first, then apply -/

/-- The end of the spec function on a `decideApplyLoop` result. -/
theorem depFinish_decideApply (avail : Nat) (s : BeaconState) (r : DepositLoopResult)
    (post' : List PendingDeposit) (T' : BeaconState) (hT : ValExt s T') (v : BeaconState) :
    depFinish avail (decideApplyAcc (r, post', T')) = .ok v ↔
      ∃ nd, (if r.churn_reached = true then uint64Sub avail r.state.processed_amount
        else pure 0) = .ok nd ∧
        v = { T' with
          pending_deposits := s.pending_deposits.drop r.state.next_deposit_index ++ post'
          deposit_balance_to_consume := nd } := by
  obtain ⟨-, -, -, hpd⟩ := hT
  unfold depFinish decideApplyAcc
  simp only [hpd]
  cases r.churn_reached
  · simp [pure, Except.pure, eq_comm]
  · simp only [if_true]
    cases uint64Sub avail r.state.processed_amount <;>
      simp [bind, Except.bind, pure, Except.pure, eq_comm]

/-- On mainnet, the spec's `process_pending_deposits` has the same `ok` results as deciding all
deposits on the input state with `depositDecisions`, then applying the handled deposits that are
not postponed, in queue order. -/
theorem process_pending_deposits_decide (o : Oracle) (total_active_balance : Gwei)
    (s : BeaconState) (current_epoch : Epoch)
    (hcurrent : get_current_epoch Preset.mainnet s = .ok current_epoch)
    (hfar : current_epoch + 1 ≤ FAR_FUTURE_EPOCH) :
    SameOk (process_pending_deposits Preset.mainnet total_active_balance
        (apply_pending_deposit Preset.mainnet o) s)
      (do
        let finalized_slot ←
          compute_start_slot_at_epoch Preset.mainnet s.finalized_checkpoint.epoch
        let (next, dbtc, postponed) ← depositDecisions Preset.mainnet finalized_slot
          s.deposit_balance_to_consume
          (← get_activation_churn_limit Preset.mainnet total_active_balance)
          (s.pending_deposits.map (specDepositView s (current_epoch + 1)))
        let handled := (s.pending_deposits.take next).zip postponed
        let s' ← applyHandled o s handled
        pure { s' with
          pending_deposits := s.pending_deposits.drop next ++ (handled.filter (·.2)).map (·.1)
          deposit_balance_to_consume := dbtc }) := by
  have hne : uint64Add current_epoch 1 = .ok (current_epoch + 1) := by
    have : current_epoch + 1 < UINT64_SIZE :=
      Nat.lt_of_le_of_lt hfar (by simp [FAR_FUTURE_EPOCH, UINT64_SIZE])
    simp [uint64Add, this, pure, Except.pure]
  rw [process_pending_deposits_eq, hcurrent, ok_bind', hne, ok_bind']
  intro v
  cases hcl : get_activation_churn_limit Preset.mainnet total_active_balance with
  | error e =>
    simp only [bind, Except.bind]
    constructor <;> intro h <;> (repeat' split at h) <;> simp_all
  | ok cl =>
  cases hfs : compute_start_slot_at_epoch Preset.mainnet s.finalized_checkpoint.epoch with
  | error e =>
    cases hx : uint64Add s.deposit_balance_to_consume cl <;> simp [bind, Except.bind, hx]
  | ok fs =>
  unfold depositDecisions
  cases hav : uint64Add s.deposit_balance_to_consume cl with
  | error e => simp [bind, Except.bind, hav]
  | ok avail =>
  simp only [hav, ok_bind']
  have hi := forIn_depBody_decideApply o fs avail (current_epoch + 1) s hfar s.pending_deposits
    ⟨0, 0, []⟩ [] s (ValExt.refl s)
  simp only at hi
  rw [hi]
  have hhd : ∀ r : DepositLoopResult, handledOf s.pending_deposits ⟨0, 0, []⟩ r =
      (s.pending_deposits.take r.state.next_deposit_index).zip r.state.postponed := by
    intro r; simp [handledOf]
  cases hf : decideApplyLoop o fs avail (specDepositView s (current_epoch + 1)) ⟨0, 0, []⟩ [] s
      s.pending_deposits with
  | error e =>
    simp only [Functor.map, Except.map, bind, Except.bind]
    constructor
    · intro h; cases h
    · intro h
      exfalso
      obtain ⟨tr, h1, h⟩ := specM_bind_ok h
      obtain ⟨r, hl, h1⟩ := specM_bind_ok h1
      obtain ⟨T', ha, -⟩ := specM_bind_ok h
      have hn : tr.1 = r.state.next_deposit_index ∧ tr.2.2 = r.state.postponed := by
        split at h1
        · obtain ⟨_, -, h1⟩ := specM_bind_ok h1
          simp only [pure, Except.pure, Except.ok.injEq] at h1
          subst h1; exact ⟨rfl, rfl⟩
        · simp only [pure, Except.pure, Except.ok.injEq] at h1
          subst h1; exact ⟨rfl, rfl⟩
      rw [hn.1, hn.2, ← hhd] at ha
      have := (decideApplyLoop_iff o fs avail _ _ _ [] _ r _ T').mpr ⟨hl, ha, rfl⟩
      rw [hf] at this
      cases this
  | ok x =>
    obtain ⟨r, post', T'⟩ := x
    obtain ⟨hl, ha, hp⟩ := (decideApplyLoop_iff o fs avail _ _ _ _ _ r post' T').mp hf
    have hT' := applyHandled_valExt o s _ s T' (ValExt.refl s) ha
    rw [hhd] at ha hp
    show depFinish avail (decideApplyAcc (r, post', T')) = .ok v ↔ _
    rw [depFinish_decideApply avail s r post' T' hT' v, hl, ok_bind', hp]
    cases hc : r.churn_reached
    · simp only [Bool.false_eq_true, if_false, pure, Except.pure, ok_bind', ha, postponedOf,
        List.nil_append, Except.ok.injEq]
      constructor
      · rintro ⟨nd, hnd, rfl⟩
        cases hnd
        rfl
      · intro h
        exact ⟨0, rfl, h.symm⟩
    · simp only [if_true]
      cases hsub : uint64Sub avail r.state.processed_amount with
      | error e => simp [bind, Except.bind]
      | ok nd =>
        simp only [ok_bind', pure, Except.pure, ha, postponedOf, List.nil_append,
          Except.ok.injEq]
        constructor
        · rintro ⟨nd', hnd, rfl⟩
          cases hnd
          rfl
        · intro h
          exact ⟨nd, rfl, h.symm⟩

end EpochProofs.Spec
