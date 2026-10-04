import EpochProofs.Sanity.Invariants.LighthouseTailNF

/-!
# The spec epoch tail in the common form

`epochRest_nf` shows that the spec steps after the four passes give the same `ok` results as
`tailNF`. The deposits are decided once, the top-ups are summed per validator, and the
effective balance updates move next to the steps that Lighthouse runs them with.
-/

namespace EpochProofs.Spec

/-! ## Moving binds -/

/-- Two independent computations commute. -/
theorem sameOk_bind_comm {α β γ : Type} (x : SpecM α) (y : SpecM β) (k : α → β → SpecM γ) :
    SameOk (x >>= fun a => y >>= fun b => k a b) (y >>= fun b => x >>= fun a => k a b) := by
  intro v
  cases x <;> cases y <;> simp [bind, Except.bind]

/-- A swap of two steps carries over to any continuation. -/
theorem sameOk_swap_cont {A B : BeaconState → SpecM BeaconState} {s : BeaconState}
    (h : SameOk (A s >>= B) (B s >>= A)) (k : BeaconState → SpecM BeaconState) :
    SameOk (A s >>= fun t => B t >>= k) (B s >>= fun t => A t >>= k) := by
  refine SameOk.trans (SameOk.of_eq (bind_assoc (A s) B k).symm) ?_
  refine SameOk.trans (SameOk.bind h (fun _ _ => SameOk.refl _)) ?_
  exact SameOk.of_eq (bind_assoc (B s) A k)

/-- `SameOk` of the first steps carries over to any continuation. -/
theorem sameOk_cont {x y : SpecM BeaconState} (h : SameOk x y)
    (k : BeaconState → SpecM BeaconState) : SameOk (x >>= k) (y >>= k) :=
  SameOk.bind h (fun _ _ => SameOk.refl _)

/-! ## The eth1 reset -/

/-- The test of `process_eth1_data_reset`: it reads only the slot. -/
def eth1Cond (S : BeaconState) : SpecM Bool := do
  let next_epoch ← uint64Add (← get_current_epoch Preset.mainnet S) 1
  pure ((← uint64Mod next_epoch Preset.mainnet.EPOCHS_PER_ETH1_VOTING_PERIOD) == 0)

/-- `process_eth1_data_reset` clears the votes or keeps the state, by its test. -/
theorem eth1_eq (S : BeaconState) :
    process_eth1_data_reset Preset.mainnet S =
      (fun b => if b then { S with eth1_data_votes := [] } else S) <$> eth1Cond S := by
  unfold process_eth1_data_reset eth1Cond
  cases get_current_epoch Preset.mainnet S with
  | error e => rfl
  | ok c =>
    simp only [bind, Except.bind]
    cases uint64Add c 1 with
    | error e => rfl
    | ok n =>
      simp only
      cases uint64Mod n Preset.mainnet.EPOCHS_PER_ETH1_VOTING_PERIOD with
      | error e => rfl
      | ok m =>
        simp only [pure, Except.pure, Functor.map, Except.map]
        split <;> simp_all

/-- The test reads only the slot. -/
theorem eth1Cond_slot {S S' : BeaconState} (h : S'.slot = S.slot) : eth1Cond S' = eth1Cond S := by
  simp only [eth1Cond, get_current_epoch, h]

/-- A successful eth1 reset changes only the votes. -/
theorem eth1_shape {S S' : BeaconState} (h : process_eth1_data_reset Preset.mainnet S = .ok S') :
    ∃ v, S' = { S with eth1_data_votes := v } := by
  rw [eth1_eq] at h
  cases hc : eth1Cond S with
  | error e => rw [hc] at h; cases h
  | ok b =>
    rw [hc] at h
    cases b
    · cases h; exact ⟨S.eth1_data_votes, rfl⟩
    · cases h; exact ⟨[], rfl⟩

/-- The eth1 reset commutes with a step that keeps the slot and passes the votes through. -/
theorem eth1_comm (F : BeaconState → SpecM BeaconState) (S : BeaconState)
    (hslot : ∀ T, F S = .ok T → T.slot = S.slot)
    (hB : F { S with eth1_data_votes := [] } =
      (fun t => { t with eth1_data_votes := [] }) <$> F S) :
    SameOk (process_eth1_data_reset Preset.mainnet S >>= F)
      (F S >>= process_eth1_data_reset Preset.mainnet) := by
  intro w
  rw [eth1_eq]
  cases hF : F S with
  | error e =>
    cases hc : eth1Cond S with
    | error e' => simp [bind, Except.bind, Functor.map, Except.map]
    | ok b =>
      cases b
      · simp [bind, Except.bind, Functor.map, Except.map, hF]
      · simp [bind, Except.bind, Functor.map, Except.map, hB, hF]
  | ok T =>
    have hT := hslot T hF
    show _ ↔ process_eth1_data_reset Preset.mainnet T = .ok w
    rw [eth1_eq, eth1Cond_slot hT]
    cases hc : eth1Cond S with
    | error e' => simp [bind, Except.bind, Functor.map, Except.map]
    | ok b =>
      cases b
      · simp [bind, Except.bind, Functor.map, Except.map, hF]
      · simp [bind, Except.bind, Functor.map, Except.map, hB, hF]

/-! ## The total active balance with new validators -/

/-- `mapM` reads its function only on the members of the list. -/
theorem mapM_congr_mem' {α β : Type} (f g : α → SpecM β) :
    ∀ (l : List α), (∀ a ∈ l, f a = g a) → l.mapM f = l.mapM g
  | [], _ => rfl
  | a :: l, h => by
    rw [List.mapM_cons, List.mapM_cons, h a (by simp),
      mapM_congr_mem' f g l (fun x hx => h x (by simp [hx]))]

/-- Validators appended after `l` that are not active do not change the active indices. -/
theorem active_indices_append (l extra : List Validator) (epoch : Epoch)
    (hext : ∀ v ∈ extra, ¬ v.activation_epoch ≤ epoch) :
    ((l ++ extra).zipIdx.filter fun x => is_active_validator x.1 epoch).map (·.2) =
      (l.zipIdx.filter fun x => is_active_validator x.1 epoch).map (·.2) := by
  rw [List.zipIdx_append, List.filter_append]
  have : (extra.zipIdx (0 + l.length)).filter (fun x => is_active_validator x.1 epoch) = [] := by
    rw [List.filter_eq_nil_iff]
    intro x hx
    have hm : x.1 ∈ extra := List.fst_mem_of_mem_zipIdx hx
    simp [is_active_validator, hext x.1 hm]
  rw [this, List.append_nil]

/-- An active index is below the length. -/
theorem active_index_lt (l : List Validator) (epoch : Epoch) (i : Nat)
    (h : i ∈ (l.zipIdx.filter fun x => is_active_validator x.1 epoch).map (·.2)) :
    i < l.length := by
  obtain ⟨x, hx, rfl⟩ := List.mem_map.mp h
  have := List.snd_lt_of_mem_zipIdx (List.mem_filter.mp hx).1
  simpa using this

/-- Inactive validators appended after the old ones keep `get_total_active_balance`. -/
theorem get_total_active_balance_append (S T : BeaconState) (extra : List Validator)
    (epoch : Epoch) (hcur : get_current_epoch Preset.mainnet S = .ok epoch)
    (hslot : T.slot = S.slot) (hv : T.validators = S.validators ++ extra)
    (hext : ∀ v ∈ extra, v.activation_epoch = FAR_FUTURE_EPOCH)
    (hfar : epoch < FAR_FUTURE_EPOCH) :
    get_total_active_balance Preset.mainnet T = get_total_active_balance Preset.mainnet S := by
  have hcurT : get_current_epoch Preset.mainnet T = .ok epoch := by
    rw [← hcur]; simp only [get_current_epoch, hslot]
  unfold get_total_active_balance
  rw [hcur, hcurT]
  show get_total_balance Preset.mainnet T (get_active_validator_indices T epoch) =
    get_total_balance Preset.mainnet S (get_active_validator_indices S epoch)
  have hidx : get_active_validator_indices T epoch = get_active_validator_indices S epoch := by
    unfold get_active_validator_indices
    rw [hv, active_indices_append]
    intro v hv'
    rw [hext v hv']
    exact Nat.not_le.mpr hfar
  rw [hidx]
  unfold get_total_balance
  rw [mapM_congr_mem' _ _ _ (fun i hi => ?_)]
  have hlt := active_index_lt S.validators epoch i hi
  simp only [hv, listGet, List.getElem?_append_left hlt]

/-! ## New validators are not active -/

/-- A new validator from a deposit has no activation epoch. -/
theorem get_validator_from_deposit_act (pubkey : BLSPubkey) (withdrawal_credentials : Bytes32)
    (amount : Gwei) (v : Validator)
    (h : get_validator_from_deposit Preset.mainnet pubkey withdrawal_credentials amount = .ok v) :
    v.activation_epoch = FAR_FUTURE_EPOCH := by
  unfold get_validator_from_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; rfl)

/-- `T` has the slot of `S` and its validators, then new validators that are not active. -/
def ValAct (S T : BeaconState) : Prop :=
  T.slot = S.slot ∧ ∃ extra : List Validator, T.validators = S.validators ++ extra ∧
    ∀ v ∈ extra, v.activation_epoch = FAR_FUTURE_EPOCH

/-- A state has `ValAct` to itself. -/
theorem ValAct.refl (S : BeaconState) : ValAct S S := ⟨rfl, [], by simp, by simp⟩

/-- `ValAct` passes to a state with the same slot and validators. -/
theorem ValAct.of_eq {S T T' : BeaconState} (h : ValAct S T) (hs : T'.slot = T.slot)
    (hv : T'.validators = T.validators) : ValAct S T' := by
  obtain ⟨h1, extra, h2, h3⟩ := h
  exact ⟨hs.trans h1, extra, hv.trans h2, h3⟩

/-- `apply_pending_deposit` keeps `ValAct`. -/
theorem apply_valAct (o : Oracle) (S T T' : BeaconState) (d : PendingDeposit)
    (hT : ValAct S T) (h : apply_pending_deposit Preset.mainnet o T d = .ok T') :
    ValAct S T' := by
  obtain ⟨hs, extra, hv, hextra⟩ := hT
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
      refine ⟨hs, extra ++ [nv], by simp [hv], ?_⟩
      intro v hv'
      rcases List.mem_append.mp hv' with h1 | h1
      · exact hextra v h1
      · rw [List.mem_singleton.mp h1]
        exact get_validator_from_deposit_act _ _ _ _ hnv
    · cases h
      exact ⟨hs, extra, hv, hextra⟩
  · split at h
    · contradiction
    · cases h
      exact ⟨hs, extra, hv, hextra⟩

/-- A fold of deposits keeps `ValAct`. -/
theorem applyFold_valAct (o : Oracle) (S : BeaconState) :
    ∀ (ds : List PendingDeposit) (T T' : BeaconState), ValAct S T →
      ds.foldlM (apply_pending_deposit Preset.mainnet o) T = .ok T' → ValAct S T'
  | [], T, T', hT, h => by
    simp only [List.foldlM_nil, pure, Except.pure, Except.ok.injEq] at h
    subst h
    exact hT
  | d :: ds, T, T', hT, h => by
    rw [List.foldlM_cons] at h
    obtain ⟨T1, h1, h⟩ := specM_bind_ok h
    exact applyFold_valAct o S ds T1 T' (apply_valAct o S T T1 d hT h1) h

/-! ## The order of the effective balance updates -/

/-- `eraseDups` has no duplicates. -/
theorem nodup_eraseDups' : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
    rw [List.eraseDups_cons, List.nodup_cons]
    refine ⟨?_, nodup_eraseDups' _⟩
    rw [List.mem_eraseDups]
    simp
termination_by l => l.length
decreasing_by
  have := List.length_filter_le (fun b => !b == a) as
  simp only [List.length_cons]
  omega

/-- `consolidationIndices` has no duplicates. -/
theorem consolidationIndices_nodup (s : BeaconState) : (consolidationIndices s).Nodup :=
  (List.mergeSort_perm _ _).nodup_iff.mpr (nodup_eraseDups' _)

/-- The indices below `m` in three groups: the ones below `n` that `named` does not hold, the
ones from `n`, and `named`. -/
theorem range_perm_split (n m : Nat) (named : List Nat) (hnd : named.Nodup)
    (hsub : ∀ i ∈ named, i < n) (hnm : n ≤ m) :
    (List.range m).Perm ((List.range n).filter (fun i => !decide (i ∈ named)) ++
      List.range' n (m - n) ++ named) := by
  have hr : List.range m = List.range n ++ List.range' n (m - n) := by
    rw [List.range_eq_range', List.range_eq_range']
    have := @List.range'_append 0 n (m - n) 1
    simp only [Nat.zero_add, Nat.one_mul] at this
    rw [this, Nat.add_sub_cancel' hnm]
  have hG : ((List.range n).filter fun i => !!decide (i ∈ named)).Perm named := by
    rw [List.perm_iff_count]
    intro a
    have h1 : ((List.range n).filter fun i => !!decide (i ∈ named)).Nodup :=
      List.nodup_range.sublist (List.filter_sublist)
    rw [h1.count, hnd.count]
    have : a ∈ (List.range n).filter (fun i => !!decide (i ∈ named)) ↔ a ∈ named := by
      simp only [List.mem_filter, List.mem_range, Bool.not_not, decide_eq_true_eq]
      exact ⟨fun h => h.2, fun h => ⟨hsub a h, h⟩⟩
    simp only [this]
  have hsplit := List.filter_append_perm (fun i => !decide (i ∈ named)) (List.range n)
  rw [hr]
  refine (hsplit.append_right _).symm.trans ?_
  simp only [List.append_assoc]
  refine List.Perm.append_left _ ?_
  exact (List.perm_append_comm.trans (hG.append_left _))

/-! ## Steps that pass other fields through -/

/-- `add_validator_to_registry` passes the deposit queue fields through. -/
theorem add_validator_wq (t : BeaconState) (q : List PendingDeposit) (d : Gwei)
    (pubkey : BLSPubkey) (withdrawal_credentials : Bytes32) (amount : Gwei) :
    add_validator_to_registry Preset.mainnet
        { t with pending_deposits := q, deposit_balance_to_consume := d } pubkey
        withdrawal_credentials amount =
      (fun t' => { t' with pending_deposits := q, deposit_balance_to_consume := d }) <$>
        add_validator_to_registry Preset.mainnet t pubkey withdrawal_credentials amount := by
  unfold add_validator_to_registry get_index_for_new_validator
  simp only [bind, Except.bind, pure, Except.pure, Functor.map, Except.map]
  repeat' split
  all_goals first
    | rfl
    | (rename_i h; cases h; rfl)
    | (rename_i h; cases h)

/-- `apply_pending_deposit` passes the deposit queue fields through. -/
theorem apply_wq (o : Oracle) (t : BeaconState) (q : List PendingDeposit) (d : Gwei)
    (dp : PendingDeposit) :
    apply_pending_deposit Preset.mainnet o
        { t with pending_deposits := q, deposit_balance_to_consume := d } dp =
      (fun t' => { t' with pending_deposits := q, deposit_balance_to_consume := d }) <$>
        apply_pending_deposit Preset.mainnet o t dp := by
  unfold apply_pending_deposit
  dsimp only
  split
  · split
    · exact add_validator_wq t q d _ _ _
    · rfl
  · cases increase_balance t.balances ((t.validators.map (·.pubkey)).idxOf dp.pubkey)
        dp.amount with
    | error err => rfl
    | ok bs => rfl

/-- A fold of deposits passes the deposit queue fields through. -/
theorem applyFold_wq (o : Oracle) (q : List PendingDeposit) (d : Gwei) :
    ∀ (ds : List PendingDeposit) (t : BeaconState),
      ds.foldlM (apply_pending_deposit Preset.mainnet o)
          { t with pending_deposits := q, deposit_balance_to_consume := d } =
        (fun t' => { t' with pending_deposits := q, deposit_balance_to_consume := d }) <$>
          ds.foldlM (apply_pending_deposit Preset.mainnet o) t
  | [], t => rfl
  | dp :: ds, t => by
    rw [List.foldlM_cons, List.foldlM_cons, apply_wq]
    cases apply_pending_deposit Preset.mainnet o t dp with
    | error e => rfl
    | ok t1 => exact applyFold_wq o q d ds t1

/-- The queue write commutes with a fold of deposits. -/
theorem wq_news_comm (o : Oracle) (q : List PendingDeposit) (d : Gwei)
    (ds : List PendingDeposit) (t : BeaconState) (k : BeaconState → SpecM BeaconState) :
    (ds.foldlM (apply_pending_deposit Preset.mainnet o) t >>= fun t' =>
        pure { t' with pending_deposits := q, deposit_balance_to_consume := d } >>= k) =
      (writeQueue q d t >>= fun t' =>
        ds.foldlM (apply_pending_deposit Preset.mainnet o) t' >>= k) := by
  show _ = ds.foldlM (apply_pending_deposit Preset.mainnet o)
    { t with pending_deposits := q, deposit_balance_to_consume := d } >>= k
  conv => rhs; rw [applyFold_wq o q d ds t]
  cases ds.foldlM (apply_pending_deposit Preset.mainnet o) t <;> rfl

/-- `addSums` keeps the slot and passes the votes through. -/
theorem addSums_eth1 (S : BeaconState) (sums : List Gwei) :
    (∀ T, addSums S sums = .ok T → T.slot = S.slot) ∧
      addSums { S with eth1_data_votes := [] } sums =
        (fun t => { t with eth1_data_votes := [] }) <$> addSums S sums := by
  refine ⟨fun T h => ?_, ?_⟩
  · rw [addSums_eq] at h
    cases hf : ((List.range S.validators.length).map fun k => (k, sums.getD k 0)).foldlM
        (fun acc x => increase_balance acc x.1 x.2) S.balances with
    | error e => rw [hf] at h; cases h
    | ok bs => rw [hf] at h; cases h; rfl
  · rw [addSums_eq, addSums_eq]
    cases ((List.range S.validators.length).map fun k => (k, sums.getD k 0)).foldlM
        (fun acc x => increase_balance acc x.1 x.2) S.balances <;> rfl

/-! ## Effective balance updates and the new deposits -/

/-- Validator `i` exists and has a pubkey in `Pk`. -/
def NewsQ (Pk : List BLSPubkey) (i : Nat) (t : BeaconState) : Prop :=
  i < t.validators.length ∧ ∀ v, t.validators[i]? = some v → v.pubkey ∈ Pk

/-- `setEB` keeps `NewsQ`. -/
theorem NewsQ.setEB {Pk : List BLSPubkey} {i : Nat} {t : BeaconState} (j : Nat) (e : Gwei)
    (h : NewsQ Pk i t) : NewsQ Pk i (setEB t j e) := by
  refine ⟨by rw [setEB_length]; exact h.1, fun v hv => ?_⟩
  by_cases hij : i = j
  · subst hij
    obtain ⟨w, hw⟩ : ∃ w, t.validators[i]? = some w := ⟨_, List.getElem?_eq_getElem h.1⟩
    rw [setEB_some e hw, List.getElem?_set_self h.1, Option.some.injEq] at hv
    subst hv
    exact h.2 w hw
  · rw [setEB_get_ne t e hij] at hv
    exact h.2 v hv

/-- A deposit keeps `NewsQ`. -/
theorem NewsQ.apply {Pk : List BLSPubkey} {i : Nat} {t t' : BeaconState} (o : Oracle)
    (d : PendingDeposit) (h : NewsQ Pk i t)
    (ha : apply_pending_deposit Preset.mainnet o t d = .ok t') : NewsQ Pk i t' := by
  obtain ⟨⟨ys, hys⟩, -⟩ := apply_frame o d t t' ha
  refine ⟨by rw [hys, List.length_append]; exact Nat.lt_of_lt_of_le h.1 (Nat.le_add_right _ _),
    fun v hv => h.2 v ?_⟩
  rw [hys, List.getElem?_append_left h.1] at hv
  exact hv

/-- A fold of deposits for pubkeys outside `Pk` commutes with `setEB` at a validator with a
pubkey in `Pk`. -/
theorem newsFold_hF1 (o : Oracle) (Pk : List BLSPubkey) (i : Nat) (e : Gwei) :
    ∀ (ds : List PendingDeposit), (∀ d ∈ ds, d.pubkey ∉ Pk) → ∀ t, NewsQ Pk i t →
      ds.foldlM (apply_pending_deposit Preset.mainnet o) (setEB t i e) =
        (fun t' => setEB t' i e) <$> ds.foldlM (apply_pending_deposit Preset.mainnet o) t
  | [], _, t, _ => rfl
  | d :: ds, hds, t, hq => by
    have hother : ApplyOther i d t :=
      ⟨hq.1, fun v hv heq => hds d (by simp) (heq ▸ hq.2 v hv)⟩
    rw [List.foldlM_cons, List.foldlM_cons, apply_hF1 o d i t e hother]
    cases ha : apply_pending_deposit Preset.mainnet o t d with
    | error err => rfl
    | ok t1 =>
      exact newsFold_hF1 o Pk i e ds (fun d' hd' => hds d' (by simp [hd'])) t1
        (hq.apply o d ha)

/-- A fold of deposits for pubkeys outside `Pk` keeps a validator with a pubkey in `Pk` and its
balance. -/
theorem newsFold_hF2 (o : Oracle) (Pk : List BLSPubkey) (i : Nat) :
    ∀ (ds : List PendingDeposit), (∀ d ∈ ds, d.pubkey ∉ Pk) → ∀ t t', NewsQ Pk i t →
      ds.foldlM (apply_pending_deposit Preset.mainnet o) t = .ok t' →
      t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]?
  | [], _, t, t', _, h => by cases h; exact ⟨rfl, rfl⟩
  | d :: ds, hds, t, t', hq, h => by
    rw [List.foldlM_cons] at h
    obtain ⟨t1, h1, h⟩ := specM_bind_ok h
    have hother : ApplyOther i d t :=
      ⟨hq.1, fun v hv heq => hds d (by simp) (heq ▸ hq.2 v hv)⟩
    obtain ⟨hv1, hb1⟩ := apply_hF2 o d i t t1 hother h1
    obtain ⟨hv2, hb2⟩ := newsFold_hF2 o Pk i ds (fun d' hd' => hds d' (by simp [hd'])) t1 t'
      (hq.apply o d h1) h
    exact ⟨hv2.trans hv1, hb2.trans hb1⟩

/-! ## The pieces of the common form -/

/-- The deposit decisions on the views that Lighthouse predicts. -/
def decOf (s1 : BeaconState) (total_active_balance : Gwei) (current_epoch : Epoch) :
    SpecM (Nat × Gwei × List Bool) := do
  let finalized_slot ←
    compute_start_slot_at_epoch Preset.mainnet s1.finalized_checkpoint.epoch
  depositDecisions Preset.mainnet finalized_slot s1.deposit_balance_to_consume
    (← get_activation_churn_limit Preset.mainnet total_active_balance)
    ((s1.pending_deposits.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH).map
      (lhDepositView s1 current_epoch))

/-- The handled deposits of a decision, with their postponed flags. -/
def handledOf' (s1 : BeaconState) (x : Nat × Gwei × List Bool) :
    List (PendingDeposit × Bool) :=
  (s1.pending_deposits.take x.1).zip x.2.2

/-- The top-up sums of a decision. -/
def sumsOf (s1 : BeaconState) (x : Nat × Gwei × List Bool) : SpecM (List Gwei) :=
  (knownTopups (s1.validators.map (·.pubkey)) (handledOf' s1 x)).foldlM
    (fun acc y => increase_balance acc y.1 y.2) (List.replicate s1.validators.length 0)

/-- The new pending deposit queue of a decision. -/
def queueOf (s1 : BeaconState) (x : Nat × Gwei × List Bool) : List PendingDeposit :=
  s1.pending_deposits.drop x.1 ++ ((handledOf' s1 x).filter (·.2)).map (·.1)

/-- The deposits for new pubkeys of a decision. -/
def newsOf (s1 : BeaconState) (x : Nat × Gwei × List Bool) : List PendingDeposit :=
  newDeposits (s1.validators.map (·.pubkey)) (handledOf' s1 x)

/-- The common form after the decisions and the sums. -/
def specNfAfter (o : Oracle) (s1 S4 : BeaconState) (total_active_balance : Gwei)
    (x : Nat × Gwei × List Bool) (sums : List Gwei) : SpecM BeaconState := do
  let n := s1.validators.length
  let named := consolidationIndices s1
  let t1 ← addSums S4 sums
  let t2 ← ((List.range n).filter (fun i => !decide (i ∈ named))).foldlM ebUpdateAt t1
  let t3 ← process_eth1_data_reset Preset.mainnet t2
  let t4 ← writeQueue (queueOf s1 x) x.2.1 t3
  let t5 ← (newsOf s1 x).foldlM (apply_pending_deposit Preset.mainnet o) t4
  let t6 ← (List.range' n (t5.validators.length - n)).foldlM ebUpdateAt t5
  let t7 ← process_pending_consolidations Preset.mainnet t6
  let t8 ← named.foldlM ebUpdateAt t7
  let t9 ← process_builder_pending_payments Preset.mainnet total_active_balance t8
  epochRest2 o t9

/-- `tailNF` in pieces. -/
theorem tailNF_eq_spec (o : Oracle) (s1 S4 : BeaconState) (total_active_balance : Gwei)
    (current_epoch : Epoch) :
    tailNF o s1 S4 total_active_balance current_epoch =
      (decOf s1 total_active_balance current_epoch >>= fun x => sumsOf s1 x >>= fun sums =>
        specNfAfter o s1 S4 total_active_balance x sums) := by
  unfold tailNF decOf
  simp only [bind_assoc]
  rfl

/-! ## The total active balance stays the same -/

/-- `process_pending_deposits` keeps `ValAct`. -/
theorem process_pending_deposits_valAct (o : Oracle) (tab : Gwei) (S S6 : BeaconState)
    (h : process_pending_deposits Preset.mainnet tab (apply_pending_deposit Preset.mainnet o) S =
      .ok S6) : ValAct S S6 := by
  unfold process_pending_deposits at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, _, _, _, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := forIn_ok_inv
    (fun r : MProd (List PendingDeposit) (MProd Bool (MProd Uint64 (MProd Uint64 BeaconState))) =>
      ValAct S r.2.2.2.2) _ _ ?_ _ _ (ValAct.refl S) hr
  · simp only [bind, Except.bind, pure, Except.pure] at h
    repeat' split at h
    all_goals first
      | contradiction
      | (cases h; exact hP.of_eq rfl rfl)
  · intro _ _ ⟨_, _, _, _, _⟩ r hst hb
    simp only [bind, Except.bind, pure, Except.pure] at hb
    repeat' split at hb
    all_goals first
      | contradiction
      | (cases hb; exact hst)
      | (cases hb
         exact apply_valAct o S _ _ _ hst ‹apply_pending_deposit _ _ _ _ = _›)

/-- The spec tail after the eth1 reset, with one total active balance. -/
def specTail (o : Oracle) (tab : Gwei) (S5 : BeaconState) : SpecM BeaconState := do
  let S6 ← process_pending_deposits Preset.mainnet tab (apply_pending_deposit Preset.mainnet o) S5
  let S7 ← process_pending_consolidations Preset.mainnet S6
  let S8 ← process_builder_pending_payments Preset.mainnet tab S7
  let S9 ← process_effective_balance_updates Preset.mainnet S8
  epochRest2 o S9

/-- In `epochRest`, both totals are the total of the state before it. -/
theorem epochRest_specTail (o : Oracle) (S4 : BeaconState) (tab : Gwei) (cur : Epoch)
    (hcur : get_current_epoch Preset.mainnet S4 = .ok cur) (hfar : cur < FAR_FUTURE_EPOCH)
    (htab : get_total_active_balance Preset.mainnet S4 = .ok tab) :
    SameOk (epochRest Preset.mainnet o S4)
      (process_eth1_data_reset Preset.mainnet S4 >>= specTail o tab) := by
  unfold epochRest
  refine SameOk.bind (SameOk.refl _) (fun S5 h5 => ?_)
  obtain ⟨v, rfl⟩ := eth1_shape h5
  have hcur5 : get_current_epoch Preset.mainnet { S4 with eth1_data_votes := v } = .ok cur := hcur
  have htab5 : get_total_active_balance Preset.mainnet { S4 with eth1_data_votes := v } =
      .ok tab := by
    rw [get_total_active_balance_append S4 { S4 with eth1_data_votes := v } [] cur hcur rfl
      (by simp) (by simp) hfar]
    exact htab
  rw [htab5, ok_bind']
  unfold specTail
  refine SameOk.bind (SameOk.refl _) (fun S6 h6 => ?_)
  refine SameOk.bind (SameOk.refl _) (fun S7 h7 => ?_)
  have hva := process_pending_deposits_valAct o tab _ S6 h6
  have hc7 := process_pending_consolidations_checkpointsStable Preset.mainnet S6 S7 h7
  have hv7 := (consolidations_frame S6 S7 h7).1
  obtain ⟨hs7, extra, hv7', hext⟩ := hva.of_eq hc7.1 hv7
  have htab7 : get_total_active_balance Preset.mainnet S7 = .ok tab := by
    rw [get_total_active_balance_append _ S7 extra cur hcur5 hs7 hv7' hext hfar]
    exact htab5
  rw [htab7, ok_bind']
  exact SameOk.refl _

/-! ## The deposits in pieces -/

/-- The decide-then-apply form of the deposits, with projections in place of the match. -/
theorem decide_form_eq (o : Oracle) (tab : Gwei) (s : BeaconState) (current_epoch : Epoch) :
    (do
      let finalized_slot ←
        compute_start_slot_at_epoch Preset.mainnet s.finalized_checkpoint.epoch
      let (next, dbtc, postponed) ← depositDecisions Preset.mainnet finalized_slot
        s.deposit_balance_to_consume
        (← get_activation_churn_limit Preset.mainnet tab)
        (s.pending_deposits.map (specDepositView s (current_epoch + 1)))
      let handled := (s.pending_deposits.take next).zip postponed
      let s' ← applyHandled o s handled
      pure { s' with
        pending_deposits := s.pending_deposits.drop next ++ (handled.filter (·.2)).map (·.1)
        deposit_balance_to_consume := dbtc } : SpecM BeaconState) =
    (compute_start_slot_at_epoch Preset.mainnet s.finalized_checkpoint.epoch >>= fun fs =>
      get_activation_churn_limit Preset.mainnet tab >>= fun c =>
      depositDecisions Preset.mainnet fs s.deposit_balance_to_consume c
        (s.pending_deposits.map (specDepositView s (current_epoch + 1))) >>= fun x =>
      applyHandled o s ((s.pending_deposits.take x.1).zip x.2.2) >>= fun s' =>
      pure { s' with
        pending_deposits := s.pending_deposits.drop x.1 ++
          (((s.pending_deposits.take x.1).zip x.2.2).filter (·.2)).map (·.1)
        deposit_balance_to_consume := x.2.1 }) := by
  rfl

/-- The spec tail after the sums, before any reordering. -/
def innerAfter (o : Oracle) (s1 : BeaconState) (tab : Gwei) (S5 : BeaconState)
    (x : Nat × Gwei × List Bool) (sums : List Gwei) : SpecM BeaconState := do
  let t ← addSums S5 sums
  let t' ← (newsOf s1 x).foldlM (apply_pending_deposit Preset.mainnet o) t
  let S7 ← process_pending_consolidations Preset.mainnet
    { t' with pending_deposits := queueOf s1 x, deposit_balance_to_consume := x.2.1 }
  let S8 ← process_builder_pending_payments Preset.mainnet tab S7
  let S9 ← process_effective_balance_updates Preset.mainnet S8
  epochRest2 o S9

/-- The spec deposits, decided once on the predicted views and with summed top-ups. -/
theorem specTail_inner (o : Oracle) (s1 S5 : BeaconState) (tab : Gwei) (cur : Epoch)
    (hcur5 : get_current_epoch Preset.mainnet S5 = .ok cur)
    (hfar : cur + 1 ≤ FAR_FUTURE_EPOCH)
    (hpend : S5.pending_deposits = s1.pending_deposits)
    (hdbtc : S5.deposit_balance_to_consume = s1.deposit_balance_to_consume)
    (hfinal : S5.finalized_checkpoint = s1.finalized_checkpoint)
    (hpk : S5.validators.map (·.pubkey) = s1.validators.map (·.pubkey))
    (hviews : (s1.pending_deposits.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH).map
        (specDepositView S5 (cur + 1)) =
      (s1.pending_deposits.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH).map
        (lhDepositView s1 cur))
    (hlen5 : S5.balances.length = S5.validators.length)
    (hbs5 : ∀ i (h : i < S5.balances.length), S5.balances[i] < UINT64_SIZE) :
    SameOk (specTail o tab S5)
      (decOf s1 tab cur >>= fun x => sumsOf s1 x >>= fun sums =>
        innerAfter o s1 tab S5 x sums) := by
  have hn : S5.validators.length = s1.validators.length := by
    rw [← List.length_map (f := (·.pubkey)), hpk, List.length_map]
  have hD := process_pending_deposits_decide o tab S5 cur hcur5 hfar
  rw [decide_form_eq] at hD
  unfold specTail
  refine SameOk.trans (sameOk_cont hD _) ?_
  unfold decOf
  simp only [bind_assoc]
  rw [hpend, hdbtc, hfinal]
  refine SameOk.bind (SameOk.refl _) (fun fs _ => ?_)
  refine SameOk.bind (SameOk.refl _) (fun c _ => ?_)
  rw [depositDecisions_take_max, ← List.map_take, hviews]
  refine SameOk.bind (SameOk.refl _) (fun x _ => ?_)
  have hsplit := applyHandled_split o S5 ((s1.pending_deposits.take x.1).zip x.2.2)
  rw [hpk] at hsplit
  refine SameOk.trans (sameOk_cont hsplit _) ?_
  have hin : ∀ y ∈ knownTopups (s1.validators.map (·.pubkey))
      ((s1.pending_deposits.take x.1).zip x.2.2), y.1 < S5.validators.length := by
    intro y hy
    unfold knownTopups at hy
    obtain ⟨z, hz, rfl⟩ := List.mem_map.mp hy
    have hm : z.1.pubkey ∈ s1.validators.map (·.pubkey) := by
      have := (List.mem_filter.mp hz).2
      simp only [Bool.and_eq_true, decide_eq_true_eq] at this
      exact this.2
    rw [hn]
    have := List.idxOf_lt_length_of_mem hm
    simpa using this
  have hsum := topups_sum S5 _ hin hlen5 hbs5
  refine SameOk.trans (sameOk_cont (sameOk_cont hsum _) _) ?_
  apply SameOk.of_eq
  simp only [bind_assoc, sumsOf, innerAfter, handledOf', newsOf, queueOf, hn]
  rfl

/-! ## Moving the effective balance updates -/

/-- A fold of updates moves before a step that commutes with each update. -/
theorem fold_past (F : BeaconState → SpecM BeaconState) (P : BeaconState → Prop)
    (l : List Nat) (hP : ∀ i ∈ l, ∀ t e, P t → P (setEB t i e))
    (hF1 : ∀ i ∈ l, ∀ t e, P t → F (setEB t i e) = (fun t' => setEB t' i e) <$> F t)
    (hF2 : ∀ i ∈ l, ∀ t t', P t → F t = .ok t' →
      t'.validators[i]? = t.validators[i]? ∧ t'.balances[i]? = t.balances[i]?)
    (s : BeaconState) (hs : P s) (k : BeaconState → SpecM BeaconState) :
    SameOk (F s >>= fun t => l.foldlM ebUpdateAt t >>= k)
      (l.foldlM ebUpdateAt s >>= fun t => F t >>= k) :=
  (sameOk_swap_cont (A := fun t => l.foldlM ebUpdateAt t) (B := F)
    (ebFold_commute F P l hP hF1 hF2 s hs) k).flip

/-- A fold of updates keeps the number of validators. -/
theorem ebFold_len : ∀ (l : List Nat) (t t' : BeaconState),
    l.foldlM ebUpdateAt t = .ok t' → t'.validators.length = t.validators.length
  | [], t, t', h => by cases h; rfl
  | i :: l, t, t', h => by
    rw [List.foldlM_cons] at h
    obtain ⟨t1, h1, h⟩ := specM_bind_ok h
    obtain ⟨v, b, e, -, -, -, rfl⟩ := (ebUpdateAt_ok t t1 i).mp h1
    rw [ebFold_len l _ t' h, setEB_length]

/-- A fold of updates keeps `consolidationIndices`. -/
theorem ebFold_ci : ∀ (l : List Nat) (t t' : BeaconState),
    l.foldlM ebUpdateAt t = .ok t' → consolidationIndices t' = consolidationIndices t
  | [], t, t', h => by cases h; rfl
  | i :: l, t, t', h => by
    rw [List.foldlM_cons] at h
    obtain ⟨t1, h1, h⟩ := specM_bind_ok h
    obtain ⟨v, b, e, -, -, -, rfl⟩ := (ebUpdateAt_ok t t1 i).mp h1
    rw [ebFold_ci l _ t' h, consolidationIndices_setEB]

/-- `add_validator_to_registry` keeps the pending consolidations. -/
theorem add_validator_pc (t t' : BeaconState) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei)
    (h : add_validator_to_registry Preset.mainnet t pubkey withdrawal_credentials amount =
      .ok t') : t'.pending_consolidations = t.pending_consolidations := by
  unfold add_validator_to_registry at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; rfl)

/-- `apply_pending_deposit` keeps the pending consolidations. -/
theorem apply_pc (o : Oracle) (d : PendingDeposit) (t t' : BeaconState)
    (h : apply_pending_deposit Preset.mainnet o t d = .ok t') :
    t'.pending_consolidations = t.pending_consolidations := by
  unfold apply_pending_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · split at h
    · exact add_validator_pc t t' _ _ _ h
    · cases h; rfl
  · split at h
    · contradiction
    · cases h; rfl

/-- A fold of deposits keeps the pending consolidations, and does not shrink the validators. -/
theorem applyFold_pc (o : Oracle) : ∀ (ds : List PendingDeposit) (t t' : BeaconState),
    ds.foldlM (apply_pending_deposit Preset.mainnet o) t = .ok t' →
      t'.pending_consolidations = t.pending_consolidations ∧
        t.validators.length ≤ t'.validators.length
  | [], t, t', h => by cases h; exact ⟨rfl, Nat.le_refl _⟩
  | d :: ds, t, t', h => by
    rw [List.foldlM_cons] at h
    obtain ⟨t1, h1, h⟩ := specM_bind_ok h
    obtain ⟨hp, hl⟩ := applyFold_pc o ds t1 t' h
    obtain ⟨⟨ys, hys⟩, -⟩ := apply_frame o d t t1 h1
    refine ⟨hp.trans (apply_pc o d t t1 h1), ?_⟩
    rw [hys, List.length_append] at hl
    exact Nat.le_trans (Nat.le_add_right _ _) hl

/-- The validators below `n` that `named` does not hold. -/
def ebL1 (n : Nat) (named : List Nat) : List Nat :=
  (List.range n).filter (fun i => !decide (i ∈ named))

/-- The validators from `n` to the end of `t`. -/
def ebL2 (n : Nat) (t : BeaconState) : List Nat :=
  List.range' n (t.validators.length - n)

/-- The effective balance updates after the builder payments move before them, in three
groups. -/
theorem inner_py (o : Oracle) (tab : Gwei) (n : Nat) (named : List Nat) (hnd : named.Nodup)
    (hsub : ∀ i ∈ named, i < n) (t7 : BeaconState) (hn : n ≤ t7.validators.length) :
    SameOk (process_builder_pending_payments Preset.mainnet tab t7 >>= fun t8 =>
        process_effective_balance_updates Preset.mainnet t8 >>= epochRest2 o)
      ((ebL1 n named ++ ebL2 n t7 ++ named).foldlM ebUpdateAt t7 >>= fun t =>
        process_builder_pending_payments Preset.mainnet tab t >>= epochRest2 o) := by
  refine SameOk.trans (y := process_builder_pending_payments Preset.mainnet tab t7 >>= fun t8 =>
      (ebL1 n named ++ ebL2 n t7 ++ named).foldlM ebUpdateAt t8 >>= epochRest2 o)
    (SameOk.bind (SameOk.refl _) (fun t8 h8 => ?_)) ?_
  · have hv := (payments_frame tab t7 t8 h8).1
    refine SameOk.trans (sameOk_cont (process_effective_balance_updates_fold t8) _) ?_
    refine sameOk_cont (ebFold_perm ?_ t8) _
    rw [hv]
    exact range_perm_split n t7.validators.length named hnd hsub hn
  · exact fold_past _ (fun _ => True) _ (fun _ _ _ _ _ => trivial)
      (fun i _ t e _ => payments_hF1 tab i t e) (fun i _ t t' _ h => payments_hF2 tab i t t' h)
      t7 trivial _

/-- No validator of the first two groups is named by a consolidation. -/
theorem ebL12_not_named (n : Nat) (named : List Nat) (hsub : ∀ i ∈ named, i < n)
    (t : BeaconState) (i : Nat) (hi : i ∈ ebL1 n named ++ ebL2 n t) : i ∉ named := by
  rcases List.mem_append.mp hi with h | h
  · unfold ebL1 at h
    have := (List.mem_filter.mp h).2
    simpa using this
  · unfold ebL2 at h
    intro hm
    have h1 := (List.mem_range'_1.mp h).1
    have h2 := hsub i hm
    omega

/-- The updates of the first two groups move before the consolidations. -/
theorem inner_co (o : Oracle) (tab : Gwei) (n : Nat) (named : List Nat) (hnd : named.Nodup)
    (hsub : ∀ i ∈ named, i < n) (t5 : BeaconState) (hn : n ≤ t5.validators.length)
    (hci : consolidationIndices t5 = named) :
    SameOk (process_pending_consolidations Preset.mainnet t5 >>= fun t7 =>
        process_builder_pending_payments Preset.mainnet tab t7 >>= fun t8 =>
        process_effective_balance_updates Preset.mainnet t8 >>= epochRest2 o)
      ((ebL1 n named ++ ebL2 n t5).foldlM ebUpdateAt t5 >>= fun t6 =>
        process_pending_consolidations Preset.mainnet t6 >>= fun t7 =>
        named.foldlM ebUpdateAt t7 >>= fun t8 =>
        process_builder_pending_payments Preset.mainnet tab t8 >>= epochRest2 o) := by
  refine SameOk.trans (y := process_pending_consolidations Preset.mainnet t5 >>= fun t7 =>
      (ebL1 n named ++ ebL2 n t5).foldlM ebUpdateAt t7 >>= fun t6 =>
        named.foldlM ebUpdateAt t6 >>= fun t8 =>
        process_builder_pending_payments Preset.mainnet tab t8 >>= epochRest2 o)
    (SameOk.bind (SameOk.refl _) (fun t7 h7 => ?_)) ?_
  · have hv := (consolidations_frame t5 t7 h7).1
    have hl2 : ebL2 n t7 = ebL2 n t5 := by unfold ebL2; rw [hv]
    refine SameOk.trans (inner_py o tab n named hnd hsub t7 (by rw [hv]; exact hn)) ?_
    rw [hl2, List.foldlM_append, bind_assoc]
    exact SameOk.refl _
  · refine fold_past _ (fun t => consolidationIndices t = named) _
      (fun i _ t e h => by rw [consolidationIndices_setEB]; exact h)
      (fun i hi t e h => consolidations_hF1 i t e
        (by rw [h]; exact ebL12_not_named n named hsub t5 i hi))
      (fun i hi t t' h h' => consolidations_hF2 i t t'
        (by rw [h]; exact ebL12_not_named n named hsub t5 i hi) h') t5 hci _

/-- `consolidationIndices` reads only the pending consolidations. -/
theorem consolidationIndices_of_pc {t t' : BeaconState}
    (h : t'.pending_consolidations = t.pending_consolidations) :
    consolidationIndices t' = consolidationIndices t := by
  unfold consolidationIndices
  rw [h]

/-- The updates of the old validators that no consolidation names move before the deposits for
new pubkeys. -/
theorem inner_news (o : Oracle) (tab : Gwei) (n : Nat) (named : List Nat) (hnd : named.Nodup)
    (hsub : ∀ i ∈ named, i < n) (Pk : List BLSPubkey) (ds : List PendingDeposit)
    (hds : ∀ d ∈ ds, d.pubkey ∉ Pk) (t3 : BeaconState) (hlen : t3.validators.length = n)
    (hq : ∀ i ∈ ebL1 n named, NewsQ Pk i t3) (hci : consolidationIndices t3 = named) :
    SameOk (ds.foldlM (apply_pending_deposit Preset.mainnet o) t3 >>= fun t5 =>
        process_pending_consolidations Preset.mainnet t5 >>= fun t7 =>
        process_builder_pending_payments Preset.mainnet tab t7 >>= fun t8 =>
        process_effective_balance_updates Preset.mainnet t8 >>= epochRest2 o)
      ((ebL1 n named).foldlM ebUpdateAt t3 >>= fun t4 =>
        ds.foldlM (apply_pending_deposit Preset.mainnet o) t4 >>= fun t5 =>
        (ebL2 n t5).foldlM ebUpdateAt t5 >>= fun t6 =>
        process_pending_consolidations Preset.mainnet t6 >>= fun t7 =>
        named.foldlM ebUpdateAt t7 >>= fun t8 =>
        process_builder_pending_payments Preset.mainnet tab t8 >>= epochRest2 o) := by
  refine SameOk.trans (y := ds.foldlM (apply_pending_deposit Preset.mainnet o) t3 >>= fun t5 =>
      (ebL1 n named).foldlM ebUpdateAt t5 >>= fun t4 =>
        (ebL2 n t4).foldlM ebUpdateAt t4 >>= fun t6 =>
        process_pending_consolidations Preset.mainnet t6 >>= fun t7 =>
        named.foldlM ebUpdateAt t7 >>= fun t8 =>
        process_builder_pending_payments Preset.mainnet tab t8 >>= epochRest2 o)
    (SameOk.bind (SameOk.refl _) (fun t5 h5 => ?_)) ?_
  · obtain ⟨hpc, hle⟩ := applyFold_pc o ds t3 t5 h5
    refine SameOk.trans (inner_co o tab n named hnd hsub t5 (hlen ▸ hle)
      ((consolidationIndices_of_pc hpc).trans hci)) ?_
    rw [List.foldlM_append, bind_assoc]
    refine SameOk.bind (SameOk.refl _) (fun t4 h4 => ?_)
    have hl2 : ebL2 n t4 = ebL2 n t5 := by unfold ebL2; rw [ebFold_len _ _ _ h4]
    rw [hl2]
    exact SameOk.refl _
  · exact fold_past (fun t => ds.foldlM (apply_pending_deposit Preset.mainnet o) t)
      (fun t => ∀ i ∈ ebL1 n named, NewsQ Pk i t) _
      (fun i _ t e h j hj => (h j hj).setEB i e)
      (fun i hi t e h => newsFold_hF1 o Pk i e ds hds t (h i hi))
      (fun i hi t t' h h' => newsFold_hF2 o Pk i ds hds t t' (h i hi) h') t3 hq _

/-- A validator below the length has `NewsQ` when the pubkeys are `Pk`. -/
theorem newsQ_of_pk {Pk : List BLSPubkey} {t : BeaconState} (hpk : t.validators.map (·.pubkey) = Pk)
    (i : Nat) (hi : i < t.validators.length) : NewsQ Pk i t := by
  refine ⟨hi, fun v hv => ?_⟩
  rw [← hpk]
  exact List.mem_map.mpr ⟨v, List.mem_of_getElem? hv, rfl⟩

/-- The updates of the old validators that no consolidation names move before the eth1 reset,
the queue write and the deposits for new pubkeys. -/
theorem inner_t1 (o : Oracle) (tab : Gwei) (n : Nat) (named : List Nat) (hnd : named.Nodup)
    (hsub : ∀ i ∈ named, i < n) (Pk : List BLSPubkey) (ds : List PendingDeposit)
    (hds : ∀ d ∈ ds, d.pubkey ∉ Pk) (q : List PendingDeposit) (dbtc : Gwei) (t1 : BeaconState)
    (hpk : t1.validators.map (·.pubkey) = Pk) (hlen : t1.validators.length = n)
    (hci : consolidationIndices t1 = named) :
    SameOk (process_eth1_data_reset Preset.mainnet t1 >>= fun t2 => writeQueue q dbtc t2 >>=
        fun t3 => ds.foldlM (apply_pending_deposit Preset.mainnet o) t3 >>= fun t5 =>
        process_pending_consolidations Preset.mainnet t5 >>= fun t7 =>
        process_builder_pending_payments Preset.mainnet tab t7 >>= fun t8 =>
        process_effective_balance_updates Preset.mainnet t8 >>= epochRest2 o)
      ((ebL1 n named).foldlM ebUpdateAt t1 >>= fun t2 =>
        process_eth1_data_reset Preset.mainnet t2 >>= fun t3 => writeQueue q dbtc t3 >>= fun t4 =>
        ds.foldlM (apply_pending_deposit Preset.mainnet o) t4 >>= fun t5 =>
        (ebL2 n t5).foldlM ebUpdateAt t5 >>= fun t6 =>
        process_pending_consolidations Preset.mainnet t6 >>= fun t7 =>
        named.foldlM ebUpdateAt t7 >>= fun t8 =>
        process_builder_pending_payments Preset.mainnet tab t8 >>= epochRest2 o) := by
  have hl1 : ∀ i ∈ ebL1 n named, i < n := by
    intro i hi
    unfold ebL1 at hi
    exact List.mem_range.mp (List.mem_filter.mp hi).1
  refine SameOk.trans (y := process_eth1_data_reset Preset.mainnet t1 >>= fun t2 =>
      writeQueue q dbtc t2 >>= fun t3 => (ebL1 n named).foldlM ebUpdateAt t3 >>= fun t4 =>
      ds.foldlM (apply_pending_deposit Preset.mainnet o) t4 >>= fun t5 =>
      (ebL2 n t5).foldlM ebUpdateAt t5 >>= fun t6 =>
      process_pending_consolidations Preset.mainnet t6 >>= fun t7 =>
      named.foldlM ebUpdateAt t7 >>= fun t8 =>
      process_builder_pending_payments Preset.mainnet tab t8 >>= epochRest2 o)
    (SameOk.bind (SameOk.refl _) (fun t2 h2 =>
      SameOk.bind (SameOk.refl _) (fun t3 h3 => ?_))) ?_
  · obtain ⟨v, rfl⟩ := eth1_shape h2
    cases h3
    exact inner_news o tab n named hnd hsub Pk ds hds _ hlen
      (fun i hi => newsQ_of_pk hpk i (hlen ▸ hl1 i hi)) hci
  · refine SameOk.trans (SameOk.bind (SameOk.refl _) (fun t2 _ =>
      fold_past (writeQueue q dbtc) (fun _ => True) _ (fun _ _ _ _ _ => trivial)
        (fun i _ t e _ => writeQueue_hF1 q dbtc i t e)
        (fun i _ t t' _ h => writeQueue_hF2 q dbtc i t t' h) t2 trivial _)) ?_
    exact fold_past (process_eth1_data_reset Preset.mainnet) (fun _ => True) _
      (fun _ _ _ _ _ => trivial) (fun i _ t e _ => eth1_hF1 i t e)
      (fun i _ t t' _ h => eth1_hF2 i t t' h) t1 trivial _

/-- `addSums` keeps every field but the balances. -/
theorem addSums_shape {S T : BeaconState} {sums : List Gwei} (h : addSums S sums = .ok T) :
    ∃ bs, T = { S with balances := bs } := by
  rw [addSums_eq] at h
  cases hf : ((List.range S.validators.length).map fun k => (k, sums.getD k 0)).foldlM
      (fun acc x => increase_balance acc x.1 x.2) S.balances with
  | error e => rw [hf] at h; cases h
  | ok bs => rw [hf] at h; cases h; exact ⟨bs, rfl⟩

/-- A deposit for a new pubkey has a pubkey outside the old ones. -/
theorem newsOf_not_mem (s1 : BeaconState) (x : Nat × Gwei × List Bool) :
    ∀ d ∈ newsOf s1 x, d.pubkey ∉ s1.validators.map (·.pubkey) := by
  intro d hd
  unfold newsOf newDeposits at hd
  obtain ⟨z, hz, rfl⟩ := List.mem_map.mp hd
  have := (List.mem_filter.mp hz).2
  simp only [Bool.and_eq_true, Bool.not_eq_true', decide_eq_false_iff_not] at this
  exact this.2

/-- After the eth1 reset, the spec steps reach the common form for one decision. -/
theorem specAfter_nf (o : Oracle) (s1 S4 : BeaconState) (tab : Gwei)
    (x : Nat × Gwei × List Bool) (sums : List Gwei)
    (hpk : S4.validators.map (·.pubkey) = s1.validators.map (·.pubkey))
    (hpc : S4.pending_consolidations = s1.pending_consolidations)
    (hnamed : ∀ i ∈ consolidationIndices s1, i < s1.validators.length) :
    SameOk (process_eth1_data_reset Preset.mainnet S4 >>= fun S5 =>
        innerAfter o s1 tab S5 x sums)
      (specNfAfter o s1 S4 tab x sums) := by
  have hn : S4.validators.length = s1.validators.length := by
    rw [← List.length_map (f := (·.pubkey)), hpk, List.length_map]
  have hB := addSums_eth1 S4 sums
  refine SameOk.trans (sameOk_swap_cont (A := process_eth1_data_reset Preset.mainnet)
    (B := fun S => addSums S sums) (eth1_comm _ S4 hB.1 hB.2) _) ?_
  unfold specNfAfter
  refine SameOk.bind (SameOk.refl _) (fun t1 h1 => ?_)
  obtain ⟨bs, rfl⟩ := addSums_shape h1
  refine SameOk.trans (SameOk.bind (SameOk.refl _) (fun t2 _ =>
    SameOk.of_eq (wq_news_comm o (queueOf s1 x) x.2.1 (newsOf s1 x) t2 (fun S6 =>
      process_pending_consolidations Preset.mainnet S6 >>= fun S7 =>
      process_builder_pending_payments Preset.mainnet tab S7 >>= fun S8 =>
      process_effective_balance_updates Preset.mainnet S8 >>= epochRest2 o)))) ?_
  exact inner_t1 o tab s1.validators.length (consolidationIndices s1)
    (consolidationIndices_nodup s1) hnamed _ (newsOf s1 x) (newsOf_not_mem s1 x)
    (queueOf s1 x) x.2.1 _ hpk hn (consolidationIndices_of_pc hpc)

/-! ## The spec tail reaches the common form -/

/-- After the Lighthouse pass over the rows of `s1`, the validators keep their pubkeys. -/
theorem pass_pubkeys (ctx : LhStepContext) (bf : Row → Gwei) (s1 : BeaconState)
    (churn0 churn : Epoch × Gwei) (rows : List Row) (activation_epoch : Epoch)
    (hfinalized : ctx.finalized_epoch ≤ ctx.current_epoch)
    (hexit : ∀ i (h : i < s1.validators.length), s1.validators[i].exit_epoch ≤ FAR_FUTURE_EPOCH)
    (hcurrent : ctx.current_epoch + 1 < UINT64_SIZE)
    (hactivation : compute_activation_exit_epoch Preset.mainnet ctx.current_epoch =
      .ok activation_epoch)
    (h : passM (fun c r => lhRowStep Preset.mainnet ctx (bf r) c r) churn0 (rowsOf s1) =
      .ok (churn, rows)) :
    (rows.map (·.validator)).map (·.pubkey) = s1.validators.map (·.pubkey) := by
  obtain ⟨hlen, hget⟩ := passM_rows_registry ctx bf s1 churn0 churn rows activation_epoch
    hfinalized hexit hcurrent hactivation h
  apply List.ext_getElem
  · simp [hlen]
  · intro i h1 h2
    have h1' : i < rows.length := by simpa using h1
    have h2' : i < s1.validators.length := by simpa using h2
    obtain ⟨c, g, -, hv⟩ := hget i h2' h1'
    simp only [List.getElem_map, hv]
    rfl

/-- The spec steps after the four passes give the same `ok` results as `tailNF`. -/
theorem epochRest_nf (o : Oracle) (s1 : BeaconState) (current_epoch : Epoch)
    (total_active_balance : Gwei) (ctx : LhStepContext) (bf : Row → Gwei)
    (churn : Epoch × Gwei) (rows : List Row) (activation_epoch : Epoch)
    (hpass : passM (fun c r => lhRowStep Preset.mainnet ctx (bf r) c r)
      (s1.earliest_exit_epoch, s1.exit_balance_to_consume) (rowsOf s1) = .ok (churn, rows))
    (hctxcur : ctx.current_epoch = current_epoch)
    (hctxfin : ctx.finalized_epoch = s1.finalized_checkpoint.epoch)
    (hcur : get_current_epoch Preset.mainnet s1 = .ok current_epoch)
    (hfin : s1.finalized_checkpoint.epoch ≤ current_epoch)
    (hnext : current_epoch + 1 < FAR_FUTURE_EPOCH)
    (hact : compute_activation_exit_epoch Preset.mainnet current_epoch = .ok activation_epoch)
    (hexit : ∀ i (h : i < s1.validators.length), s1.validators[i].exit_epoch ≤ FAR_FUTURE_EPOCH)
    (hvalid : ∀ v ∈ s1.validators, v.exit_epoch = FAR_FUTURE_EPOCH →
      v.withdrawable_epoch = FAR_FUTURE_EPOCH)
    (htab4 : get_total_active_balance Preset.mainnet (s1.withRowsChurn churn rows) =
      .ok total_active_balance)
    (hbal4 : ∀ r ∈ rows, r.balance < 2 ^ 64)
    (hnamed : ∀ i ∈ consolidationIndices s1, i < s1.validators.length) :
    SameOk (epochRest Preset.mainnet o (s1.withRowsChurn churn rows))
      (tailNF o s1 (s1.withRowsChurn churn rows) total_active_balance current_epoch) := by
  subst hctxcur
  have hfinalized : ctx.finalized_epoch ≤ ctx.current_epoch := by rw [hctxfin]; exact hfin
  have hcur64 : ctx.current_epoch + 1 < UINT64_SIZE := by
    have h' : @LT.lt Nat _ (ctx.current_epoch + 1) (2 ^ 64 - 1) := hnext
    show @LT.lt Nat _ (ctx.current_epoch + 1) (2 ^ 64)
    omega
  have hfar : ctx.current_epoch < FAR_FUTURE_EPOCH :=
    Nat.lt_trans (Nat.lt_succ_self _) hnext
  have hpk := pass_pubkeys ctx bf s1 _ churn rows activation_epoch hfinalized hexit hcur64
    hact hpass
  have hcur4 : get_current_epoch Preset.mainnet (s1.withRowsChurn churn rows) =
      .ok ctx.current_epoch := hcur
  refine SameOk.trans (epochRest_specTail o _ total_active_balance ctx.current_epoch hcur4 hfar
    htab4) ?_
  refine SameOk.trans (y := process_eth1_data_reset Preset.mainnet (s1.withRowsChurn churn rows) >>=
      fun S5 => decOf s1 total_active_balance ctx.current_epoch >>= fun x => sumsOf s1 x >>=
        fun sums => innerAfter o s1 total_active_balance S5 x sums)
    (SameOk.bind (SameOk.refl _) (fun S5 h5 => ?_)) ?_
  · obtain ⟨v, rfl⟩ := eth1_shape h5
    have hviews := views_eq ctx bf s1 _ churn rows activation_epoch hfinalized hexit hvalid
      hnext hact hpass { s1.withRowsChurn churn rows with eth1_data_votes := v } rfl
      (s1.pending_deposits.take Preset.mainnet.MAX_PENDING_DEPOSITS_PER_EPOCH)
    refine specTail_inner o s1 _ total_active_balance ctx.current_epoch hcur (Nat.le_of_lt hnext)
      rfl rfl rfl hpk hviews (by simp [BeaconState.withRowsChurn, BeaconState.withRows]) ?_
    intro i hi
    simp only [BeaconState.withRowsChurn, BeaconState.withRows, List.getElem_map]
    exact hbal4 _ (List.getElem_mem _)
  · refine SameOk.trans (sameOk_bind_comm _ _ _) ?_
    refine SameOk.trans (SameOk.bind (SameOk.refl _) (fun x _ => sameOk_bind_comm _ _ _)) ?_
    rw [tailNF_eq_spec]
    refine SameOk.bind (SameOk.refl _) (fun x _ => SameOk.bind (SameOk.refl _) (fun sums _ => ?_))
    exact specAfter_nf o s1 _ total_active_balance x sums hpk rfl hnamed

end EpochProofs.Spec
