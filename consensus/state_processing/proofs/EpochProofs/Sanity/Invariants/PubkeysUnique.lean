import EpochProofs.Sanity.Invariants.ExitEpochs

/-!
# Validator pubkeys are unique

`PubkeysUnique` says that no two validators share a pubkey. Lighthouse looks deposits up by
pubkey in a map, so it needs this. Every block step keeps the pubkey list (`PkSame`).
`process_epoch` keeps the list too, except that a pending deposit appends a validator, and only
for a pubkey that is not in the list.
-/

namespace EpochProofs.Spec

/-- No two validators share a pubkey. -/
def PubkeysUnique (s : BeaconState) : Prop :=
  (s.validators.map (·.pubkey)).Nodup

/-- The list of validator pubkeys does not change. -/
def PkSame (s s' : BeaconState) : Prop :=
  s'.validators.map (·.pubkey) = s.validators.map (·.pubkey)

/-- A state has the same pubkeys as itself. -/
theorem PkSame.refl (s : BeaconState) : PkSame s s := rfl

/-- `PkSame` composes. -/
theorem PkSame.trans {s1 s2 s3 : BeaconState} (h12 : PkSame s1 s2) (h23 : PkSame s2 s3) :
    PkSame s1 s3 :=
  Eq.trans h23 h12

/-- Same validators keep the pubkeys. -/
theorem PkSame.of_eq {s s' : BeaconState} (hv : s'.validators = s.validators) : PkSame s s' := by
  unfold PkSame
  rw [hv]

/-- `PkSame` keeps `PubkeysUnique`. -/
theorem PkSame.pubkeysUnique {s s' : BeaconState} (h : PkSame s s') (hs : PubkeysUnique s) :
    PubkeysUnique s' := by
  unfold PubkeysUnique
  rw [h]
  exact hs

/-- `PkSame` keeps the number of validators. -/
theorem PkSame.length {s s' : BeaconState} (h : PkSame s s') :
    s'.validators.length = s.validators.length := by
  have := congrArg List.length h
  simpa using this

/-- Same length and the same pubkey at each index give `PkSame`. -/
theorem PkSame.of_pointwise {s s' : BeaconState}
    (hlen : s'.validators.length = s.validators.length)
    (hpt : ∀ (i : Nat) (v v' : Validator), s.validators[i]? = some v →
      s'.validators[i]? = some v' → v'.pubkey = v.pubkey) : PkSame s s' := by
  unfold PkSame
  apply List.ext_getElem (by simpa using hlen)
  intro i h1 h2
  simp only [List.getElem_map]
  have hi : i < s.validators.length := by simpa using h2
  have hi' : i < s'.validators.length := by simpa using h1
  exact hpt i _ _ (List.getElem?_eq_getElem hi) (List.getElem?_eq_getElem hi')

/-- A list write that keeps the pubkey at its index keeps the pubkey list. -/
theorem map_pubkey_set {l : List Validator} {i : Nat} {v a : Validator} (hv : l[i]? = some v)
    (ha : a.pubkey = v.pubkey) : (l.set i a).map (·.pubkey) = l.map (·.pubkey) := by
  apply List.ext_getElem (by simp)
  intro j h1 h2
  simp only [List.getElem_map, List.getElem_set]
  split
  · rename_i hij
    subst hij
    rw [List.getElem?_eq_getElem (by simpa using h2)] at hv
    rw [ha, ← Option.some.inj hv]
  · rfl

/-- `initiate_validator_exit` keeps the pubkeys. -/
theorem initiate_validator_exit_pkSame {p : Preset} {tab : Gwei} {s s' : BeaconState}
    {index : Nat} (h : initiate_validator_exit p tab s index = .ok s') : PkSame s s' := by
  obtain ⟨-, hlen, hother, v, v', hv, hv', hch⟩ := initiate_validator_exit_ok p tab s s' index h
  refine .of_pointwise hlen fun j u u' hu hu' => ?_
  by_cases hj : j = index
  · subst hj
    rw [hv] at hu
    rw [hv'] at hu'
    cases hu
    cases hu'
    exact hch.keeps.1
  · rw [hother j hj, hu] at hu'
    cases hu'
    rfl

/-- `slash_validator` keeps the pubkeys. -/
theorem slash_validator_pkSame (p : Preset) (s s' : BeaconState) (i : ValidatorIndex)
    (w : Option ValidatorIndex) (h : slash_validator p s i w = .ok s') : PkSame s s' := by
  obtain ⟨-, hlen, hother, ⟨v, v', hv, hv', hch⟩, -⟩ := slash_validator_ok p s s' i w h
  refine .of_pointwise hlen fun j u u' hu hu' => ?_
  by_cases hj : j = i
  · subst hj
    rw [hv] at hu
    rw [hv'] at hu'
    cases hu
    cases hu'
    exact hch.keeps.1
  · rw [hother j hj, hu] at hu'
    cases hu'
    rfl

/-- `process_proposer_slashing` keeps the pubkeys. -/
theorem process_proposer_slashing_pkSame (p : Preset) (o : Oracle) (s s' : BeaconState)
    (ps : ProposerSlashing) (h : process_proposer_slashing p o s ps = .ok s') : PkSame s s' := by
  obtain ⟨_, _, pay, _, _, _, hsl⟩ := process_proposer_slashing_ok p o s s' ps h
  have h2 := slash_validator_pkSame p { s with builder_pending_payments := pay } s' _ none hsl
  unfold PkSame at h2 ⊢
  rw [h2]

/-- `process_attester_slashing` keeps the pubkeys. -/
theorem process_attester_slashing_pkSame (p : Preset) (o : Oracle) (s s' : BeaconState)
    (as : AttesterSlashing) (h : process_attester_slashing p o s as = .ok s') : PkSame s s' := by
  obtain ⟨heff, hf⟩ := process_attester_slashing_ok p o s s' as h
  refine .of_pointwise heff.2.1 fun j v v' hv hv' => ?_
  rcases hf j v v' hv hv' with rfl | ⟨-, hch⟩
  · rfl
  · exact hch.keeps.1

/-- `process_voluntary_exit` keeps the pubkeys. -/
theorem process_voluntary_exit_pkSame (p : Preset) (o : Oracle) (s s' : BeaconState)
    (e : SignedVoluntaryExit) (h : process_voluntary_exit p o s e = .ok s') : PkSame s s' := by
  obtain ⟨tab, -, -, -, -, -, -, hi⟩ := process_voluntary_exit_initiate p o s s' e h
  exact initiate_validator_exit_pkSame hi

/-- `process_bls_to_execution_change` keeps the pubkeys. -/
theorem process_bls_to_execution_change_pkSame (p : Preset) (o : Oracle) (s s' : BeaconState)
    (c : SignedBLSToExecutionChange) (h : process_bls_to_execution_change p o s c = .ok s') :
    PkSame s s' := by
  obtain ⟨-, -, -, -, hlen, hpt⟩ := process_bls_to_execution_change_effects p o s s' c h
  refine .of_pointwise hlen fun j u u' hu hu' => ?_
  obtain ⟨wc, h'⟩ := hpt j u hu
  rw [hu'] at h'
  cases h'
  rfl

/-- `process_withdrawal_request` keeps the pubkeys. -/
theorem process_withdrawal_request_pkSame (p : Preset) (s s' : BeaconState)
    (withdrawal_request : WithdrawalRequest)
    (h : process_withdrawal_request p s withdrawal_request = .ok s') : PkSame s s' := by
  unfold process_withdrawal_request at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h; exact PkSame.refl _)
    | exact initiate_validator_exit_pkSame ‹initiate_validator_exit _ _ _ _ = .ok _›
    | (cases h; exact initiate_validator_exit_pkSame ‹initiate_validator_exit _ _ _ _ = .ok _›)
    | (cases h
       obtain ⟨-, -, -, hv, -, -⟩ := compute_exit_epoch_and_update_churn_bound p _ s _ _ _
         ‹compute_exit_epoch_and_update_churn _ _ _ _ = .ok _›
       exact PkSame.of_eq hv)

/-- `switch_to_compounding_validator` keeps the pubkeys. -/
theorem switch_to_compounding_validator_pkSame {p : Preset} {s s' : BeaconState} {index : Nat}
    (h : switch_to_compounding_validator p s index = .ok s') : PkSame s s' := by
  unfold switch_to_compounding_validator at h
  obtain ⟨v, hv, h⟩ := specM_bind_ok h
  obtain ⟨l, hl, h⟩ := specM_bind_ok h
  obtain ⟨hvs, -, -⟩ := queue_excess_active_balance_cases p _ s' index h
  unfold PkSame
  rw [hvs, (ExitsSanity.listSet_ok hl).2]
  exact map_pubkey_set (listGet_ok hv).2 rfl

/-- A successful `if` ran one of its branches. -/
private theorem ite_ok_p {α : Type} {c : Prop} [Decidable c] {t e : SpecM α} {x : α}
    (h : (if c then t else e) = .ok x) : (c ∧ t = .ok x) ∨ (¬ c ∧ e = .ok x) := by
  by_cases hc : c
  · exact .inl ⟨hc, by rw [if_pos hc] at h; exact h⟩
  · exact .inr ⟨hc, by rw [if_neg hc] at h; exact h⟩

/-- Splits the binds and `if`s of a successful run without `simp`. -/
local macro "peel_ok" h:ident : tactic => `(tactic| repeat' first
  | (guard_hyp $h:ident : ((_ : SpecM _) >>= _) = _; obtain ⟨_, _, $h:ident⟩ := specM_bind_ok $h)
  | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _
     rcases ite_ok_p $h with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩))

/-- Splits the binds and `if`s of a successful run, and closes a `throw` step. -/
local macro "peel_ok'" h:ident : tactic => `(tactic| repeat' first
  | (guard_hyp $h:ident : ((_ : SpecM _) >>= _) = _
     obtain ⟨_, hx, $h:ident⟩ := specM_bind_ok $h
     try (guard_hyp hx : (throw _ : SpecM _) = _; cases hx))
  | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _;
     rcases ite_ok_p $h with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩))

/-- The last step of a consolidation keeps the pubkeys. -/
private theorem consolidation_exit_pkSame {p : Preset} {tab : Gwei} {s : BeaconState}
    {x i : Nat} {v v' : Validator} {r : Epoch × BeaconState} {l : List Validator}
    (hr : compute_consolidation_epoch_and_update_churn p tab s x = .ok r)
    (hl : listSet r.2.validators i v' = .ok l) (hv : listGet s.validators i = .ok v)
    (hv' : v'.pubkey = v.pubkey) (pc : List PendingConsolidation) :
    PkSame s { r.2 with validators := l, pending_consolidations := pc } := by
  obtain ⟨e, s1⟩ := r
  obtain ⟨-, -, -, hvs, -, -⟩ :=
    compute_consolidation_epoch_and_update_churn_bound p tab s s1 x e hr
  rw [hvs] at hl
  show l.map _ = _
  rw [(ExitsSanity.listSet_ok hl).2]
  exact map_pubkey_set (listGet_ok hv).2 hv'

/-- `process_consolidation_request` keeps the pubkeys. -/
theorem process_consolidation_request_pkSame (p : Preset) (s s' : BeaconState)
    (consolidation_request : ConsolidationRequest)
    (h : process_consolidation_request p s consolidation_request = .ok s') : PkSame s s' := by
  unfold process_consolidation_request at h
  peel_ok h
  all_goals first
    | (cases h; exact PkSame.refl _)
    | exact switch_to_compounding_validator_pkSame h
    | (cases h
       refine consolidation_exit_pkSame
         ‹compute_consolidation_epoch_and_update_churn _ _ _ _ = .ok _›
         ‹listSet _ _ _ = .ok _› ‹listGet _ _ = .ok _› ?_ _
       rfl)

/-- A request loop of `apply_parent_execution_payload` keeps the pubkeys. -/
private theorem request_loop_pkSame {α : Type} {l : List α}
    {body : α → BeaconState → SpecM (ForInStep BeaconState)} {s s' : BeaconState}
    (hloop : forIn l s body = .ok s')
    (hb : ∀ a st r, body a st = .ok r → PkSame st r.value) : PkSame s s' :=
  forIn_rel PkSame PkSame.refl (fun _ _ _ => PkSame.trans) l body s s' hloop hb

/-- Proves the body step of a request loop from the step of one request. -/
local macro "loop_body" e:term : tactic => `(tactic| (
  intro _ _ _ hbody
  obtain ⟨_, h1, hbody⟩ := specM_bind_ok hbody
  obtain ⟨_, -, hbody⟩ := specM_bind_ok hbody
  cases hbody
  exact $e h1))

/-- `apply_parent_execution_payload` keeps the pubkeys. -/
theorem apply_parent_execution_payload_pkSame (p : Preset) (o : Oracle) (s s' : BeaconState)
    (requests : ExecutionRequests)
    (h : apply_parent_execution_payload p o s requests = .ok s') : PkSame s s' := by
  unfold apply_parent_execution_payload at h
  peel_ok' h
  all_goals first
    | (cases h
       refine (request_loop_pkSame (by assumption : forIn requests.deposits _ _ = Except.ok _)
         (by loop_body (fun h => PkSame.of_eq (by cases h; rfl)))).trans
         ((request_loop_pkSame (by assumption : forIn requests.withdrawals _ _ = Except.ok _)
         (by loop_body process_withdrawal_request_pkSame _ _ _ _)).trans
         ((request_loop_pkSame (by assumption : forIn requests.consolidations _ _ = Except.ok _)
         (by loop_body process_consolidation_request_pkSame _ _ _ _)).trans
         ((request_loop_pkSame (by assumption : forIn requests.builder_deposits _ _ = Except.ok _)
         (by loop_body (fun h =>
           PkSame.of_eq (process_builder_deposit_request_frame _ _ _ _ _ h).1.1))).trans
         ((request_loop_pkSame (by assumption : forIn requests.builder_exits _ _ = Except.ok _)
         (by loop_body (fun h =>
           PkSame.of_eq (process_builder_exit_request_frame _ _ _ _ h).1.1))).trans
         ?_))))
       first
         | exact congrArg (List.map (·.pubkey)) (settle_builder_payment_frame _ _ _
             ‹settle_builder_payment _ _ = Except.ok _›).1.1
         | exact rfl)

/-- `process_parent_execution_payload` keeps the pubkeys. -/
theorem process_parent_execution_payload_pkSame (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_parent_execution_payload p o s block = .ok s') :
    PkSame s s' := by
  unfold process_parent_execution_payload at h
  peel_ok' h
  all_goals first
    | (cases h; exact PkSame.refl _)
    | exact apply_parent_execution_payload_pkSame p o _ _ _ h

/-- `process_operations` keeps the pubkeys. -/
theorem process_operations_pkSame (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (s s' : BeaconState) (body : BeaconBlockBody) (parent_slot : Slot)
    (h : process_operations p o GLOAS_FORK_EPOCH s body parent_slot = .ok s') : PkSame s s' := by
  have fold : ∀ {α : Type} (f : BeaconState → α → SpecM BeaconState),
      (∀ st a st', f st a = .ok st' → PkSame st st') →
      ∀ (l : List α) (st st' : BeaconState), l.foldlM f st = .ok st' → PkSame st st' :=
    fun f hf => foldlM_rel PkSame PkSame.refl (fun _ _ _ => PkSame.trans) f hf
  unfold process_operations at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  repeat' split at h
  all_goals first | (cases h; done) | skip
  rename_i _ s1 h1 _ s2 h2 _ s3 h3 _ s4 h4 _ s5 h5
  exact (fold _ (fun st a st' hst => process_proposer_slashing_pkSame p o st st' a hst)
    _ _ _ h1).trans
    ((fold _ (fun st a st' hst => process_attester_slashing_pkSame p o st st' a hst)
    _ _ _ h2).trans
    ((fold _ (fun st a st' hst => PkSame.of_eq
      (process_attestation_validators p o st st' a parent_slot hst)) _ _ _ h3).trans
    ((fold _ (fun st a st' hst => process_voluntary_exit_pkSame p o st st' a hst)
    _ _ _ h4).trans
    ((fold _ (fun st a st' hst => process_bls_to_execution_change_pkSame p o st st' a hst)
    _ _ _ h5).trans
    (fold _ (fun st a st' hst => PkSame.of_eq
      (by rw [process_payload_attestation_eq p o GLOAS_FORK_EPOCH st st' a hst]))
      _ _ _ h)))))

/-- `process_block` keeps the pubkeys. -/
theorem process_block_pkSame (p : Preset) (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState) (block : BeaconBlock)
    (h : process_block p o max_blobs_per_block GLOAS_FORK_EPOCH s block = .ok s') :
    PkSame s s' := by
  unfold process_block at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  obtain ⟨s2, h2, h⟩ := specM_bind_ok h
  obtain ⟨s3, h3, h⟩ := specM_bind_ok h
  obtain ⟨s4, h4, h⟩ := specM_bind_ok h
  obtain ⟨s5, h5, h⟩ := specM_bind_ok h
  obtain ⟨s6, h6, h⟩ := specM_bind_ok h
  obtain ⟨s7, h7, h⟩ := specM_bind_ok h
  obtain ⟨hfr, -⟩ := process_sync_aggregate_frame p o _ _ _ h
  exact (process_parent_execution_payload_pkSame p o _ _ block h1).trans
    ((PkSame.of_eq (process_block_header_frame p o _ _ block h2).1.1).trans
    ((PkSame.of_eq (process_withdrawals_core p _ _ h3).1).trans
    ((PkSame.of_eq (process_execution_payload_bid_frame p o _ _ _ _ h4).1.1).trans
    ((PkSame.of_eq (process_randao_frame p o _ _ _ h5).1.1).trans
    ((PkSame.of_eq (process_eth1_data_frame p _ _ _ h6).1.1).trans
    ((process_operations_pkSame p o GLOAS_FORK_EPOCH _ _ _ _ h7).trans
    (PkSame.of_eq (by rw [hfr]))))))))

/-! ## Epoch steps -/

/-- `process_registry_updates` keeps the pubkeys. -/
theorem process_registry_updates_pkSame (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_registry_updates p total_active_balance s = .ok s') :
    PkSame s s' := by
  unfold process_registry_updates at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨r, hr, h⟩ := specM_bind_ok h
  cases h
  refine SlashingsSanity.forIn_invariant (PkSame s) _ ?_ _ s _ (PkSame.refl s) hr
  intro index st r hst hb
  simp only [bind, Except.bind, pure, Except.pure] at hb
  repeat' split at hb
  all_goals first
    | contradiction
    | (cases hb; exact hst)
    | (cases hb
       exact hst.trans (initiate_validator_exit_pkSame ‹initiate_validator_exit _ _ _ _ = _›))
    | (cases hb
       refine hst.trans ?_
       obtain ⟨-, hl⟩ := ExitsSanity.listSet_ok ‹listSet _ _ _ = _›
       obtain ⟨-, hg⟩ := listGet_ok ‹listGet _ _ = _›
       show _ = _
       rw [hl]
       exact map_pubkey_set hg rfl)

/-- `process_effective_balance_updates` keeps the pubkeys. It writes only effective balances. -/
theorem process_effective_balance_updates_pkSame (p : Preset) (s s' : BeaconState)
    (h : process_effective_balance_updates p s = .ok s') : PkSame s s' := by
  rw [process_effective_balance_updates_eq] at h
  cases hm : s.validators.zipIdx.mapM (effectiveBalanceStep p s.balances) with
  | error e => simp [hm, Except.map] at h
  | ok validators =>
    simp only [hm, Except.map, Except.ok.injEq] at h
    subst h
    obtain ⟨hlen, hget⟩ := mapM_ok_getElem _ _ _ hm
    rw [List.length_zipIdx] at hlen
    refine .of_pointwise hlen fun i v v' hv hv' => ?_
    have hi : i < validators.length := (List.getElem?_eq_some_iff.mp hv').1
    have hiv : i < s.validators.length := hlen ▸ hi
    have hs' := hget i (by simpa using hiv) hi
    rw [List.getElem_zipIdx] at hs'
    rw [List.getElem?_eq_getElem hiv] at hv
    change validators[i]? = some v' at hv'
    rw [List.getElem?_eq_getElem hi] at hv'
    cases hv
    cases hv'
    generalize validators[i] = w at hs' ⊢
    unfold effectiveBalanceStep at hs'
    simp only [bind, Except.bind, pure, Except.pure] at hs'
    repeat' split at hs'
    all_goals first
      | contradiction
      | (cases hs'; rfl)

/-- A validator from a deposit has the deposit pubkey. -/
theorem get_validator_from_deposit_pubkey (p : Preset) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei) (v : Validator)
    (h : get_validator_from_deposit p pubkey withdrawal_credentials amount = .ok v) :
    v.pubkey = pubkey := by
  unfold get_validator_from_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; rfl)

/-- `apply_pending_deposit` keeps `PubkeysUnique`. It adds a validator only for a pubkey that is
not in the list. -/
theorem apply_pending_deposit_pubkeysUnique (p : Preset) (o : Oracle) (s s' : BeaconState)
    (deposit : PendingDeposit) (h : apply_pending_deposit p o s deposit = .ok s')
    (hs : PubkeysUnique s) : PubkeysUnique s' := by
  have hadd : ∀ (wc : Bytes32) (amount : Gwei) (t : BeaconState),
      deposit.pubkey ∉ s.validators.map (·.pubkey) →
      add_validator_to_registry p s deposit.pubkey wc amount = .ok t → PubkeysUnique t := by
    intro wc amount t hnot ht
    unfold add_validator_to_registry at ht
    obtain ⟨v, hv, ht⟩ := specM_bind_ok ht
    obtain ⟨l, hl, ht⟩ := specM_bind_ok ht
    simp only [bind, Except.bind, pure, Except.pure] at ht
    repeat' split at ht
    all_goals first | contradiction | skip
    cases ht
    unfold set_or_append_list get_index_for_new_validator at hl
    simp only [beq_self_eq_true, if_true, pure, Except.pure, Except.ok.injEq] at hl
    subst hl
    unfold PubkeysUnique
    simp only [List.map_append, List.map_cons, List.map_nil]
    rw [get_validator_from_deposit_pubkey p _ _ _ v hv]
    exact List.nodup_append.mpr ⟨hs, by simp, fun a ha b hb => by
      rw [List.mem_singleton.mp hb]
      rintro rfl
      exact hnot ha⟩
  unfold apply_pending_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact hs)
    | exact hadd _ _ _ ‹_› h

/-- `process_pending_deposits` with `apply_pending_deposit` keeps `PubkeysUnique`. -/
theorem process_pending_deposits_pubkeysUnique (p : Preset) (o : Oracle)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_pending_deposits p total_active_balance (apply_pending_deposit p o) s = .ok s')
    (hs : PubkeysUnique s) : PubkeysUnique s' := by
  unfold process_pending_deposits at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, _, _, _, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := SlashingsSanity.forIn_invariant
    (fun r : MProd (List PendingDeposit) (MProd Bool (MProd Uint64 (MProd Uint64 BeaconState))) =>
      PubkeysUnique r.2.2.2.2) _ ?_ _ _ _ hs hr
  · simp only [bind, Except.bind, pure, Except.pure] at h
    repeat' split at h
    all_goals first
      | contradiction
      | (cases h; exact hP)
  · intro _ ⟨_, _, _, _, _⟩ r hst hb
    simp only [bind, Except.bind, pure, Except.pure] at hb
    repeat' split at hb
    all_goals first
      | contradiction
      | (cases hb; exact hst)
      | (cases hb
         exact apply_pending_deposit_pubkeysUnique p o _ _ _
           ‹apply_pending_deposit _ _ _ _ = _› hst)

/-- `process_epoch` keeps `PubkeysUnique`. -/
theorem process_epoch_pubkeysUnique (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_epoch p o s = .ok s') (hs : PubkeysUnique s) : PubkeysUnique s' := by
  unfold process_epoch at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  obtain ⟨s2, h2, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨s3, h3, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨s4, h4, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨s5, h5, h⟩ := specM_bind_ok h
  obtain ⟨s6, h6, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨s7, h7, h⟩ := specM_bind_ok h
  obtain ⟨s8, h8, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨s9, h9, h⟩ := specM_bind_ok h
  obtain ⟨s10, h10, h⟩ := specM_bind_ok h
  obtain ⟨s11, h11, h⟩ := specM_bind_ok h
  obtain ⟨s12, h12, h⟩ := specM_bind_ok h
  obtain ⟨s13, h13, h⟩ := specM_bind_ok h
  obtain ⟨s14, h14, h⟩ := specM_bind_ok h
  obtain ⟨s15, h15, h⟩ := specM_bind_ok h
  have d6 : PubkeysUnique s6 :=
    ((PkSame.of_eq (process_justification_and_finalization_frame p s s1 h1).1.1).trans
    ((PkSame.of_eq (process_inactivity_updates_validators p _ _ h2)).trans
    ((PkSame.of_eq (process_rewards_and_penalties_validators p _ _ _ h3)).trans
    ((process_registry_updates_pkSame p _ _ _ h4).trans
    ((PkSame.of_eq (process_slashings_validators p _ _ _ h5)).trans
    (PkSame.of_eq (process_eth1_data_reset_frame p _ _ h6).1.1)))))).pubkeysUnique hs
  have d7 := process_pending_deposits_pubkeysUnique p o _ _ _ h7 d6
  exact ((PkSame.of_eq (process_pending_consolidations_validators p _ _ h8)).trans
    ((PkSame.of_eq (process_builder_pending_payments_validators p _ _ _ h9)).trans
    ((process_effective_balance_updates_pkSame p _ _ h10).trans
    ((PkSame.of_eq (process_slashings_reset_frame p _ _ h11).1.1).trans
    ((PkSame.of_eq (process_randao_mixes_reset_frame p _ _ h12).1.1).trans
    ((PkSame.of_eq (process_historical_summaries_update_frame p o _ _ h13).1.1).trans
    ((PkSame.of_eq (process_sync_committee_updates_frame p o _ _ h14).1.1).trans
    ((PkSame.of_eq (process_proposer_lookahead_frame p o _ _ h15).1.1).trans
    (PkSame.of_eq (process_ptc_window_frame p o _ _ h).1.1))))))))).pubkeysUnique d7

/-- `process_slots` with the real `process_epoch` keeps `PubkeysUnique`. -/
theorem process_slots_pubkeysUnique (p : Preset) (o : Oracle) (s s' : BeaconState) (slot : Slot)
    (h : process_slots p o (process_epoch p o) s slot = .ok s') (hs : PubkeysUnique s) :
    PubkeysUnique s' :=
  process_slots_invariant p o (process_epoch p o) PubkeysUnique
    (fun _ _ ht hP => (PkSame.of_eq (process_slot_frame p o _ _ ht).1.1).pubkeysUnique hP)
    (fun _ _ ht hP => process_epoch_pubkeysUnique p o _ _ ht hP)
    (fun _ _ hP => hP) s s' slot hs h

/-- `state_transition` keeps `PubkeysUnique`. -/
theorem state_transition_pubkeysUnique (p : Preset) (o : Oracle)
    (max_blobs_per_block : Epoch → Uint64) (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState)
    (signed_block : SignedBeaconBlock) (validate_result : Bool) (hs : PubkeysUnique s)
    (h : state_transition p o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
      validate_result = .ok s') :
    PubkeysUnique s' := by
  unfold state_transition at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h
       exact (process_block_pkSame p o max_blobs_per_block GLOAS_FORK_EPOCH _ _ _
         ‹process_block _ _ _ _ _ _ = _›).pubkeysUnique
         (process_slots_pubkeysUnique p o _ _ _ ‹process_slots _ _ _ _ _ = _› hs))

end EpochProofs.Spec
