import EpochProofs.Sanity.Invariants.PubkeysUnique
import EpochProofs.Sanity.Invariants.Lengths

/-!
# Pending consolidations point at existing validators

`ConsolidationsInRange` says that the source and target index of each pending consolidation
are below the number of validators. A consolidation request takes both indices from the pubkey
list, and only for pubkeys in it. `process_pending_consolidations` drops a prefix, and pending
deposits only append validators. Every other step keeps the queue and the number of validators
(`CStep`).
-/

namespace EpochProofs.Spec

/-- Each pending consolidation has its source and target index below the number of
validators. -/
def ConsolidationsInRange (s : BeaconState) : Prop :=
  ∀ c ∈ s.pending_consolidations,
    c.source_index < s.validators.length ∧ c.target_index < s.validators.length

/-- The queue of pending consolidations and the number of validators stay. -/
def CStep (s s' : BeaconState) : Prop :=
  s'.pending_consolidations = s.pending_consolidations ∧
    s'.validators.length = s.validators.length

/-- A state takes a `CStep` to itself. -/
theorem CStep.refl (s : BeaconState) : CStep s s := ⟨rfl, rfl⟩

/-- `CStep` composes. -/
theorem CStep.trans {s1 s2 s3 : BeaconState} (h12 : CStep s1 s2) (h23 : CStep s2 s3) :
    CStep s1 s3 :=
  ⟨h23.1.trans h12.1, h23.2.trans h12.2⟩

/-- `CStep` keeps `ConsolidationsInRange`. -/
theorem CStep.inRange {s s' : BeaconState} (h : CStep s s') (hs : ConsolidationsInRange s) :
    ConsolidationsInRange s' := by
  intro c hc
  rw [h.1] at hc
  rw [h.2]
  exact hs c hc

/-- A successful `if` ran one of its branches. -/
private theorem ite_ok_c {α : Type} {c : Prop} [Decidable c] {t e : SpecM α} {x : α}
    (h : (if c then t else e) = .ok x) : (c ∧ t = .ok x) ∨ (¬ c ∧ e = .ok x) := by
  by_cases hc : c
  · exact .inl ⟨hc, by rw [if_pos hc] at h; exact h⟩
  · exact .inr ⟨hc, by rw [if_neg hc] at h; exact h⟩

/-- Unfolds the list helpers of a step, then splits its binds, `if`s and `match`es. Each error
branch closes. -/
local macro "c_split" h:ident : tactic => `(tactic| (
  try simp only [increase_balance, decrease_balance, bind_assoc, listSet_bind] at $h:ident
  try simp only [throw, throwThe, MonadExceptOf.throw] at $h:ident
  try simp only [bind, Except.bind, pure, Except.pure] at $h:ident
  repeat' first
    | split at $h:ident
    | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _
       rcases ite_ok_c $h:ident with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩)
  all_goals try contradiction))

/-- Splits the binds and `if`s of a successful run without `simp`. -/
local macro "peel_ok" h:ident : tactic => `(tactic| repeat' first
  | (guard_hyp $h:ident : ((_ : SpecM _) >>= _) = _
     obtain ⟨_, hx, $h:ident⟩ := specM_bind_ok $h
     try (guard_hyp hx : (throw _ : SpecM _) = _; cases hx))
  | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _;
     rcases ite_ok_c $h with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩))

/-- Closes `CStep s s'` from the `CStep` facts in the context. -/
local macro "c_close" : tactic => `(tactic| (
  first
    | exact ⟨rfl, rfl⟩
    | exact ⟨rfl, List.length_set⟩
    | exact ‹CStep _ _›
    | exact ⟨(‹CStep _ _›).1, (‹CStep _ _›).2⟩
    | exact ⟨(‹CStep _ _›).1, List.length_set.trans (‹CStep _ _›).2⟩))

/-- A `for` loop in `SpecM` keeps every property that each successful pass keeps. -/
private theorem forIn_ok_invariant_c {α β : Type} (P : β → Prop)
    (f : α → β → SpecM (ForInStep β)) (hf : ∀ a b r, P b → f a b = .ok r → P r.value) :
    ∀ (l : List α) (b b' : β), P b → forIn l b f = .ok b' → P b' := by
  intro l
  induction l with
  | nil => intro b b' hb h; cases h; exact hb
  | cons a as ih =>
    intro b b' hb h
    rw [List.forIn_cons] at h
    obtain ⟨r, hr, h⟩ := specM_bind_ok h
    have := hf a b r hb hr
    cases r with
    | done c => cases h; exact this
    | yield c => exact ih c b' this h

/-- A `for` loop over the state keeps `CStep` when each pass keeps it. -/
private theorem forIn_cstep {α : Type} {f : α → BeaconState → SpecM (ForInStep BeaconState)}
    {l : List α} {s s' : BeaconState} (h : forIn l s f = .ok s')
    (hf : ∀ a st r, f a st = .ok r → CStep st r.value) : CStep s s' :=
  forIn_ok_invariant_c (CStep s) f (fun a b r hb hr => hb.trans (hf a b r hr)) l s s'
    (CStep.refl s) h

/-- A `List.foldlM` over the state keeps a reflexive and transitive relation. -/
private theorem foldlM_rel_c {α : Type} (R : BeaconState → BeaconState → Prop)
    (hrefl : ∀ s, R s s) (htrans : ∀ s1 s2 s3, R s1 s2 → R s2 s3 → R s1 s3)
    (f : BeaconState → α → SpecM BeaconState) (hf : ∀ s a s', f s a = .ok s' → R s s') :
    ∀ (l : List α) (s s' : BeaconState), l.foldlM f s = .ok s' → R s s' := by
  intro l
  induction l with
  | nil => intro s s' h; cases h; exact hrefl s
  | cons a as ih =>
    intro s s' h
    rw [List.foldlM_cons] at h
    obtain ⟨s1, h1, h⟩ := specM_bind_ok h
    exact htrans _ _ _ (hf s a s1 h1) (ih s1 s' h)

/-! ## Block operations -/

/-- The state from `compute_exit_epoch_and_update_churn` keeps the queue and the registry. -/
theorem compute_exit_epoch_and_update_churn_cstep (p : Preset) (total_active_balance : Gwei)
    (s : BeaconState) (exit_balance : Gwei) (r : Epoch × BeaconState)
    (h : compute_exit_epoch_and_update_churn p total_active_balance s exit_balance = .ok r) :
    CStep s r.2 := by
  unfold compute_exit_epoch_and_update_churn at h
  c_split h
  all_goals (cases h; exact CStep.refl _)

/-- `initiate_validator_exit` keeps the queue and the registry size. -/
theorem initiate_validator_exit_cstep (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (index : ValidatorIndex)
    (h : initiate_validator_exit p total_active_balance s index = .ok s') : CStep s s' := by
  obtain ⟨⟨vs, x, y, rfl⟩, hlen, -⟩ :=
    initiate_validator_exit_ok p total_active_balance s s' index h
  exact ⟨rfl, hlen⟩

/-- `slash_validator` keeps the queue and the registry size. -/
theorem slash_validator_cstep (p : Preset) (s s' : BeaconState) (index : ValidatorIndex)
    (whistleblower_index : Option ValidatorIndex)
    (h : slash_validator p s index whistleblower_index = .ok s') : CStep s s' := by
  have hlen := (slash_validator_ok p s s' index whistleblower_index h).2.1
  unfold slash_validator at h
  obtain ⟨epoch, -, h⟩ := specM_bind_ok h
  obtain ⟨v0, hv0, h⟩ := specM_bind_ok h
  obtain ⟨s1, hs1, h⟩ := SlashingsSanity.bind_if_ok h
  obtain ⟨tab, hs1⟩ := slash_validator_initiate_ok p s s1 index v0 hv0 hs1
  have c1 := initiate_validator_exit_cstep p tab s s1 index hs1
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨c1.1, hlen⟩)

/-- `process_proposer_slashing` keeps the queue and the registry size. -/
theorem process_proposer_slashing_cstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (ps : ProposerSlashing) (h : process_proposer_slashing p o s ps = .ok s') : CStep s s' := by
  obtain ⟨_, _, pay, _, _, _, hsl⟩ := process_proposer_slashing_ok p o s s' ps h
  have h2 := slash_validator_cstep p { s with builder_pending_payments := pay } s' _ none hsl
  exact ⟨h2.1, h2.2⟩

/-- `process_attester_slashing` keeps the queue and the registry size. -/
theorem process_attester_slashing_cstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (as : AttesterSlashing) (h : process_attester_slashing p o s as = .ok s') : CStep s s' := by
  unfold process_attester_slashing at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  repeat' split at h
  all_goals first | (cases h; done) | skip
  cases h
  refine forIn_ok_invariant_c (fun r : MProd Bool BeaconState => CStep s r.snd) _ ?_ _
    (⟨false, s⟩ : MProd Bool BeaconState) _ (CStep.refl s) ‹forIn _ _ _ = _›
  intro index r step hr hstep
  obtain ⟨_, -, hstep⟩ := specM_bind_ok hstep
  obtain ⟨_, -, hstep⟩ := specM_bind_ok hstep
  split at hstep
  · obtain ⟨r', hr', hstep⟩ := specM_bind_ok hstep
    cases hstep
    exact hr.trans (slash_validator_cstep p _ _ index none hr')
  · cases hstep
    exact hr

/-- `process_attestation` keeps the queue and the registry. -/
theorem process_attestation_cstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') : CStep s s' := by
  obtain ⟨_, _, _, -, -, hs | hs⟩ := process_attestation_shape p o s s' attestation parent_slot h
  all_goals (subst hs; exact ⟨rfl, rfl⟩)

/-- `process_voluntary_exit` keeps the queue and the registry size. -/
theorem process_voluntary_exit_cstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (e : SignedVoluntaryExit) (h : process_voluntary_exit p o s e = .ok s') : CStep s s' := by
  obtain ⟨tab, -, -, -, -, -, -, hi⟩ := process_voluntary_exit_initiate p o s s' e h
  exact initiate_validator_exit_cstep p tab s s' _ hi

/-- `process_bls_to_execution_change` keeps the queue and the registry size. -/
theorem process_bls_to_execution_change_cstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (c : SignedBLSToExecutionChange) (h : process_bls_to_execution_change p o s c = .ok s') :
    CStep s s' := by
  obtain ⟨v, wc, -, -, -, rfl⟩ := process_bls_to_execution_change_ok p o s s' c h
  exact ⟨rfl, List.length_set⟩

/-- `process_payload_attestation` keeps the state. -/
theorem process_payload_attestation_cstep (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (s s' : BeaconState) (payload_attestation : PayloadAttestation)
    (h : process_payload_attestation p o GLOAS_FORK_EPOCH s payload_attestation = .ok s') :
    CStep s s' := by
  rw [process_payload_attestation_eq p o GLOAS_FORK_EPOCH s s' payload_attestation h]
  exact CStep.refl s

/-- `process_operations` keeps the queue and the registry size. -/
theorem process_operations_cstep (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (s s' : BeaconState) (body : BeaconBlockBody) (parent_slot : Slot)
    (h : process_operations p o GLOAS_FORK_EPOCH s body parent_slot = .ok s') : CStep s s' := by
  have fold : ∀ {α : Type} (f : BeaconState → α → SpecM BeaconState),
      (∀ st a st', f st a = .ok st' → CStep st st') →
      ∀ (l : List α) (st st' : BeaconState), l.foldlM f st = .ok st' → CStep st st' :=
    fun f hf => foldlM_rel_c CStep CStep.refl (fun _ _ _ => CStep.trans) f hf
  unfold process_operations at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  repeat' split at h
  all_goals first | (cases h; done) | skip
  rename_i _ s1 h1 _ s2 h2 _ s3 h3 _ s4 h4 _ s5 h5
  exact (fold _ (fun st a st' hst => process_proposer_slashing_cstep p o st st' a hst)
    _ _ _ h1).trans
    ((fold _ (fun st a st' hst => process_attester_slashing_cstep p o st st' a hst)
    _ _ _ h2).trans
    ((fold _ (fun st a st' hst => process_attestation_cstep p o st st' a parent_slot hst)
    _ _ _ h3).trans
    ((fold _ (fun st a st' hst => process_voluntary_exit_cstep p o st st' a hst)
    _ _ _ h4).trans
    ((fold _ (fun st a st' hst => process_bls_to_execution_change_cstep p o st st' a hst)
    _ _ _ h5).trans
    (fold _ (fun st a st' hst =>
      process_payload_attestation_cstep p o GLOAS_FORK_EPOCH st st' a hst) _ _ _ h)))))

/-- `process_block_header` keeps the queue and the registry. -/
theorem process_block_header_cstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_block_header p o s block = .ok s') : CStep s s' := by
  unfold process_block_header at h
  c_split h
  all_goals (cases h; c_close)

/-- `process_randao` keeps the queue and the registry. -/
theorem process_randao_cstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (body : BeaconBlockBody) (h : process_randao p o s body = .ok s') : CStep s s' := by
  unfold process_randao at h
  c_split h
  all_goals (cases h; c_close)

/-- `process_eth1_data` keeps the queue and the registry. -/
theorem process_eth1_data_cstep (p : Preset) (s s' : BeaconState) (body : BeaconBlockBody)
    (h : process_eth1_data p s body = .ok s') : CStep s s' := by
  unfold process_eth1_data at h
  c_split h
  all_goals (cases h; c_close)

/-- `process_execution_payload_bid` keeps the queue and the registry. -/
theorem process_execution_payload_bid_cstep (p : Preset) (o : Oracle)
    (max_blobs_per_block : Epoch → Uint64) (s s' : BeaconState)
    (signed_bid : SignedExecutionPayloadBid)
    (h : process_execution_payload_bid p o max_blobs_per_block s signed_bid = .ok s') :
    CStep s s' := by
  unfold process_execution_payload_bid at h
  peel_ok h
  all_goals (cases h; c_close)

/-- `apply_withdrawals` keeps the queue and the registry. -/
theorem apply_withdrawals_cstep (withdrawals : List Withdrawal) (s s' : BeaconState)
    (h : apply_withdrawals s withdrawals = .ok s') : CStep s s' := by
  obtain ⟨⟨balances, builders, rfl⟩, -⟩ := apply_withdrawals_ok withdrawals s s' h
  exact CStep.refl _

/-- `update_next_withdrawal_index` keeps the queue and the registry. -/
theorem update_next_withdrawal_index_cstep (s s' : BeaconState) (withdrawals : List Withdrawal)
    (h : update_next_withdrawal_index s withdrawals = .ok s') : CStep s s' := by
  unfold update_next_withdrawal_index at h
  c_split h
  all_goals (cases h; c_close)

/-- `update_next_withdrawal_builder_index` keeps the queue and the registry. -/
theorem update_next_withdrawal_builder_index_cstep (s s' : BeaconState) (count : Uint64)
    (h : update_next_withdrawal_builder_index s count = .ok s') : CStep s s' := by
  unfold update_next_withdrawal_builder_index at h
  c_split h
  all_goals (cases h; c_close)

/-- `update_next_withdrawal_validator_index` keeps the queue and the registry. -/
theorem update_next_withdrawal_validator_index_cstep (p : Preset) (s s' : BeaconState)
    (withdrawals : List Withdrawal)
    (h : update_next_withdrawal_validator_index p s withdrawals = .ok s') : CStep s s' := by
  unfold update_next_withdrawal_validator_index at h
  c_split h
  all_goals (cases h; c_close)

/-- `process_withdrawals` keeps the queue and the registry. -/
theorem process_withdrawals_cstep (p : Preset) (s s' : BeaconState)
    (h : process_withdrawals p s = .ok s') : CStep s s' := by
  unfold process_withdrawals at h
  split at h
  · cases h
    exact CStep.refl _
  · obtain ⟨_, -, h⟩ := specM_bind_ok h
    obtain ⟨s1, h1, h⟩ := specM_bind_ok h
    obtain ⟨s2, h2, h⟩ := specM_bind_ok h
    obtain ⟨s3, h3, h⟩ := specM_bind_ok h
    obtain ⟨s4, h4, h⟩ := specM_bind_ok h
    have c1 := apply_withdrawals_cstep _ _ _ h2
    have c2 := update_next_withdrawal_index_cstep _ _ _ h3
    have c3 := update_next_withdrawal_builder_index_cstep _ _ _ h4
    have c4 := update_next_withdrawal_validator_index_cstep p _ _ _ h
    exact c1.trans (c2.trans (CStep.trans ⟨rfl, rfl⟩ (c3.trans c4)))

/-- `process_sync_aggregate` keeps the queue and the registry. -/
theorem process_sync_aggregate_cstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (sync_aggregate : SyncAggregate)
    (h : process_sync_aggregate p o s sync_aggregate = .ok s') : CStep s s' := by
  obtain ⟨hfr, -⟩ := process_sync_aggregate_frame p o s s' sync_aggregate h
  rw [hfr]
  exact ⟨rfl, rfl⟩

/-! ## Execution requests -/

/-- A request step keeps `ConsolidationsInRange` and the registry size. -/
def RStep (s s' : BeaconState) : Prop :=
  (ConsolidationsInRange s → ConsolidationsInRange s') ∧
    s'.validators.length = s.validators.length

/-- A state takes a request step to itself. -/
theorem RStep.refl (s : BeaconState) : RStep s s := ⟨id, rfl⟩

/-- `RStep` composes. -/
theorem RStep.trans {s1 s2 s3 : BeaconState} (h12 : RStep s1 s2) (h23 : RStep s2 s3) :
    RStep s1 s3 :=
  ⟨fun h => h23.1 (h12.1 h), h23.2.trans h12.2⟩

/-- A `CStep` is a request step. -/
theorem CStep.rstep {s s' : BeaconState} (h : CStep s s') : RStep s s' := ⟨h.inRange, h.2⟩

/-- `process_withdrawal_request` keeps the queue and the registry size. -/
theorem process_withdrawal_request_cstep (p : Preset) (s s' : BeaconState)
    (withdrawal_request : WithdrawalRequest)
    (h : process_withdrawal_request p s withdrawal_request = .ok s') : CStep s s' := by
  unfold process_withdrawal_request at h
  c_split h
  all_goals
    try (cases h)
    try (have := initiate_validator_exit_cstep p _ _ _ _ ‹initiate_validator_exit _ _ _ _ = _›)
    try (have := compute_exit_epoch_and_update_churn_cstep p _ _ _ _
                   ‹compute_exit_epoch_and_update_churn _ _ _ _ = _›)
    c_close

/-- `switch_to_compounding_validator` keeps the queue and the registry size. -/
theorem switch_to_compounding_validator_cstep {p : Preset} {s s' : BeaconState} {index : Nat}
    (h : switch_to_compounding_validator p s index = .ok s') : CStep s s' := by
  have hlen := (switch_to_compounding_validator_pkSame h).length
  unfold switch_to_compounding_validator at h
  obtain ⟨v, hv, h⟩ := specM_bind_ok h
  obtain ⟨l, hl, h⟩ := specM_bind_ok h
  unfold queue_excess_active_balance at h
  c_split h
  all_goals (cases h; exact ⟨rfl, hlen⟩)

/-- The state from `compute_consolidation_epoch_and_update_churn` keeps the queue and the
registry. -/
theorem compute_consolidation_epoch_and_update_churn_cstep (p : Preset)
    (total_active_balance : Gwei) (s : BeaconState) (consolidation_balance : Gwei)
    (r : Epoch × BeaconState)
    (h : compute_consolidation_epoch_and_update_churn p total_active_balance s
      consolidation_balance = .ok r) : CStep s r.2 := by
  unfold compute_consolidation_epoch_and_update_churn at h
  c_split h
  all_goals (cases h; exact CStep.refl _)

/-- The last step of a consolidation appends a consolidation whose indices point at existing
validators. -/
private theorem consolidation_append_inRange {p : Preset} {tab : Gwei} {s : BeaconState}
    {x i : Nat} {v' : Validator} {r : Epoch × BeaconState} {l : List Validator}
    {src tgt : BLSPubkey}
    (hr : compute_consolidation_epoch_and_update_churn p tab s x = .ok r)
    (hl : listSet r.2.validators i v' = .ok l)
    (hsrc : src ∈ s.validators.map (·.pubkey)) (htgt : tgt ∈ s.validators.map (·.pubkey)) :
    RStep s { r.2 with
      validators := l
      pending_consolidations := r.2.pending_consolidations ++
        [{ source_index := (s.validators.map (·.pubkey)).idxOf src
           target_index := (s.validators.map (·.pubkey)).idxOf tgt }] } := by
  obtain ⟨e, s1⟩ := r
  obtain ⟨-, -, -, hvs, -, -⟩ :=
    compute_consolidation_epoch_and_update_churn_bound p tab s s1 x e hr
  have hc := (compute_consolidation_epoch_and_update_churn_cstep p tab s x _ hr).1
  rw [hvs] at hl
  have hlen : l.length = s.validators.length := by
    rw [(ExitsSanity.listSet_ok hl).2, List.length_set]
  refine ⟨fun hs c hc' => ?_, hlen⟩
  change c ∈ s1.pending_consolidations ++ _ at hc'
  change c.source_index < l.length ∧ c.target_index < l.length
  rw [hlen]
  rw [hc] at hc'
  rcases List.mem_append.mp hc' with hc' | hc'
  · exact hs c hc'
  · rw [List.mem_singleton.mp hc']
    have h1 := List.idxOf_lt_length_of_mem hsrc
    have h2 := List.idxOf_lt_length_of_mem htgt
    simp only [List.length_map] at h1 h2
    exact ⟨h1, h2⟩

/-- `process_consolidation_request` keeps `ConsolidationsInRange` and the registry size. A new
consolidation takes its indices from the pubkey list, for pubkeys in it. -/
theorem process_consolidation_request_rstep (p : Preset) (s s' : BeaconState)
    (consolidation_request : ConsolidationRequest)
    (h : process_consolidation_request p s consolidation_request = .ok s') : RStep s s' := by
  unfold process_consolidation_request at h
  peel_ok h
  all_goals first
    | (cases h; exact RStep.refl _)
    | exact (switch_to_compounding_validator_cstep h).rstep
    | (cases h
       refine consolidation_append_inRange
         ‹compute_consolidation_epoch_and_update_churn _ _ _ _ = .ok _›
         ‹listSet _ _ _ = .ok _› ?_ ?_ <;>
       exact Decidable.of_not_not ‹_›)

/-- `process_deposit_request` keeps the queue and the registry. -/
theorem process_deposit_request_cstep (s s' : BeaconState) (deposit_request : DepositRequest)
    (h : process_deposit_request s deposit_request = .ok s') : CStep s s' := by
  cases h
  exact ⟨rfl, rfl⟩

/-- `process_builder_deposit_request` keeps the queue and the registry. -/
theorem process_builder_deposit_request_cstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (request : BuilderDepositRequest)
    (h : process_builder_deposit_request p o s request = .ok s') : CStep s s' := by
  unfold process_builder_deposit_request at h
  unfold add_builder_to_registry at h
  c_split h
  all_goals (cases h; c_close)

/-- `process_builder_exit_request` keeps the queue and the registry. -/
theorem process_builder_exit_request_cstep (p : Preset) (s s' : BeaconState)
    (request : BuilderExitRequest)
    (h : process_builder_exit_request p s request = .ok s') : CStep s s' := by
  unfold process_builder_exit_request at h
  unfold initiate_builder_exit at h
  c_split h
  all_goals (cases h; c_close)

/-- `settle_builder_payment` keeps the queue and the registry. -/
theorem settle_builder_payment_cstep (s s' : BeaconState) (payment_index : Uint64)
    (h : settle_builder_payment s payment_index = .ok s') : CStep s s' := by
  unfold settle_builder_payment at h
  c_split h
  all_goals (cases h; c_close)

/-- A request loop of `apply_parent_execution_payload` is a request step. -/
private theorem request_loop_rstep {α : Type} {l : List α}
    {body : α → BeaconState → SpecM (ForInStep BeaconState)} {s s' : BeaconState}
    (hloop : forIn l s body = .ok s')
    (hb : ∀ a st r, body a st = .ok r → RStep st r.value) : RStep s s' :=
  forIn_rel RStep RStep.refl (fun _ _ _ => RStep.trans) l body s s' hloop hb

/-- Proves the body step of a request loop from the step of one request. -/
local macro "loop_body" e:term : tactic => `(tactic| (
  intro _ _ _ hbody
  obtain ⟨_, h1, hbody⟩ := specM_bind_ok hbody
  obtain ⟨_, -, hbody⟩ := specM_bind_ok hbody
  cases hbody
  exact $e h1))

/-- `apply_parent_execution_payload` is a request step. -/
theorem apply_parent_execution_payload_rstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (requests : ExecutionRequests)
    (h : apply_parent_execution_payload p o s requests = .ok s') : RStep s s' := by
  unfold apply_parent_execution_payload at h
  peel_ok h
  all_goals first
    | (cases h
       refine (request_loop_rstep (by assumption : forIn requests.deposits _ _ = Except.ok _)
         (by loop_body (fun h => (process_deposit_request_cstep _ _ _ h).rstep))).trans
         ((request_loop_rstep (by assumption : forIn requests.withdrawals _ _ = Except.ok _)
         (by loop_body (fun h => (process_withdrawal_request_cstep p _ _ _ h).rstep))).trans
         ((request_loop_rstep (by assumption : forIn requests.consolidations _ _ = Except.ok _)
         (by loop_body process_consolidation_request_rstep p _ _ _)).trans
         ((request_loop_rstep (by assumption : forIn requests.builder_deposits _ _ = Except.ok _)
         (by loop_body (fun h => (process_builder_deposit_request_cstep p o _ _ _ h).rstep))).trans
         ((request_loop_rstep (by assumption : forIn requests.builder_exits _ _ = Except.ok _)
         (by loop_body (fun h => (process_builder_exit_request_cstep p _ _ _ h).rstep))).trans
         ?_))))
       first
         | (have hset := settle_builder_payment_cstep _ _ _
             ‹settle_builder_payment _ _ = Except.ok _›
            exact CStep.rstep (hset.trans ⟨rfl, rfl⟩))
         | exact CStep.rstep ⟨rfl, rfl⟩)

/-- `process_parent_execution_payload` is a request step. -/
theorem process_parent_execution_payload_rstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_parent_execution_payload p o s block = .ok s') :
    RStep s s' := by
  unfold process_parent_execution_payload at h
  peel_ok h
  all_goals first
    | (cases h; exact RStep.refl _)
    | exact apply_parent_execution_payload_rstep p o _ _ _ h

/-- `process_block` keeps `ConsolidationsInRange`. -/
theorem process_block_inRange (p : Preset) (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState) (block : BeaconBlock)
    (h : process_block p o max_blobs_per_block GLOAS_FORK_EPOCH s block = .ok s')
    (hs : ConsolidationsInRange s) : ConsolidationsInRange s' := by
  unfold process_block at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  obtain ⟨s2, h2, h⟩ := specM_bind_ok h
  obtain ⟨s3, h3, h⟩ := specM_bind_ok h
  obtain ⟨s4, h4, h⟩ := specM_bind_ok h
  obtain ⟨s5, h5, h⟩ := specM_bind_ok h
  obtain ⟨s6, h6, h⟩ := specM_bind_ok h
  obtain ⟨s7, h7, h⟩ := specM_bind_ok h
  have e1 := (process_parent_execution_payload_rstep p o _ _ block h1).1 hs
  exact ((process_block_header_cstep p o _ _ block h2).trans
    ((process_withdrawals_cstep p _ _ h3).trans
    ((process_execution_payload_bid_cstep p o max_blobs_per_block _ _ _ h4).trans
    ((process_randao_cstep p o _ _ _ h5).trans
    ((process_eth1_data_cstep p _ _ _ h6).trans
    ((process_operations_cstep p o GLOAS_FORK_EPOCH _ _ _ _ h7).trans
    (process_sync_aggregate_cstep p o _ _ _ h))))))).inRange e1

/-! ## Slots and epoch steps -/

/-- `process_slot` keeps the queue and the registry. -/
theorem process_slot_cstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_slot p o s = .ok s') : CStep s s' := by
  have hf := process_slot_frame p o s s' h
  unfold process_slot at h
  c_split h
  all_goals (cases h; c_close)

/-- `process_justification_and_finalization` keeps the queue and the registry. -/
theorem process_justification_and_finalization_cstep (p : Preset) (s s' : BeaconState)
    (h : process_justification_and_finalization p s = .ok s') : CStep s s' := by
  have hv := (process_justification_and_finalization_frame p s s' h).1.1
  refine ⟨?_, by rw [hv]⟩
  unfold process_justification_and_finalization at h
  c_split h
  all_goals first
    | (cases h; rfl)
    | skip
  unfold weigh_justification_and_finalization at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨t1, ht1, h⟩ := specM_bind_ok h
  have q1 : t1.pending_consolidations = s.pending_consolidations := by
    c_split ht1
    all_goals (cases ht1; rfl)
  obtain ⟨t2, ht2, h⟩ := specM_bind_ok h
  have q2 : t2.pending_consolidations = s.pending_consolidations := by
    c_split ht2
    all_goals (cases ht2; exact q1)
  obtain ⟨t3, ht3, h⟩ := specM_bind_ok h
  have q3 : t3.pending_consolidations = s.pending_consolidations := by
    c_split ht3
    all_goals (cases ht3; exact q2)
  obtain ⟨t4, ht4, h⟩ := specM_bind_ok h
  have q4 : t4.pending_consolidations = s.pending_consolidations := by
    c_split ht4
    all_goals (cases ht4; exact q3)
  obtain ⟨t5, ht5, h⟩ := specM_bind_ok h
  have q5 : t5.pending_consolidations = s.pending_consolidations := by
    c_split ht5
    all_goals (cases ht5; exact q4)
  c_split h
  all_goals (cases h; exact q5)

/-- `process_inactivity_updates` keeps the queue and the registry. -/
theorem process_inactivity_updates_cstep (p : Preset) (s s' : BeaconState)
    (h : process_inactivity_updates p s = .ok s') : CStep s s' := by
  unfold process_inactivity_updates at h
  c_split h
  all_goals (cases h; c_close)

/-- `process_rewards_and_penalties` keeps the queue and the registry. -/
theorem process_rewards_and_penalties_cstep (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_rewards_and_penalties p total_active_balance s = .ok s') :
    CStep s s' := by
  unfold process_rewards_and_penalties at h
  c_split h
  all_goals (cases h; c_close)

/-- `process_registry_updates` keeps the queue and the registry size. -/
theorem process_registry_updates_cstep (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_registry_updates p total_active_balance s = .ok s') :
    CStep s s' := by
  unfold process_registry_updates at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨r, hr, h⟩ := specM_bind_ok h
  cases h
  refine forIn_cstep hr ?_
  intro _ _ _ hb
  c_split hb
  all_goals
    cases hb
    simp only [ForInStep.value]
    first
      | exact ⟨rfl, rfl⟩
      | exact initiate_validator_exit_cstep p _ _ _ _ ‹initiate_validator_exit _ _ _ _ = _›
      | exact ⟨rfl, List.length_set⟩

/-- `process_slashings` keeps the queue and the registry. -/
theorem process_slashings_cstep (p : Preset) (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_slashings p total_active_balance s = .ok s') : CStep s s' := by
  unfold process_slashings at h
  c_split h
  all_goals (cases h; c_close)

/-- `process_builder_pending_payments` keeps the queue and the registry. -/
theorem process_builder_pending_payments_cstep (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_builder_pending_payments p total_active_balance s = .ok s') :
    CStep s s' := by
  unfold process_builder_pending_payments at h
  c_split h
  all_goals (cases h; c_close)

/-- The queue stays and the registry only grows. -/
def DStep (s s' : BeaconState) : Prop :=
  s'.pending_consolidations = s.pending_consolidations ∧
    s.validators.length ≤ s'.validators.length

/-- `DStep` keeps `ConsolidationsInRange`. -/
theorem DStep.inRange {s s' : BeaconState} (h : DStep s s') (hs : ConsolidationsInRange s) :
    ConsolidationsInRange s' := by
  intro c hc
  rw [h.1] at hc
  obtain ⟨h1, h2⟩ := hs c hc
  exact ⟨Nat.lt_of_lt_of_le h1 h.2, Nat.lt_of_lt_of_le h2 h.2⟩

/-- `apply_pending_deposit` keeps the queue, and the registry only grows. -/
theorem apply_pending_deposit_dstep (p : Preset) (o : Oracle) (s s' : BeaconState)
    (deposit : PendingDeposit) (h : apply_pending_deposit p o s deposit = .ok s') :
    DStep s s' := by
  unfold apply_pending_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨rfl, Nat.le_refl _⟩)
    | skip
  unfold add_validator_to_registry get_index_for_new_validator at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨l1, h1, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  cases h
  refine ⟨rfl, ?_⟩
  show s.validators.length ≤ l1.length
  rw [set_or_append_list_len_eq h1]
  exact Nat.le_succ _

/-- `process_pending_deposits` with `apply_pending_deposit` keeps the queue, and the registry
only grows. -/
theorem process_pending_deposits_dstep (p : Preset) (o : Oracle)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_pending_deposits p total_active_balance (apply_pending_deposit p o) s = .ok s') :
    DStep s s' := by
  unfold process_pending_deposits at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, _, _, _, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := forIn_ok_invariant_c
    (fun r : MProd (List PendingDeposit) (MProd Bool (MProd Uint64 (MProd Uint64 BeaconState))) =>
      DStep s r.2.2.2.2) _ ?_ _ _ _ ⟨rfl, Nat.le_refl _⟩ hr
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
         have hd := apply_pending_deposit_dstep p o _ _ _ ‹apply_pending_deposit _ _ _ _ = _›
         exact ⟨hd.1.trans hst.1, Nat.le_trans hst.2 hd.2⟩)

/-- `process_pending_consolidations` keeps `ConsolidationsInRange`. It keeps the registry and
drops a prefix of the queue. -/
theorem process_pending_consolidations_inRange (p : Preset) (s s' : BeaconState)
    (h : process_pending_consolidations p s = .ok s') (hs : ConsolidationsInRange s) :
    ConsolidationsInRange s' := by
  unfold process_pending_consolidations at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := forIn_ok_invariant_c (fun r : MProd Nat BeaconState => CStep s r.snd) _ ?_
    _ _ _ (CStep.refl s) hr
  · cases h
    intro c hc
    change c ∈ st.pending_consolidations.drop _ at hc
    have hc' := List.mem_of_mem_drop hc
    simp only at hP
    rw [hP.1] at hc'
    change c.source_index < st.validators.length ∧ c.target_index < st.validators.length
    rw [hP.2]
    exact hs c hc'
  · intro _ ⟨_, _⟩ r hst hb
    c_split hb
    all_goals
      cases hb
      try simp only at hst
      c_close

/-- `process_effective_balance_updates` keeps the queue and the registry size. -/
theorem process_effective_balance_updates_cstep (p : Preset) (s s' : BeaconState)
    (h : process_effective_balance_updates p s = .ok s') : CStep s s' := by
  rw [process_effective_balance_updates_eq] at h
  cases hm : s.validators.zipIdx.mapM (effectiveBalanceStep p s.balances) with
  | error e => simp [hm, Except.map] at h
  | ok validators =>
    simp only [hm, Except.map, Except.ok.injEq] at h
    subst h
    obtain ⟨hlen, -⟩ := mapM_ok_getElem _ _ _ hm
    rw [List.length_zipIdx] at hlen
    exact ⟨rfl, hlen⟩

/-- A step with the same validators and the same queue. -/
private theorem cstep_of_frame {s s' : BeaconState} (hv : s'.validators = s.validators)
    (hq : s'.pending_consolidations = s.pending_consolidations) : CStep s s' :=
  ⟨hq, by rw [hv]⟩

/-- `process_epoch` keeps `ConsolidationsInRange`. -/
theorem process_epoch_inRange (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_epoch p o s = .ok s') (hs : ConsolidationsInRange s) :
    ConsolidationsInRange s' := by
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
  have e5 : ConsolidationsInRange s5 :=
    ((process_justification_and_finalization_cstep p _ _ h1).trans
    ((process_inactivity_updates_cstep p _ _ h2).trans
    ((process_rewards_and_penalties_cstep p _ _ _ h3).trans
    ((process_registry_updates_cstep p _ _ _ h4).trans
    (process_slashings_cstep p _ _ _ h5))))).inRange hs
  have e6 : ConsolidationsInRange s6 := by
    unfold process_eth1_data_reset at h6
    c_split h6
    all_goals (cases h6; exact (CStep.refl s5).inRange e5)
  have e7 := (process_pending_deposits_dstep p o _ _ _ h7).inRange e6
  have e8 := process_pending_consolidations_inRange p _ _ h8 e7
  have e9 := (process_builder_pending_payments_cstep p _ _ _ h9).inRange e8
  have e10 := (process_effective_balance_updates_cstep p _ _ h10).inRange e9
  have e11 : ConsolidationsInRange s11 := by
    unfold process_slashings_reset at h11
    c_split h11
    all_goals (cases h11; exact (CStep.refl s10).inRange e10)
  have e12 : ConsolidationsInRange s12 := by
    unfold process_randao_mixes_reset at h12
    c_split h12
    all_goals (cases h12; exact (CStep.refl s11).inRange e11)
  have e13 : ConsolidationsInRange s13 := by
    unfold process_historical_summaries_update at h13
    c_split h13
    all_goals (cases h13; exact (CStep.refl s12).inRange e12)
  obtain ⟨_, _, rfl⟩ := process_sync_committee_updates_shape p o _ _ h14
  obtain ⟨_, rfl⟩ := process_proposer_lookahead_shape p o _ _ h15
  obtain ⟨_, rfl⟩ := process_ptc_window_shape p o _ _ h
  exact e13

/-- `process_slots` with the real `process_epoch` keeps `ConsolidationsInRange`. -/
theorem process_slots_inRange (p : Preset) (o : Oracle) (s s' : BeaconState) (slot : Slot)
    (h : process_slots p o (process_epoch p o) s slot = .ok s') (hs : ConsolidationsInRange s) :
    ConsolidationsInRange s' :=
  process_slots_invariant p o (process_epoch p o) ConsolidationsInRange
    (fun _ _ ht hP => (process_slot_cstep p o _ _ ht).inRange hP)
    (fun _ _ ht hP => process_epoch_inRange p o _ _ ht hP)
    (fun _ _ hP => hP) s s' slot hs h

/-- `state_transition` keeps `ConsolidationsInRange`. -/
theorem state_transition_inRange (p : Preset) (o : Oracle)
    (max_blobs_per_block : Epoch → Uint64) (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState)
    (signed_block : SignedBeaconBlock) (validate_result : Bool) (hs : ConsolidationsInRange s)
    (h : state_transition p o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
      validate_result = .ok s') :
    ConsolidationsInRange s' := by
  unfold state_transition at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h
       exact process_block_inRange p o max_blobs_per_block GLOAS_FORK_EPOCH _ _ _
         ‹process_block _ _ _ _ _ _ = _›
         (process_slots_inRange p o _ _ _ ‹process_slots _ _ _ _ _ = _› hs))

end EpochProofs.Spec
