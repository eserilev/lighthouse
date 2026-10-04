import EpochProofs.Sanity.Invariants.BalanceFloor
import EpochProofs.Sanity.Invariants.EpochEnd

/-!
# Exit epochs are `u64` values

`ExitEpochsU64` says that every exit epoch is at most `FAR_FUTURE_EPOCH`. A new exit epoch `e`
comes with a withdrawable epoch `e + MIN_VALIDATOR_WITHDRAWABILITY_DELAY` that `uint64Add`
computes, so `e` is below `2^64`. Every block and epoch step keeps `ExitEpochsU64`.
-/

namespace EpochProofs.Spec

/-- A successful `if` ran one of its branches. -/
private theorem ite_ok_x {α : Type} {c : Prop} [Decidable c] {t e : SpecM α} {x : α}
    (h : (if c then t else e) = .ok x) : (c ∧ t = .ok x) ∨ (¬ c ∧ e = .ok x) := by
  by_cases hc : c
  · exact .inl ⟨hc, by rw [if_pos hc] at h; exact h⟩
  · exact .inr ⟨hc, by rw [if_neg hc] at h; exact h⟩

/-- Splits the binds and `if`s of a successful run without `simp`. -/
local macro "peel_ok" h:ident : tactic => `(tactic| repeat' first
  | (guard_hyp $h:ident : ((_ : SpecM _) >>= _) = _; obtain ⟨_, _, $h:ident⟩ := specM_bind_ok $h)
  | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _
     rcases ite_ok_x $h with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩))

/-- Splits the binds and `if`s of a successful run, and closes a `throw` step. -/
local macro "peel_ok'" h:ident : tactic => `(tactic| repeat' first
  | (guard_hyp $h:ident : ((_ : SpecM _) >>= _) = _
     obtain ⟨_, hx, $h:ident⟩ := specM_bind_ok $h
     try (guard_hyp hx : (throw _ : SpecM _) = _; cases hx))
  | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _;
     rcases ite_ok_x $h with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩))

/-! ## Exit epochs are `u64` values -/

/-- Every exit epoch is at most `FAR_FUTURE_EPOCH`, so it is a `u64` value. -/
def ExitEpochsU64 (s : BeaconState) : Prop :=
  ∀ v ∈ s.validators, v.exit_epoch ≤ FAR_FUTURE_EPOCH

/-- Same validators keep `ExitEpochsU64`. -/
theorem ExitEpochsU64.of_validators_eq {s s' : BeaconState} (hv : s'.validators = s.validators)
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  intro v h
  rw [hv] at h
  exact hs v h

/-- A list write keeps `ExitEpochsU64` if the new value has a `u64` exit epoch. -/
theorem exitEpochsU64_set {l : List Validator} {i : Nat} {v' : Validator}
    (hl : ∀ v ∈ l, v.exit_epoch ≤ FAR_FUTURE_EPOCH) (hv' : v'.exit_epoch ≤ FAR_FUTURE_EPOCH) :
    ∀ v ∈ l.set i v', v.exit_epoch ≤ FAR_FUTURE_EPOCH := by
  intro u hu
  rcases List.mem_or_eq_of_mem_set hu with hu | rfl
  · exact hl u hu
  · exact hv'

/-- An exit epoch `e` with a successful `e + MIN_VALIDATOR_WITHDRAWABILITY_DELAY` is a `u64`
value. -/
private theorem exit_le_far {p : Preset} {e w : Nat}
    (hw : uint64Add e p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY = .ok w) : e ≤ FAR_FUTURE_EPOCH := by
  unfold uint64Add at hw
  split at hw
  · rename_i hlt
    simp only [UINT64_SIZE, FAR_FUTURE_EPOCH, Uint64, Epoch] at *
    omega
  · cases hw

/-- `initiate_validator_exit` keeps `ExitEpochsU64`. The new exit epoch plus the delay is
computed with `uint64Add`. -/
theorem initiate_validator_exit_exitEpochsU64 {p : Preset} {tab : Gwei} {s s' : BeaconState}
    {index : Nat} (h : initiate_validator_exit p tab s index = .ok s') (hs : ExitEpochsU64 s) :
    ExitEpochsU64 s' := by
  unfold initiate_validator_exit at h
  obtain ⟨v, hv, h⟩ := specM_bind_ok h
  by_cases hfar : v.exit_epoch = FAR_FUTURE_EPOCH
  · simp only [hfar, bne_self_eq_false, Bool.false_eq_true, if_false] at h
    obtain ⟨_, -, h⟩ := specM_bind_ok h
    obtain ⟨⟨e, s1⟩, hx, h⟩ := specM_bind_ok h
    obtain ⟨w, hw, h⟩ := specM_bind_ok h
    obtain ⟨l, hl, h⟩ := specM_bind_ok h
    cases h
    obtain ⟨-, -, -, hvs, -, -⟩ := compute_exit_epoch_and_update_churn_bound p tab s s1 _ e hx
    rw [(ExitsSanity.listSet_ok hl).2, hvs]
    exact exitEpochsU64_set hs (exit_le_far hw)
  · have hne : (v.exit_epoch != FAR_FUTURE_EPOCH) = true := by simp [hfar]
    simp only [hne, if_true, pure, Except.pure, Except.ok.injEq] at h
    subst h
    exact hs

/-- `slash_validator` keeps `ExitEpochsU64`. It changes exit epochs only through
`initiate_validator_exit`. -/
theorem slash_validator_exitEpochsU64 (p : Preset) (s s' : BeaconState) (i : ValidatorIndex)
    (w : Option ValidatorIndex) (h : slash_validator p s i w = .ok s') (hs : ExitEpochsU64 s) :
    ExitEpochsU64 s' := by
  unfold slash_validator at h
  obtain ⟨epoch, -, h⟩ := specM_bind_ok h
  obtain ⟨v0, hv0, h⟩ := specM_bind_ok h
  obtain ⟨s1, hs1, h⟩ := SlashingsSanity.bind_if_ok h
  obtain ⟨tab, hs1⟩ := slash_validator_initiate_ok p s s1 i v0 hv0 hs1
  have h1 := initiate_validator_exit_exitEpochsU64 hs1 hs
  obtain ⟨v1, hv1, h⟩ := specM_bind_ok h
  obtain ⟨we, -, h⟩ := specM_bind_ok h
  obtain ⟨vs2, hvs2, h⟩ := specM_bind_ok h
  have hfinal : ∀ v ∈ vs2, v.exit_epoch ≤ FAR_FUTURE_EPOCH := by
    rw [(ExitsSanity.listSet_ok hvs2).2]
    exact exitEpochsU64_set h1 (h1 v1 (List.mem_of_getElem? (listGet_ok hv1).2))
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact hfinal)

/-- `process_proposer_slashing` keeps `ExitEpochsU64`. -/
theorem process_proposer_slashing_exitEpochsU64 (p : Preset) (o : Oracle) (s s' : BeaconState)
    (ps : ProposerSlashing) (h : process_proposer_slashing p o s ps = .ok s')
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  obtain ⟨-, -, payments, -, -, -, hsl⟩ := process_proposer_slashing_ok p o s s' ps h
  exact slash_validator_exitEpochsU64 p _ s' _ none hsl hs

/-- `process_attester_slashing` keeps `ExitEpochsU64`. -/
theorem process_attester_slashing_exitEpochsU64 (p : Preset) (o : Oracle) (s s' : BeaconState)
    (as : AttesterSlashing) (h : process_attester_slashing p o s as = .ok s')
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  unfold process_attester_slashing at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  repeat' split at h
  all_goals first | (cases h; done) | skip
  cases h
  refine SlashingsSanity.forIn_invariant
    (fun r : MProd Bool BeaconState => ExitEpochsU64 r.snd) _ ?_ _
    (⟨false, s⟩ : MProd Bool BeaconState) _ hs ‹forIn _ _ _ = _›
  intro index r step hr hstep
  obtain ⟨_, -, hstep⟩ := specM_bind_ok hstep
  obtain ⟨_, -, hstep⟩ := specM_bind_ok hstep
  split at hstep
  · obtain ⟨r', hr', hstep⟩ := specM_bind_ok hstep
    cases hstep
    exact slash_validator_exitEpochsU64 p _ _ index none hr' hr
  · cases hstep
    exact hr

/-- `process_voluntary_exit` keeps `ExitEpochsU64`. -/
theorem process_voluntary_exit_exitEpochsU64 (p : Preset) (o : Oracle) (s s' : BeaconState)
    (e : SignedVoluntaryExit) (h : process_voluntary_exit p o s e = .ok s')
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  obtain ⟨tab, -, -, -, -, -, -, hi⟩ := process_voluntary_exit_initiate p o s s' e h
  exact initiate_validator_exit_exitEpochsU64 hi hs

/-- `process_bls_to_execution_change` keeps `ExitEpochsU64`. -/
theorem process_bls_to_execution_change_exitEpochsU64 (p : Preset) (o : Oracle)
    (s s' : BeaconState) (c : SignedBLSToExecutionChange)
    (h : process_bls_to_execution_change p o s c = .ok s') (hs : ExitEpochsU64 s) :
    ExitEpochsU64 s' := by
  obtain ⟨-, -, -, -, hlen, hpt⟩ := process_bls_to_execution_change_effects p o s s' c h
  intro u hu
  obtain ⟨j, hj, rfl⟩ := List.getElem_of_mem hu
  have hj' : j < s.validators.length := hlen ▸ hj
  obtain ⟨wc, h'⟩ := hpt j _ (List.getElem?_eq_getElem hj')
  rw [List.getElem?_eq_getElem hj] at h'
  rw [Option.some.inj h']
  exact hs s.validators[j] (List.getElem_mem hj')

/-- `process_withdrawal_request` keeps `ExitEpochsU64`. -/
theorem process_withdrawal_request_exitEpochsU64 (p : Preset) (s s' : BeaconState)
    (withdrawal_request : WithdrawalRequest)
    (h : process_withdrawal_request p s withdrawal_request = .ok s') (hs : ExitEpochsU64 s) :
    ExitEpochsU64 s' := by
  unfold process_withdrawal_request at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | (cases h; done)
    | (cases h; exact hs)
    | exact initiate_validator_exit_exitEpochsU64 ‹initiate_validator_exit _ _ _ _ = .ok _› hs
    | (cases h
       exact initiate_validator_exit_exitEpochsU64 ‹initiate_validator_exit _ _ _ _ = .ok _› hs)
    | (cases h
       obtain ⟨-, -, -, hv, -, -⟩ := compute_exit_epoch_and_update_churn_bound p _ s _ _ _
         ‹compute_exit_epoch_and_update_churn _ _ _ _ = .ok _›
       exact ExitEpochsU64.of_validators_eq hv hs)

/-- `switch_to_compounding_validator` keeps `ExitEpochsU64`. -/
theorem switch_to_compounding_validator_exitEpochsU64 {p : Preset} {s s' : BeaconState}
    {index : Nat} (h : switch_to_compounding_validator p s index = .ok s')
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  unfold switch_to_compounding_validator at h
  obtain ⟨v, hv, h⟩ := specM_bind_ok h
  obtain ⟨l, hl, h⟩ := specM_bind_ok h
  obtain ⟨hvs, -, -⟩ := queue_excess_active_balance_cases p _ s' index h
  have hvs' := hvs.trans (ExitsSanity.listSet_ok hl).2
  intro u hu
  rw [hvs'] at hu
  refine exitEpochsU64_set hs ?_ u hu
  exact hs v (List.mem_of_getElem? (listGet_ok hv).2)

/-- The last step of a consolidation keeps `ExitEpochsU64`. -/
private theorem consolidation_exit_exitEpochsU64 {p : Preset} {tab : Gwei} {s : BeaconState}
    {x i : Nat} {v' : Validator} {r : Epoch × BeaconState} {w : Uint64} {l : List Validator}
    (hr : compute_consolidation_epoch_and_update_churn p tab s x = .ok r)
    (hw : uint64Add r.1 p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY = .ok w)
    (hl : listSet r.2.validators i v' = .ok l) (hv' : v'.exit_epoch = r.1)
    (pc : List PendingConsolidation) (hs : ExitEpochsU64 s) :
    ExitEpochsU64 { r.2 with validators := l, pending_consolidations := pc } := by
  obtain ⟨e, s1⟩ := r
  obtain ⟨-, -, -, hvs, -, -⟩ :=
    compute_consolidation_epoch_and_update_churn_bound p tab s s1 x e hr
  rw [hvs] at hl
  intro u hu
  change u ∈ l at hu
  rw [(ExitsSanity.listSet_ok hl).2] at hu
  exact exitEpochsU64_set hs (hv' ▸ exit_le_far hw) u hu

/-- `process_consolidation_request` keeps `ExitEpochsU64`. -/
theorem process_consolidation_request_exitEpochsU64 (p : Preset) (s s' : BeaconState)
    (consolidation_request : ConsolidationRequest)
    (h : process_consolidation_request p s consolidation_request = .ok s')
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  unfold process_consolidation_request at h
  peel_ok h
  all_goals first
    | (cases h; exact hs)
    | exact switch_to_compounding_validator_exitEpochsU64 h hs
    | (cases h
       exact consolidation_exit_exitEpochsU64
         ‹compute_consolidation_epoch_and_update_churn _ _ _ _ = .ok _›
         ‹uint64Add _ _ = .ok _› ‹listSet _ _ _ = .ok _› rfl _ hs)

/-- A request loop of `apply_parent_execution_payload` keeps a state property. -/
private theorem request_loop_keeps {α : Type} (P : BeaconState → Prop) {l : List α}
    {body : α → BeaconState → SpecM (ForInStep BeaconState)} {s s' : BeaconState}
    (hloop : forIn l s body = .ok s')
    (hb : ∀ a st r, body a st = .ok r → P st → P r.value) : P s → P s' :=
  forIn_rel (fun a b => P a → P b) (fun _ h => h) (fun _ _ _ h12 h23 h => h23 (h12 h)) l body
    s s' hloop hb

/-- Proves the body step of a request loop from the step of one request. -/
local macro "loop_body_p" e:term : tactic => `(tactic| (
  intro _ _ _ hbody hst
  obtain ⟨_, h1, hbody⟩ := specM_bind_ok hbody
  obtain ⟨_, -, hbody⟩ := specM_bind_ok hbody
  cases hbody
  exact $e h1 hst))

/-- `apply_parent_execution_payload` keeps `ExitEpochsU64`. -/
theorem apply_parent_execution_payload_exitEpochsU64 (p : Preset) (o : Oracle)
    (s s' : BeaconState) (requests : ExecutionRequests)
    (h : apply_parent_execution_payload p o s requests = .ok s') (hs : ExitEpochsU64 s) :
    ExitEpochsU64 s' := by
  unfold apply_parent_execution_payload at h
  peel_ok' h
  all_goals first
    | (cases h
       have h1 := request_loop_keeps ExitEpochsU64
         (by assumption : forIn requests.deposits _ _ = Except.ok _)
         (by loop_body_p (fun h hst => ExitEpochsU64.of_validators_eq (by cases h; rfl) hst))
         hs
       have h2 := request_loop_keeps ExitEpochsU64
         (by assumption : forIn requests.withdrawals _ _ = Except.ok _)
         (by loop_body_p process_withdrawal_request_exitEpochsU64 _ _ _ _) h1
       have h3 := request_loop_keeps ExitEpochsU64
         (by assumption : forIn requests.consolidations _ _ = Except.ok _)
         (by loop_body_p process_consolidation_request_exitEpochsU64 _ _ _ _) h2
       have h4 := request_loop_keeps ExitEpochsU64
         (by assumption : forIn requests.builder_deposits _ _ = Except.ok _)
         (by loop_body_p (fun h hst => ExitEpochsU64.of_validators_eq
           (process_builder_deposit_request_frame _ _ _ _ _ h).1.1 hst)) h3
       have h5 := request_loop_keeps ExitEpochsU64
         (by assumption : forIn requests.builder_exits _ _ = Except.ok _)
         (by loop_body_p (fun h hst => ExitEpochsU64.of_validators_eq
           (process_builder_exit_request_frame _ _ _ _ h).1.1 hst)) h4
       first
         | (have h6 := ExitEpochsU64.of_validators_eq
             (settle_builder_payment_frame _ _ _ ‹settle_builder_payment _ _ = Except.ok _›).1.1 h5
            exact fun v hv => h6 v hv)
         | exact fun v hv => h5 v hv)

/-- `process_parent_execution_payload` keeps `ExitEpochsU64`. -/
theorem process_parent_execution_payload_exitEpochsU64 (p : Preset) (o : Oracle)
    (s s' : BeaconState) (block : BeaconBlock)
    (h : process_parent_execution_payload p o s block = .ok s') (hs : ExitEpochsU64 s) :
    ExitEpochsU64 s' := by
  unfold process_parent_execution_payload at h
  peel_ok' h
  all_goals first
    | (cases h; exact hs)
    | exact apply_parent_execution_payload_exitEpochsU64 p o _ _ _ h hs

/-- `process_operations` keeps `ExitEpochsU64`. -/
theorem process_operations_exitEpochsU64 (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (s s' : BeaconState) (body : BeaconBlockBody) (parent_slot : Slot)
    (h : process_operations p o GLOAS_FORK_EPOCH s body parent_slot = .ok s')
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  have fold : ∀ {α : Type} (f : BeaconState → α → SpecM BeaconState),
      (∀ st a st', f st a = .ok st' → ExitEpochsU64 st → ExitEpochsU64 st') →
      ∀ (l : List α) (st st' : BeaconState), l.foldlM f st = .ok st' →
        ExitEpochsU64 st → ExitEpochsU64 st' := fun f hf =>
    foldlM_rel (fun a b => ExitEpochsU64 a → ExitEpochsU64 b) (fun _ h => h)
      (fun _ _ _ h12 h23 h => h23 (h12 h)) f hf
  unfold process_operations at h
  simp only [bind, Except.bind, pure, Except.pure, throw, throwThe, MonadExceptOf.throw] at h
  repeat' split at h
  all_goals first | (cases h; done) | skip
  rename_i _ s1 h1 _ s2 h2 _ s3 h3 _ s4 h4 _ s5 h5
  have e1 := fold _ (fun st a st' hst => process_proposer_slashing_exitEpochsU64 p o st st' a hst)
    _ _ _ h1 hs
  have e2 := fold _ (fun st a st' hst => process_attester_slashing_exitEpochsU64 p o st st' a hst)
    _ _ _ h2 e1
  have e3 := fold _ (fun st a st' hst => ExitEpochsU64.of_validators_eq
    (process_attestation_validators p o st st' a parent_slot hst)) _ _ _ h3 e2
  have e4 := fold _ (fun st a st' hst => process_voluntary_exit_exitEpochsU64 p o st st' a hst)
    _ _ _ h4 e3
  have e5 := fold _ (fun st a st' hst =>
    process_bls_to_execution_change_exitEpochsU64 p o st st' a hst) _ _ _ h5 e4
  exact fold _ (fun st a st' hst => ExitEpochsU64.of_validators_eq
    (by rw [process_payload_attestation_eq p o GLOAS_FORK_EPOCH st st' a hst])) _ _ _ h e5

/-- `process_block` keeps `ExitEpochsU64`. -/
theorem process_block_exitEpochsU64 (p : Preset) (o : Oracle)
    (max_blobs_per_block : Epoch → Uint64) (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState)
    (block : BeaconBlock)
    (h : process_block p o max_blobs_per_block GLOAS_FORK_EPOCH s block = .ok s')
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  unfold process_block at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  obtain ⟨s2, h2, h⟩ := specM_bind_ok h
  obtain ⟨s3, h3, h⟩ := specM_bind_ok h
  obtain ⟨s4, h4, h⟩ := specM_bind_ok h
  obtain ⟨s5, h5, h⟩ := specM_bind_ok h
  obtain ⟨s6, h6, h⟩ := specM_bind_ok h
  obtain ⟨s7, h7, h⟩ := specM_bind_ok h
  have e1 := process_parent_execution_payload_exitEpochsU64 p o _ _ block h1 hs
  have e2 := ExitEpochsU64.of_validators_eq (process_block_header_frame p o _ _ block h2).1.1 e1
  have e3 := ExitEpochsU64.of_validators_eq (process_withdrawals_core p _ _ h3).1 e2
  have e4 := ExitEpochsU64.of_validators_eq
    (process_execution_payload_bid_frame p o _ _ _ _ h4).1.1 e3
  have e5 := ExitEpochsU64.of_validators_eq (process_randao_frame p o _ _ _ h5).1.1 e4
  have e6 := ExitEpochsU64.of_validators_eq (process_eth1_data_frame p _ _ _ h6).1.1 e5
  have e7 := process_operations_exitEpochsU64 p o GLOAS_FORK_EPOCH _ _ _ _ h7 e6
  obtain ⟨hfr, -⟩ := process_sync_aggregate_frame p o _ _ _ h
  exact ExitEpochsU64.of_validators_eq (by rw [hfr]) e7

/-- `process_registry_updates` keeps `ExitEpochsU64`. -/
theorem process_registry_updates_exitEpochsU64 (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_registry_updates p total_active_balance s = .ok s')
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  unfold process_registry_updates at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨r, hr, h⟩ := specM_bind_ok h
  cases h
  refine SlashingsSanity.forIn_invariant ExitEpochsU64 _ ?_ _ s _ hs hr
  intro index st r hst hb
  simp only [bind, Except.bind, pure, Except.pure] at hb
  repeat' split at hb
  all_goals first
    | contradiction
    | (cases hb; exact hst)
    | (cases hb
       exact initiate_validator_exit_exitEpochsU64 ‹initiate_validator_exit _ _ _ _ = _› hst)
    | (cases hb
       intro u hu
       change u ∈ _ at hu
       obtain ⟨-, hl⟩ := ExitsSanity.listSet_ok ‹listSet _ _ _ = _›
       rw [hl] at hu
       refine exitEpochsU64_set hst ?_ u hu
       obtain ⟨-, hg⟩ := listGet_ok ‹listGet _ _ = _›
       have := hst _ (List.mem_of_getElem? hg)
       exact this)

/-- A validator from a deposit has no exit epoch yet. -/
theorem get_validator_from_deposit_exit (p : Preset) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei) (v : Validator)
    (h : get_validator_from_deposit p pubkey withdrawal_credentials amount = .ok v) :
    v.exit_epoch = FAR_FUTURE_EPOCH := by
  unfold get_validator_from_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; rfl)

/-- `apply_pending_deposit` keeps `ExitEpochsU64`. A new validator has no exit epoch yet. -/
theorem apply_pending_deposit_exitEpochsU64 (p : Preset) (o : Oracle) (s s' : BeaconState)
    (deposit : PendingDeposit) (h : apply_pending_deposit p o s deposit = .ok s')
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  have hadd : ∀ (pk : BLSPubkey) (wc : Bytes32) (amount : Gwei) (t : BeaconState),
      add_validator_to_registry p s pk wc amount = .ok t → ExitEpochsU64 t := by
    intro pk wc amount t ht
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
    intro u hu
    rcases List.mem_append.mp hu with hu | hu
    · exact hs u hu
    · rw [List.mem_singleton.mp hu, get_validator_from_deposit_exit p _ _ _ v hv]
      exact Nat.le_refl _
  unfold apply_pending_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact hs)
    | exact hadd _ _ _ _ h

/-- `process_pending_deposits` with `apply_pending_deposit` keeps `ExitEpochsU64`. -/
theorem process_pending_deposits_exitEpochsU64 (p : Preset) (o : Oracle)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_pending_deposits p total_active_balance (apply_pending_deposit p o) s = .ok s')
    (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
  unfold process_pending_deposits at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, _, _, _, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := SlashingsSanity.forIn_invariant
    (fun r : MProd (List PendingDeposit) (MProd Bool (MProd Uint64 (MProd Uint64 BeaconState))) =>
      ExitEpochsU64 r.2.2.2.2) _ ?_ _ _ _ hs hr
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
         exact apply_pending_deposit_exitEpochsU64 p o _ _ _
           ‹apply_pending_deposit _ _ _ _ = _› hst)

/-- `process_effective_balance_updates` keeps `ExitEpochsU64`. It writes only effective
balances. -/
theorem process_effective_balance_updates_exitEpochsU64 (p : Preset) (s s' : BeaconState)
    (h : process_effective_balance_updates p s = .ok s') (hs : ExitEpochsU64 s) :
    ExitEpochsU64 s' := by
  rw [process_effective_balance_updates_eq] at h
  cases hm : s.validators.zipIdx.mapM (effectiveBalanceStep p s.balances) with
  | error e => simp [hm, Except.map] at h
  | ok validators =>
    simp only [hm, Except.map, Except.ok.injEq] at h
    subst h
    obtain ⟨hlen, hget⟩ := mapM_ok_getElem _ _ _ hm
    rw [List.length_zipIdx] at hlen
    intro u hu
    obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hu
    have hv : i < s.validators.length := hlen ▸ hi
    have hs' := hget i (by simpa using hv) hi
    rw [List.getElem_zipIdx] at hs'
    have hkeep : validators[i].exit_epoch = s.validators[i].exit_epoch := by
      generalize validators[i] = w at hs' ⊢
      unfold effectiveBalanceStep at hs'
      simp only [bind, Except.bind, pure, Except.pure] at hs'
      repeat' split at hs'
      all_goals first
        | contradiction
        | (cases hs'; rfl)
    rw [hkeep]
    exact hs _ (List.getElem_mem hv)

/-- `process_epoch` keeps `ExitEpochsU64`. -/
theorem process_epoch_exitEpochsU64 (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_epoch p o s = .ok s') (hs : ExitEpochsU64 s) : ExitEpochsU64 s' := by
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
  have d1 := ExitEpochsU64.of_validators_eq
    (process_justification_and_finalization_frame p s s1 h1).1.1 hs
  have d2 := ExitEpochsU64.of_validators_eq (process_inactivity_updates_validators p _ _ h2) d1
  have d3 := ExitEpochsU64.of_validators_eq
    (process_rewards_and_penalties_validators p _ _ _ h3) d2
  have d4 := process_registry_updates_exitEpochsU64 p _ _ _ h4 d3
  have d5 := ExitEpochsU64.of_validators_eq (process_slashings_validators p _ _ _ h5) d4
  have d6 := ExitEpochsU64.of_validators_eq (process_eth1_data_reset_frame p _ _ h6).1.1 d5
  have d7 := process_pending_deposits_exitEpochsU64 p o _ _ _ h7 d6
  have d8 := ExitEpochsU64.of_validators_eq
    (process_pending_consolidations_validators p _ _ h8) d7
  have d9 := ExitEpochsU64.of_validators_eq
    (process_builder_pending_payments_validators p _ _ _ h9) d8
  have d10 := process_effective_balance_updates_exitEpochsU64 p _ _ h10 d9
  have d11 := ExitEpochsU64.of_validators_eq (process_slashings_reset_frame p _ _ h11).1.1 d10
  have d12 := ExitEpochsU64.of_validators_eq (process_randao_mixes_reset_frame p _ _ h12).1.1 d11
  have d13 := ExitEpochsU64.of_validators_eq
    (process_historical_summaries_update_frame p o _ _ h13).1.1 d12
  have d14 := ExitEpochsU64.of_validators_eq
    (process_sync_committee_updates_frame p o _ _ h14).1.1 d13
  have d15 := ExitEpochsU64.of_validators_eq (process_proposer_lookahead_frame p o _ _ h15).1.1 d14
  exact ExitEpochsU64.of_validators_eq (process_ptc_window_frame p o _ _ h).1.1 d15

end EpochProofs.Spec
