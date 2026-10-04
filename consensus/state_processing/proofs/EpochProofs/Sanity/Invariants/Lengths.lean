import EpochProofs.Sanity.Driver
import EpochProofs.Sanity.Rows
import EpochProofs.Sanity.EffectiveBalanceInvariant

/-!
# The row lengths invariant

`RowsOk` says that `balances`, `inactivity_scores` and both participation lists have one entry
per validator. Most steps keep each of these five lengths: that is `LenStep`. Two epoch steps
add or reset rows, and they keep `RowsOk` directly. So `state_transition` keeps `RowsOk`.
-/

namespace EpochProofs.Spec

/-- The five row lengths do not change. -/
def LenStep (s s' : BeaconState) : Prop :=
  s'.validators.length = s.validators.length ∧ s'.balances.length = s.balances.length ∧
    s'.inactivity_scores.length = s.inactivity_scores.length ∧
    s'.previous_epoch_participation.length = s.previous_epoch_participation.length ∧
    s'.current_epoch_participation.length = s.current_epoch_participation.length

/-- `LenStep` is reflexive. -/
theorem LenStep.refl (s : BeaconState) : LenStep s s := ⟨rfl, rfl, rfl, rfl, rfl⟩

/-- `LenStep` is transitive. -/
theorem LenStep.trans {s1 s2 s3 : BeaconState} (h12 : LenStep s1 s2) (h23 : LenStep s2 s3) :
    LenStep s1 s3 :=
  ⟨h23.1.trans h12.1, h23.2.1.trans h12.2.1, h23.2.2.1.trans h12.2.2.1,
    h23.2.2.2.1.trans h12.2.2.2.1, h23.2.2.2.2.trans h12.2.2.2.2⟩

/-- `LenStep` keeps `RowsOk`. -/
theorem LenStep.rowsOk {s s' : BeaconState} (h : LenStep s s') (hs : RowsOk s) : RowsOk s' := by
  obtain ⟨h1, h2, h3, h4, h5⟩ := h
  obtain ⟨r1, r2, r3, r4⟩ := hs
  exact ⟨by omega, by omega, by omega, by omega⟩

/-- A `listSet` followed by more code is an `if` on the index. -/
theorem listSet_bind {α β : Type} (l : List α) (i : Nat) (a : α) (f : List α → SpecM β) :
    (listSet l i a >>= f) = if i < l.length then f (l.set i a) else .error .indexOutOfRange := by
  unfold listSet
  split <;> rfl

/-- A successful `if` ran one of its branches. -/
private theorem ite_ok {α : Type} {c : Prop} [Decidable c] {t e : SpecM α} {x : α}
    (h : (if c then t else e) = .ok x) : (c ∧ t = .ok x) ∨ (¬ c ∧ e = .ok x) := by
  by_cases hc : c
  · exact .inl ⟨hc, by rw [if_pos hc] at h; exact h⟩
  · exact .inr ⟨hc, by rw [if_neg hc] at h; exact h⟩

/-- Unfolds the list helpers of a step, then splits its binds, `if`s and `match`es. Each error
branch closes. -/
local macro "len_split" h:ident : tactic => `(tactic| (
  try simp only [increase_balance, decrease_balance, bind_assoc, listSet_bind] at $h:ident
  try simp only [throw, throwThe, MonadExceptOf.throw] at $h:ident
  try simp only [bind, Except.bind, pure, Except.pure] at $h:ident
  repeat' first
    | split at $h:ident
    | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _
       rcases ite_ok $h:ident with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩)
  all_goals try contradiction))

/-- Splits the binds and `if`s of a successful run without `simp`. -/
local macro "peel_ok" h:ident : tactic => `(tactic| repeat' first
  | (guard_hyp $h:ident : ((_ : SpecM _) >>= _) = _
     obtain ⟨_, hx, $h:ident⟩ := specM_bind_ok $h
     try (guard_hyp hx : (throw _ : SpecM _) = _; cases hx))
  | (guard_hyp $h:ident : (@ite (SpecM _) _ (_) _ _) = _;
     rcases ite_ok $h with ⟨_, $h:ident⟩ | ⟨_, $h:ident⟩))

/-- Proves one pass of a request loop from the lemma for one request. -/
local macro "loop_body" e:term : tactic => `(tactic| (
  intro _ _ _ hbody
  obtain ⟨_, h1, hbody⟩ := specM_bind_ok hbody
  obtain ⟨_, -, hbody⟩ := specM_bind_ok hbody
  cases hbody
  exact $e h1))

/-- Closes `LenStep s s'` from the length facts in the context. -/
local macro "len_close" : tactic => `(tactic| (
  simp only [LenStep, List.length_set, ForInStep.value] at *
  repeat' apply And.intro
  all_goals first | trivial | omega))

/-- A `for` loop in `SpecM` keeps every property that each successful pass keeps. -/
private theorem forIn_ok_invariant {α β : Type} (P : β → Prop)
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

/-- A `for` loop over the state keeps `LenStep` when each pass keeps it. -/
private theorem forIn_len {α : Type} {f : α → BeaconState → SpecM (ForInStep BeaconState)}
    {l : List α} {s s' : BeaconState} (h : forIn l s f = .ok s')
    (hf : ∀ a st r, f a st = .ok r → LenStep st r.value) : LenStep s s' :=
  forIn_ok_invariant (LenStep s) f (fun a b r hb hr => hb.trans (hf a b r hr)) l s s'
    (LenStep.refl s) h

/-- A `List.foldlM` over the state keeps `LenStep` when each step keeps it. -/
private theorem foldlM_len {α : Type} (f : BeaconState → α → SpecM BeaconState)
    (hf : ∀ s a s', f s a = .ok s' → LenStep s s') :
    ∀ (l : List α) (s s' : BeaconState), l.foldlM f s = .ok s' → LenStep s s' := by
  intro l
  induction l with
  | nil => intro s s' h; cases h; exact LenStep.refl s
  | cons a as ih =>
    intro s s' h
    rw [List.foldlM_cons] at h
    obtain ⟨s1, h1, h⟩ := specM_bind_ok h
    exact (hf s a s1 h1).trans (ih s1 s' h)

/-! ## Block operations -/

/-- The state from `compute_exit_epoch_and_update_churn` keeps the five row lengths. -/
theorem compute_exit_epoch_and_update_churn_len (p : Preset) (total_active_balance : Gwei)
    (s : BeaconState) (exit_balance : Gwei) (r : Epoch × BeaconState)
    (h : compute_exit_epoch_and_update_churn p total_active_balance s exit_balance = .ok r) :
    LenStep s r.2 := by
  unfold compute_exit_epoch_and_update_churn at h
  len_split h
  all_goals (cases h; exact LenStep.refl _)

/-- `initiate_validator_exit` keeps the five row lengths. -/
theorem initiate_validator_exit_len (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (index : ValidatorIndex)
    (h : initiate_validator_exit p total_active_balance s index = .ok s') : LenStep s s' := by
  unfold initiate_validator_exit at h
  len_split h
  all_goals
    cases h
    try (have := compute_exit_epoch_and_update_churn_len p total_active_balance s _ _
                    ‹compute_exit_epoch_and_update_churn _ _ _ _ = _›)
    len_close

/-- `slash_validator` keeps the five row lengths. -/
theorem slash_validator_len (p : Preset) (s s' : BeaconState) (index : ValidatorIndex)
    (whistleblower_index : Option ValidatorIndex)
    (h : slash_validator p s index whistleblower_index = .ok s') : LenStep s s' := by
  unfold slash_validator at h
  len_split h
  all_goals
    cases h
    try have := initiate_validator_exit_len p _ s _ _ ‹initiate_validator_exit _ _ _ _ = _›
    len_close

/-- `process_proposer_slashing` keeps the five row lengths. -/
theorem process_proposer_slashing_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (proposer_slashing : ProposerSlashing)
    (h : process_proposer_slashing p o s proposer_slashing = .ok s') : LenStep s s' := by
  unfold process_proposer_slashing at h
  len_split h
  all_goals
    try (have := slash_validator_len p _ _ _ _ ‹slash_validator _ _ _ _ = _›)
    len_close

/-- `process_attester_slashing` keeps the five row lengths. -/
theorem process_attester_slashing_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attester_slashing : AttesterSlashing)
    (h : process_attester_slashing p o s attester_slashing = .ok s') : LenStep s s' := by
  unfold process_attester_slashing at h
  len_split h
  all_goals
    cases h
    refine forIn_ok_invariant (fun r : MProd Bool BeaconState => LenStep s r.snd) _ ?_ _ _ _
      (LenStep.refl s) ‹forIn _ _ _ = _›
    intro _ ⟨_, st⟩ _ hst hb
    len_split hb
    all_goals
      cases hb
      try (have := slash_validator_len p _ _ _ _ ‹slash_validator _ _ _ _ = _›)
      try simp only at hst
      len_close

/-- `process_voluntary_exit` keeps the five row lengths. -/
theorem process_voluntary_exit_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (signed_voluntary_exit : SignedVoluntaryExit)
    (h : process_voluntary_exit p o s signed_voluntary_exit = .ok s') : LenStep s s' := by
  unfold process_voluntary_exit at h
  len_split h
  all_goals
    try (have := initiate_validator_exit_len p _ _ _ _ ‹initiate_validator_exit _ _ _ _ = _›)
    len_close

/-- `process_bls_to_execution_change` keeps the five row lengths. -/
theorem process_bls_to_execution_change_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (signed_address_change : SignedBLSToExecutionChange)
    (h : process_bls_to_execution_change p o s signed_address_change = .ok s') :
    LenStep s s' := by
  unfold process_bls_to_execution_change at h
  len_split h
  all_goals (cases h; len_close)

/-- `process_attestation` keeps the five row lengths. -/
theorem process_attestation_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (attestation : Attestation) (parent_slot : Slot)
    (h : process_attestation p o s attestation parent_slot = .ok s') : LenStep s s' := by
  have hv := process_attestation_validators p o s s' attestation parent_slot h
  have hb := (process_attestation_balancesUp p o s s' attestation parent_slot h)
  have hpart := process_attestation_participation_length p o s s' attestation parent_slot h
  obtain ⟨_, bal, _, ⟨_, _, hinc⟩, -, hs | hs⟩ :=
    process_attestation_shape p o s s' attestation parent_slot h
  all_goals
    obtain ⟨hlen, -⟩ := increase_balance_up hinc
    subst hs
    exact ⟨rfl, hlen, rfl, hpart.2, hpart.1⟩

/-- `process_payload_attestation` keeps the five row lengths. -/
theorem process_payload_attestation_len (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (s s' : BeaconState) (payload_attestation : PayloadAttestation)
    (h : process_payload_attestation p o GLOAS_FORK_EPOCH s payload_attestation = .ok s') :
    LenStep s s' := by
  rw [process_payload_attestation_eq p o GLOAS_FORK_EPOCH s s' payload_attestation h]
  exact LenStep.refl s

/-- `process_operations` keeps the five row lengths. -/
theorem process_operations_len (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (s s' : BeaconState) (body : BeaconBlockBody) (parent_slot : Slot)
    (h : process_operations p o GLOAS_FORK_EPOCH s body parent_slot = .ok s') : LenStep s s' := by
  unfold process_operations at h
  simp only [throw, throwThe, MonadExceptOf.throw, bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals try contradiction
  have h1 := foldlM_len _ (fun s a s' h => process_proposer_slashing_len p o s s' a h) _ _ _
    ‹List.foldlM (process_proposer_slashing p o) _ _ = _›
  have h2 := foldlM_len _ (fun s a s' h => process_attester_slashing_len p o s s' a h) _ _ _
    ‹List.foldlM (process_attester_slashing p o) _ _ = _›
  have h3 := foldlM_len _ (fun s a s' h => process_attestation_len p o s s' a parent_slot h)
    _ _ _ ‹List.foldlM (fun state attestation => process_attestation p o state attestation _)
      _ _ = _›
  have h4 := foldlM_len _ (fun s a s' h => process_voluntary_exit_len p o s s' a h) _ _ _
    ‹List.foldlM (process_voluntary_exit p o) _ _ = _›
  have h5 := foldlM_len _ (fun s a s' h => process_bls_to_execution_change_len p o s s' a h)
    _ _ _ ‹List.foldlM (process_bls_to_execution_change p o) _ _ = _›
  have h6 := foldlM_len _
    (fun s a s' h => process_payload_attestation_len p o GLOAS_FORK_EPOCH s s' a h) _ _ _ h
  exact h1.trans (h2.trans (h3.trans (h4.trans (h5.trans h6))))

/-- `process_deposit_request` keeps the five row lengths. -/
theorem process_deposit_request_len (s s' : BeaconState) (deposit_request : DepositRequest)
    (h : process_deposit_request s deposit_request = .ok s') : LenStep s s' := by
  unfold process_deposit_request at h
  cases h
  exact LenStep.refl s

/-- The state from `compute_consolidation_epoch_and_update_churn` keeps the five row lengths. -/
theorem compute_consolidation_epoch_and_update_churn_len (p : Preset)
    (total_active_balance : Gwei) (s : BeaconState) (consolidation_balance : Gwei)
    (r : Epoch × BeaconState)
    (h : compute_consolidation_epoch_and_update_churn p total_active_balance s
      consolidation_balance = .ok r) : LenStep s r.2 := by
  unfold compute_consolidation_epoch_and_update_churn at h
  len_split h
  all_goals (cases h; exact LenStep.refl _)

/-- `queue_excess_active_balance` keeps the five row lengths. -/
theorem queue_excess_active_balance_len (p : Preset) (s s' : BeaconState)
    (index : ValidatorIndex) (h : queue_excess_active_balance p s index = .ok s') :
    LenStep s s' := by
  unfold queue_excess_active_balance at h
  len_split h
  all_goals (cases h; len_close)

/-- `switch_to_compounding_validator` keeps the five row lengths. -/
theorem switch_to_compounding_validator_len (p : Preset) (s s' : BeaconState)
    (index : ValidatorIndex) (h : switch_to_compounding_validator p s index = .ok s') :
    LenStep s s' := by
  unfold switch_to_compounding_validator at h
  len_split h
  all_goals
    have := queue_excess_active_balance_len p _ _ _ h
    len_close

/-- `process_withdrawal_request` keeps the five row lengths. -/
theorem process_withdrawal_request_len (p : Preset) (s s' : BeaconState)
    (withdrawal_request : WithdrawalRequest)
    (h : process_withdrawal_request p s withdrawal_request = .ok s') : LenStep s s' := by
  unfold process_withdrawal_request at h
  len_split h
  all_goals
    try (cases h)
    try (have := initiate_validator_exit_len p _ _ _ _ ‹initiate_validator_exit _ _ _ _ = _›)
    try (have := compute_exit_epoch_and_update_churn_len p _ _ _ _
                   ‹compute_exit_epoch_and_update_churn _ _ _ _ = _›)
    len_close

set_option maxHeartbeats 1000000 in
/-- `process_consolidation_request` keeps the five row lengths. -/
theorem process_consolidation_request_len (p : Preset) (s s' : BeaconState)
    (consolidation_request : ConsolidationRequest)
    (h : process_consolidation_request p s consolidation_request = .ok s') : LenStep s s' := by
  unfold process_consolidation_request at h
  len_split h
  all_goals
    try (cases h)
    try (have := switch_to_compounding_validator_len p _ _ _
                   ‹switch_to_compounding_validator _ _ _ = _›)
    try (have := compute_consolidation_epoch_and_update_churn_len p _ _ _ _
                   ‹compute_consolidation_epoch_and_update_churn _ _ _ _ = _›)
    len_close

/-- `process_builder_deposit_request` keeps the five row lengths. -/
theorem process_builder_deposit_request_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (request : BuilderDepositRequest)
    (h : process_builder_deposit_request p o s request = .ok s') : LenStep s s' := by
  unfold process_builder_deposit_request at h
  unfold add_builder_to_registry at h
  len_split h
  all_goals (cases h; len_close)

/-- `process_builder_exit_request` keeps the five row lengths. -/
theorem process_builder_exit_request_len (p : Preset) (s s' : BeaconState)
    (request : BuilderExitRequest)
    (h : process_builder_exit_request p s request = .ok s') : LenStep s s' := by
  unfold process_builder_exit_request at h
  unfold initiate_builder_exit at h
  len_split h
  all_goals (cases h; len_close)

/-- `settle_builder_payment` keeps the five row lengths. -/
theorem settle_builder_payment_len (s s' : BeaconState) (payment_index : Uint64)
    (h : settle_builder_payment s payment_index = .ok s') : LenStep s s' := by
  unfold settle_builder_payment at h
  len_split h
  all_goals (cases h; len_close)

/-- `apply_parent_execution_payload` keeps the five row lengths. -/
theorem apply_parent_execution_payload_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (requests : ExecutionRequests)
    (h : apply_parent_execution_payload p o s requests = .ok s') : LenStep s s' := by
  unfold apply_parent_execution_payload at h
  peel_ok h
  all_goals
    cases h
    have h1 := forIn_len
      (by assumption : forIn requests.deposits _ _ = Except.ok _)
      (by loop_body process_deposit_request_len _ _ _)
    have h2 := forIn_len
      (by assumption : forIn requests.withdrawals _ _ = Except.ok _)
      (by loop_body process_withdrawal_request_len p _ _ _)
    have h3 := forIn_len
      (by assumption : forIn requests.consolidations _ _ = Except.ok _)
      (by loop_body process_consolidation_request_len p _ _ _)
    have h4 := forIn_len
      (by assumption : forIn requests.builder_deposits _ _ = Except.ok _)
      (by loop_body process_builder_deposit_request_len p o _ _ _)
    have h5 := forIn_len
      (by assumption : forIn requests.builder_exits _ _ = Except.ok _)
      (by loop_body process_builder_exit_request_len p _ _ _)
    try (have := settle_builder_payment_len _ _ _ ‹settle_builder_payment _ _ = Except.ok _›)
    len_close

/-- `process_parent_execution_payload` keeps the five row lengths. -/
theorem process_parent_execution_payload_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_parent_execution_payload p o s block = .ok s') :
    LenStep s s' := by
  unfold process_parent_execution_payload at h
  len_split h
  all_goals first
    | (cases h; exact LenStep.refl _)
    | exact apply_parent_execution_payload_len p o s s' _ h

/-- `process_block_header` keeps the five row lengths. -/
theorem process_block_header_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_block_header p o s block = .ok s') : LenStep s s' := by
  unfold process_block_header at h
  len_split h
  all_goals (cases h; len_close)

/-- `process_randao` keeps the five row lengths. -/
theorem process_randao_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (body : BeaconBlockBody) (h : process_randao p o s body = .ok s') : LenStep s s' := by
  unfold process_randao at h
  len_split h
  all_goals (cases h; len_close)

/-- `process_eth1_data` keeps the five row lengths. -/
theorem process_eth1_data_len (p : Preset) (s s' : BeaconState) (body : BeaconBlockBody)
    (h : process_eth1_data p s body = .ok s') : LenStep s s' := by
  unfold process_eth1_data at h
  len_split h
  all_goals (cases h; len_close)

/-- `process_execution_payload_bid` keeps the five row lengths. -/
theorem process_execution_payload_bid_len (p : Preset) (o : Oracle)
    (max_blobs_per_block : Epoch → Uint64) (s s' : BeaconState)
    (signed_bid : SignedExecutionPayloadBid)
    (h : process_execution_payload_bid p o max_blobs_per_block s signed_bid = .ok s') :
    LenStep s s' := by
  unfold process_execution_payload_bid at h
  peel_ok h
  all_goals (cases h; len_close)

/-- `apply_withdrawals` keeps the five row lengths. -/
theorem apply_withdrawals_len :
    ∀ (withdrawals : List Withdrawal) (s s' : BeaconState),
      apply_withdrawals s withdrawals = .ok s' → LenStep s s'
  | [], s, s', h => by
    cases h
    exact LenStep.refl s
  | w :: rest, s, s', h => by
    unfold apply_withdrawals at h
    len_split h
    all_goals
      have := apply_withdrawals_len rest _ _ h
      len_close

/-- `update_next_withdrawal_index` keeps the five row lengths. -/
theorem update_next_withdrawal_index_len (s s' : BeaconState) (withdrawals : List Withdrawal)
    (h : update_next_withdrawal_index s withdrawals = .ok s') : LenStep s s' := by
  unfold update_next_withdrawal_index at h
  len_split h
  all_goals (cases h; len_close)

/-- `update_next_withdrawal_builder_index` keeps the five row lengths. -/
theorem update_next_withdrawal_builder_index_len (s s' : BeaconState) (count : Uint64)
    (h : update_next_withdrawal_builder_index s count = .ok s') : LenStep s s' := by
  unfold update_next_withdrawal_builder_index at h
  len_split h
  all_goals (cases h; len_close)

/-- `update_next_withdrawal_validator_index` keeps the five row lengths. -/
theorem update_next_withdrawal_validator_index_len (p : Preset) (s s' : BeaconState)
    (withdrawals : List Withdrawal)
    (h : update_next_withdrawal_validator_index p s withdrawals = .ok s') : LenStep s s' := by
  unfold update_next_withdrawal_validator_index at h
  len_split h
  all_goals (cases h; len_close)

/-- `process_withdrawals` keeps the five row lengths. -/
theorem process_withdrawals_len (p : Preset) (s s' : BeaconState)
    (h : process_withdrawals p s = .ok s') : LenStep s s' := by
  unfold process_withdrawals at h
  len_split h
  all_goals
    try (cases h)
    try (have := apply_withdrawals_len _ _ _ ‹apply_withdrawals _ _ = _›)
    try (have := update_next_withdrawal_index_len _ _ _ ‹update_next_withdrawal_index _ _ = _›)
    try (have := update_next_withdrawal_builder_index_len _ _ _
                   ‹update_next_withdrawal_builder_index _ _ = _›)
    try (have := update_next_withdrawal_validator_index_len p _ _ _
                   ‹update_next_withdrawal_validator_index _ _ _ = _›)
    try simp only [update_payload_expected_withdrawals, update_builder_pending_withdrawals,
      update_pending_partial_withdrawals] at *
    len_close

/-- The sync aggregate loop keeps the length of the balance list. -/
theorem process_sync_aggregate_loop_len (p : Preset) (state : BeaconState)
    (participant_reward proposer_reward : Gwei) :
    ∀ (pairs : List (ValidatorIndex × Bool)) (balances balances' : List Gwei),
      process_sync_aggregate_loop p state participant_reward proposer_reward pairs balances =
        .ok balances' → balances'.length = balances.length
  | [], balances, balances', h => by
    cases h
    rfl
  | (i, bit) :: rest, balances, balances', h => by
    unfold process_sync_aggregate_loop at h
    len_split h
    all_goals
      rw [process_sync_aggregate_loop_len p state participant_reward proposer_reward rest _
        balances' h]
      simp only [List.length_set]

/-- `process_sync_aggregate` keeps the five row lengths. -/
theorem process_sync_aggregate_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (sync_aggregate : SyncAggregate)
    (h : process_sync_aggregate p o s sync_aggregate = .ok s') : LenStep s s' := by
  obtain ⟨_, _, _, balances, -, -, hloop, rfl⟩ := process_sync_aggregate_shape p o s s' _ h
  exact ⟨rfl, process_sync_aggregate_loop_len p s _ _ _ _ _ hloop, rfl, rfl, rfl⟩

/-- `process_block` keeps the five row lengths. -/
theorem process_block_len (p : Preset) (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState) (block : BeaconBlock)
    (h : process_block p o max_blobs_per_block GLOAS_FORK_EPOCH s block = .ok s') :
    LenStep s s' := by
  unfold process_block at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  obtain ⟨s2, h2, h⟩ := specM_bind_ok h
  obtain ⟨s3, h3, h⟩ := specM_bind_ok h
  obtain ⟨s4, h4, h⟩ := specM_bind_ok h
  obtain ⟨s5, h5, h⟩ := specM_bind_ok h
  obtain ⟨s6, h6, h⟩ := specM_bind_ok h
  obtain ⟨s7, h7, h⟩ := specM_bind_ok h
  exact (process_parent_execution_payload_len p o _ _ block h1).trans
    ((process_block_header_len p o _ _ block h2).trans
    ((process_withdrawals_len p _ _ h3).trans
    ((process_execution_payload_bid_len p o max_blobs_per_block _ _ _ h4).trans
    ((process_randao_len p o _ _ _ h5).trans
    ((process_eth1_data_len p _ _ _ h6).trans
    ((process_operations_len p o GLOAS_FORK_EPOCH _ _ _ _ h7).trans
    (process_sync_aggregate_len p o _ _ _ h)))))))

/-! ## Slots and epoch steps -/

/-- `process_slot` keeps the five row lengths. -/
theorem process_slot_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_slot p o s = .ok s') : LenStep s s' := by
  unfold process_slot at h
  len_split h
  all_goals (cases h; len_close)

/-- `weigh_justification_and_finalization` keeps the five row lengths. -/
theorem weigh_justification_and_finalization_len (p : Preset) (s s' : BeaconState)
    (total_active_balance previous_epoch_target_balance current_epoch_target_balance : Gwei)
    (h : weigh_justification_and_finalization p s total_active_balance
      previous_epoch_target_balance current_epoch_target_balance = .ok s') : LenStep s s' := by
  unfold weigh_justification_and_finalization at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨t1, ht1, h⟩ := specM_bind_ok h
  have q1 : LenStep s t1 := by
    len_split ht1
    all_goals (cases ht1; len_close)
  obtain ⟨t2, ht2, h⟩ := specM_bind_ok h
  have q2 : LenStep s t2 := by
    len_split ht2
    all_goals (cases ht2; len_close)
  obtain ⟨t3, ht3, h⟩ := specM_bind_ok h
  have q3 : LenStep s t3 := by
    len_split ht3
    all_goals (cases ht3; len_close)
  obtain ⟨t4, ht4, h⟩ := specM_bind_ok h
  have q4 : LenStep s t4 := by
    len_split ht4
    all_goals (cases ht4; len_close)
  obtain ⟨t5, ht5, h⟩ := specM_bind_ok h
  have q5 : LenStep s t5 := by
    len_split ht5
    all_goals (cases ht5; len_close)
  len_split h
  all_goals (cases h; len_close)

/-- `process_justification_and_finalization` keeps the five row lengths. -/
theorem process_justification_and_finalization_len (p : Preset) (s s' : BeaconState)
    (h : process_justification_and_finalization p s = .ok s') : LenStep s s' := by
  unfold process_justification_and_finalization at h
  len_split h
  all_goals first
    | (cases h; exact LenStep.refl _)
    | exact weigh_justification_and_finalization_len p _ _ _ _ _ h

/-- A `for` loop over a list keeps the length of the list when each pass keeps it. -/
private theorem forIn_list_len {α β : Type} {f : α → List β → SpecM (ForInStep (List β))}
    {l : List α} {acc acc' : List β} (h : forIn l acc f = .ok acc')
    (hf : ∀ a b r, f a b = .ok r → r.value.length = b.length) : acc'.length = acc.length :=
  forIn_ok_invariant (fun b : List β => b.length = acc.length) f
    (fun a b r hb hr => (hf a b r hr).trans hb) l acc acc' rfl h

/-- `process_inactivity_updates` keeps the five row lengths. -/
theorem process_inactivity_updates_len (p : Preset) (s s' : BeaconState)
    (h : process_inactivity_updates p s = .ok s') : LenStep s s' := by
  unfold process_inactivity_updates at h
  len_split h
  all_goals
    cases h
    try (have := forIn_list_len ‹forIn _ s.inactivity_scores _ = _› (by
      intro _ _ _ hb
      len_split hb
      all_goals (cases hb; simp only [ForInStep.value, List.length_set])))
    len_close

/-- `process_rewards_and_penalties` keeps the five row lengths. -/
theorem process_rewards_and_penalties_len (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_rewards_and_penalties p total_active_balance s = .ok s') :
    LenStep s s' := by
  unfold process_rewards_and_penalties at h
  len_split h
  all_goals
    cases h
    try (have := forIn_list_len ‹forIn _ s.balances _ = _› (by
      intro _ _ _ hb
      len_split hb
      all_goals
        cases hb
        have := forIn_list_len ‹forIn _ _ _ = _› (by
          intro _ _ _ hb'
          len_split hb'
          all_goals (cases hb'; simp only [ForInStep.value, List.length_set]))
        simp only [ForInStep.value] at *
        exact this))
    len_close

/-- `process_registry_updates` keeps the five row lengths. -/
theorem process_registry_updates_len (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_registry_updates p total_active_balance s = .ok s') :
    LenStep s s' := by
  unfold process_registry_updates at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨r, hr, h⟩ := specM_bind_ok h
  cases h
  refine forIn_len hr ?_
  intro _ _ _ hb
  len_split hb
  all_goals
    cases hb
    try (have := initiate_validator_exit_len p _ _ _ _ ‹initiate_validator_exit _ _ _ _ = _›)
    len_close

/-- `process_slashings` keeps the five row lengths. -/
theorem process_slashings_len (p : Preset) (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_slashings p total_active_balance s = .ok s') : LenStep s s' := by
  unfold process_slashings at h
  len_split h
  all_goals
    cases h
    have := forIn_list_len ‹forIn _ s.balances _ = _› (by
      intro _ _ _ hb
      len_split hb
      all_goals (cases hb; simp only [ForInStep.value, List.length_set]))
    len_close

/-- `process_eth1_data_reset` keeps the five row lengths. -/
theorem process_eth1_data_reset_len (p : Preset) (s s' : BeaconState)
    (h : process_eth1_data_reset p s = .ok s') : LenStep s s' := by
  unfold process_eth1_data_reset at h
  len_split h
  all_goals (cases h; len_close)

/-- `process_pending_consolidations` keeps the five row lengths. -/
theorem process_pending_consolidations_len (p : Preset) (s s' : BeaconState)
    (h : process_pending_consolidations p s = .ok s') : LenStep s s' := by
  unfold process_pending_consolidations at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := forIn_ok_invariant (fun r : MProd Nat BeaconState => LenStep s r.snd) _ ?_
    _ _ _ (LenStep.refl s) hr
  · cases h
    exact hP
  · intro _ ⟨_, _⟩ r hst hb
    len_split hb
    all_goals
      cases hb
      try simp only at hst
      len_close

/-- `process_builder_pending_payments` keeps the five row lengths. -/
theorem process_builder_pending_payments_len (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_builder_pending_payments p total_active_balance s = .ok s') :
    LenStep s s' := by
  unfold process_builder_pending_payments at h
  len_split h
  all_goals (cases h; len_close)

/-- `process_effective_balance_updates` keeps the five row lengths. -/
theorem process_effective_balance_updates_len (p : Preset) (s s' : BeaconState)
    (h : process_effective_balance_updates p s = .ok s') : LenStep s s' := by
  rw [process_effective_balance_updates_eq] at h
  cases hm : s.validators.zipIdx.mapM (effectiveBalanceStep p s.balances) with
  | error e => simp [hm, Except.map] at h
  | ok validators =>
    simp only [hm, Except.map, Except.ok.injEq] at h
    subst h
    obtain ⟨hlen, -⟩ := mapM_ok_getElem _ _ _ hm
    rw [List.length_zipIdx] at hlen
    exact ⟨hlen, rfl, rfl, rfl, rfl⟩

/-- `process_slashings_reset` keeps the five row lengths. -/
theorem process_slashings_reset_len (p : Preset) (s s' : BeaconState)
    (h : process_slashings_reset p s = .ok s') : LenStep s s' := by
  unfold process_slashings_reset at h
  len_split h
  all_goals (cases h; len_close)

/-- `process_randao_mixes_reset` keeps the five row lengths. -/
theorem process_randao_mixes_reset_len (p : Preset) (s s' : BeaconState)
    (h : process_randao_mixes_reset p s = .ok s') : LenStep s s' := by
  unfold process_randao_mixes_reset at h
  len_split h
  all_goals (cases h; len_close)

/-- `process_historical_summaries_update` keeps the five row lengths. -/
theorem process_historical_summaries_update_len (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_historical_summaries_update p o s = .ok s') : LenStep s s' := by
  unfold process_historical_summaries_update at h
  len_split h
  all_goals (cases h; len_close)

/-- `set_or_append_list` at index `len(list)` appends one item. -/
theorem set_or_append_list_len_eq {α : Type} {l l' : List α} {a : α}
    (h : set_or_append_list l l.length a = .ok l') : l'.length = l.length + 1 := by
  unfold set_or_append_list at h
  simp only [beq_self_eq_true, if_true, pure, Except.pure, Except.ok.injEq] at h
  subst h
  exact List.length_append

/-- With `RowsOk`, `add_validator_to_registry` appends one row to each list. -/
theorem add_validator_to_registry_rowsOk (p : Preset) (s s' : BeaconState) (pubkey : BLSPubkey)
    (withdrawal_credentials : Bytes32) (amount : Gwei)
    (h : add_validator_to_registry p s pubkey withdrawal_credentials amount = .ok s')
    (hs : RowsOk s) : RowsOk s' := by
  obtain ⟨r1, r2, r3, r4⟩ := hs
  unfold add_validator_to_registry get_index_for_new_validator at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨l1, h1, h⟩ := specM_bind_ok h
  obtain ⟨l2, h2, h⟩ := specM_bind_ok h
  obtain ⟨l3, h3, h⟩ := specM_bind_ok h
  obtain ⟨l4, h4, h⟩ := specM_bind_ok h
  obtain ⟨l5, h5, h⟩ := specM_bind_ok h
  cases h
  simp only at h2 h3 h4 h5
  rw [← r1] at h2
  rw [← r3] at h3
  rw [← r4] at h4
  rw [← r2] at h5
  have e1 := set_or_append_list_len_eq h1
  have e2 := set_or_append_list_len_eq h2
  have e3 := set_or_append_list_len_eq h3
  have e4 := set_or_append_list_len_eq h4
  have e5 := set_or_append_list_len_eq h5
  refine ⟨?_, ?_, ?_, ?_⟩ <;> simp only <;> omega

/-- `apply_pending_deposit` keeps `RowsOk`. -/
theorem apply_pending_deposit_rowsOk (p : Preset) (o : Oracle) (s s' : BeaconState)
    (deposit : PendingDeposit) (h : apply_pending_deposit p o s deposit = .ok s')
    (hs : RowsOk s) : RowsOk s' := by
  unfold apply_pending_deposit at h
  len_split h
  all_goals first
    | exact add_validator_to_registry_rowsOk p s s' _ _ _ h hs
    | (cases h
       refine LenStep.rowsOk ?_ hs
       len_close)

/-- `process_pending_deposits` with `apply_pending_deposit` keeps `RowsOk`. -/
theorem process_pending_deposits_rowsOk (p : Preset) (o : Oracle)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_pending_deposits p total_active_balance (apply_pending_deposit p o) s = .ok s')
    (hs : RowsOk s) : RowsOk s' := by
  unfold process_pending_deposits at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, _, _, _, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := forIn_ok_invariant
    (fun r : MProd (List PendingDeposit) (MProd Bool (MProd Uint64 (MProd Uint64 BeaconState))) =>
      RowsOk r.2.2.2.2) _ ?_ _ _ _ hs hr
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
         exact apply_pending_deposit_rowsOk p o _ _ _ ‹apply_pending_deposit _ _ _ _ = _› hst)

/-- `process_participation_flag_updates` keeps `RowsOk`. -/
theorem process_participation_flag_updates_rowsOk (s : BeaconState) (hs : RowsOk s) :
    RowsOk (process_participation_flag_updates s) := by
  obtain ⟨r1, r2, r3, r4⟩ := hs
  exact ⟨r1, r2, r4, List.length_replicate⟩

/-- `process_epoch` keeps `RowsOk`. -/
theorem process_epoch_rowsOk (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_epoch p o s = .ok s') (hs : RowsOk s) : RowsOk s' := by
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
  have e6 : RowsOk s6 :=
    ((process_justification_and_finalization_len p _ _ h1).trans
    ((process_inactivity_updates_len p _ _ h2).trans
    ((process_rewards_and_penalties_len p _ _ _ h3).trans
    ((process_registry_updates_len p _ _ _ h4).trans
    ((process_slashings_len p _ _ _ h5).trans
    (process_eth1_data_reset_len p _ _ h6)))))).rowsOk hs
  have e7 := process_pending_deposits_rowsOk p o _ _ _ h7 e6
  have e13 : RowsOk s13 :=
    ((process_pending_consolidations_len p _ _ h8).trans
    ((process_builder_pending_payments_len p _ _ _ h9).trans
    ((process_effective_balance_updates_len p _ _ h10).trans
    ((process_slashings_reset_len p _ _ h11).trans
    ((process_randao_mixes_reset_len p _ _ h12).trans
    (process_historical_summaries_update_len p o _ _ h13)))))).rowsOk e7
  have e := process_participation_flag_updates_rowsOk _ e13
  obtain ⟨_, _, rfl⟩ := process_sync_committee_updates_shape p o _ _ h14
  obtain ⟨_, rfl⟩ := process_proposer_lookahead_shape p o _ _ h15
  obtain ⟨_, rfl⟩ := process_ptc_window_shape p o _ _ h
  exact e

/-- `process_slots` with the real `process_epoch` keeps `RowsOk`. -/
theorem process_slots_rowsOk (p : Preset) (o : Oracle) (s s' : BeaconState) (slot : Slot)
    (h : process_slots p o (process_epoch p o) s slot = .ok s') (hs : RowsOk s) :
    RowsOk s' :=
  process_slots_invariant p o (process_epoch p o) RowsOk
    (fun _ _ h hP => (process_slot_len p o _ _ h).rowsOk hP)
    (fun _ _ h hP => process_epoch_rowsOk p o _ _ h hP)
    (fun _ _ hP => hP) s s' slot hs h

/-- `state_transition` keeps `RowsOk`. -/
theorem state_transition_rowsOk (p : Preset) (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState) (signed_block : SignedBeaconBlock)
    (validate_result : Bool) (hs : RowsOk s)
    (h : state_transition p o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
      validate_result = .ok s') :
    RowsOk s' := by
  unfold state_transition at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h
       exact (process_block_len p o max_blobs_per_block GLOAS_FORK_EPOCH _ _ _
         ‹process_block _ _ _ _ _ _ = _›).rowsOk
         (process_slots_rowsOk p o _ _ _ ‹process_slots _ _ _ _ _ = _› hs))

end EpochProofs.Spec
