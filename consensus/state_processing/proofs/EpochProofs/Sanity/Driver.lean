import EpochProofs.Spec.Block.Block
import EpochProofs.Sanity.JustificationFinalization
import EpochProofs.Sanity.EpochCommittees
import EpochProofs.Sanity.Block.Attestations
import EpochProofs.Sanity.Block.Slashings
import EpochProofs.Sanity.Block.Exits
import EpochProofs.Sanity.Block.Withdrawals
import EpochProofs.Sanity.Block.ParentPayload
import EpochProofs.Sanity.Block.SyncAggregate

/-!
# The state transition keeps the FFG invariant

Each epoch step after `process_justification_and_finalization` keeps the slot and the
checkpoints. So `process_epoch` keeps the slot and turns `FfgInvariant` into
`FfgInvariantAfter`. Each block step keeps the slot and the checkpoints. So `state_transition`
keeps `FfgInvariant`.
-/

namespace EpochProofs.Spec

/-- Splits the binds, `if`s and `match`es of a step that does not write the slot or the
checkpoints, and closes each success branch by `rfl`. -/
local macro "cs_tac" h:ident : tactic => `(tactic| (
  simp only [bind, Except.bind, pure, Except.pure] at $h:ident
  repeat' split at $h:ident
  all_goals first
    | contradiction
    | (cases $h:ident; exact ⟨rfl, rfl, rfl, rfl⟩)))

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

/-- A `List.foldlM` in `SpecM` keeps the slot and the checkpoints when each step keeps them. -/
private theorem foldlM_checkpointsStable {α : Type} (f : BeaconState → α → SpecM BeaconState)
    (hf : ∀ s a s', f s a = .ok s' → CheckpointsStable s s') :
    ∀ (l : List α) (s s' : BeaconState), l.foldlM f s = .ok s' → CheckpointsStable s s' := by
  intro l
  induction l with
  | nil => intro s s' h; cases h; exact CheckpointsStable.refl s
  | cons a as ih =>
    intro s s' h
    rw [List.foldlM_cons] at h
    obtain ⟨s1, h1, h⟩ := specM_bind_ok h
    exact (hf s a s1 h1).trans (ih s1 s' h)

/-! ## Epoch steps -/

/-- `process_inactivity_updates` keeps the slot and the checkpoints. -/
theorem process_inactivity_updates_checkpointsStable (p : Preset) (s s' : BeaconState)
    (h : process_inactivity_updates p s = .ok s') : CheckpointsStable s s' := by
  unfold process_inactivity_updates at h
  cs_tac h

/-- `process_rewards_and_penalties` keeps the slot and the checkpoints. -/
theorem process_rewards_and_penalties_checkpointsStable (p : Preset)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_rewards_and_penalties p total_active_balance s = .ok s') :
    CheckpointsStable s s' := by
  unfold process_rewards_and_penalties at h
  cs_tac h

/-- `compute_exit_epoch_and_update_churn` keeps the slot and the checkpoints. -/
theorem compute_exit_epoch_and_update_churn_checkpointsStable (p : Preset)
    (total_active_balance : Gwei) (s : BeaconState) (exit_balance : Gwei)
    (r : Epoch × BeaconState)
    (h : compute_exit_epoch_and_update_churn p total_active_balance s exit_balance = .ok r) :
    CheckpointsStable s r.2 := by
  unfold compute_exit_epoch_and_update_churn at h
  cs_tac h

/-- `initiate_validator_exit` keeps the slot and the checkpoints. -/
theorem initiate_validator_exit_checkpointsStable (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (index : ValidatorIndex)
    (h : initiate_validator_exit p total_active_balance s index = .ok s') :
    CheckpointsStable s s' := by
  unfold initiate_validator_exit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨rfl, rfl, rfl, rfl⟩)
    | (cases h
       exact compute_exit_epoch_and_update_churn_checkpointsStable p total_active_balance s _ _
         ‹_ = Except.ok _›)

/-- `process_registry_updates` keeps the slot and the checkpoints. -/
theorem process_registry_updates_checkpointsStable (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_registry_updates p total_active_balance s = .ok s') :
    CheckpointsStable s s' := by
  unfold process_registry_updates at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨r, hr, h⟩ := specM_bind_ok h
  cases h
  refine forIn_ok_invariant (CheckpointsStable s) _ ?_ _ s _ (CheckpointsStable.refl s) hr
  intro index st r hst hb
  simp only [bind, Except.bind, pure, Except.pure] at hb
  repeat' split at hb
  all_goals first
    | contradiction
    | (cases hb; exact hst)
    | (cases hb
       exact hst.trans (initiate_validator_exit_checkpointsStable p total_active_balance _ _ _
         ‹initiate_validator_exit _ _ _ _ = _›))

/-- `process_slashings` keeps the slot and the checkpoints. -/
theorem process_slashings_checkpointsStable (p : Preset) (total_active_balance : Gwei)
    (s s' : BeaconState) (h : process_slashings p total_active_balance s = .ok s') :
    CheckpointsStable s s' := by
  unfold process_slashings at h
  cs_tac h

/-- `process_eth1_data_reset` keeps the slot and the checkpoints. -/
theorem process_eth1_data_reset_checkpointsStable (p : Preset) (s s' : BeaconState)
    (h : process_eth1_data_reset p s = .ok s') : CheckpointsStable s s' := by
  unfold process_eth1_data_reset at h
  cs_tac h

/-- `add_validator_to_registry` keeps the slot and the checkpoints. -/
theorem add_validator_to_registry_checkpointsStable (p : Preset) (s s' : BeaconState)
    (pubkey : BLSPubkey) (withdrawal_credentials : Bytes32) (amount : Gwei)
    (h : add_validator_to_registry p s pubkey withdrawal_credentials amount = .ok s') :
    CheckpointsStable s s' := by
  unfold add_validator_to_registry at h
  cs_tac h

/-- `apply_pending_deposit` keeps the slot and the checkpoints. -/
theorem apply_pending_deposit_checkpointsStable (p : Preset) (o : Oracle) (s s' : BeaconState)
    (deposit : PendingDeposit) (h : apply_pending_deposit p o s deposit = .ok s') :
    CheckpointsStable s s' := by
  unfold apply_pending_deposit at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨rfl, rfl, rfl, rfl⟩)
    | exact add_validator_to_registry_checkpointsStable p s s' _ _ _ h

/-- `process_pending_deposits` with `apply_pending_deposit` keeps the slot and the
checkpoints. -/
theorem process_pending_deposits_checkpointsStable (p : Preset) (o : Oracle)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_pending_deposits p total_active_balance (apply_pending_deposit p o) s = .ok s') :
    CheckpointsStable s s' := by
  unfold process_pending_deposits at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, _, _, _, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := forIn_ok_invariant
    (fun r : MProd (List PendingDeposit) (MProd Bool (MProd Uint64 (MProd Uint64 BeaconState))) =>
      CheckpointsStable s r.2.2.2.2) _ ?_ _ _ _ (CheckpointsStable.refl s) hr
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
         exact hst.trans (apply_pending_deposit_checkpointsStable p o _ _ _
           ‹apply_pending_deposit _ _ _ _ = _›))

/-- `process_pending_consolidations` keeps the slot and the checkpoints. -/
theorem process_pending_consolidations_checkpointsStable (p : Preset) (s s' : BeaconState)
    (h : process_pending_consolidations p s = .ok s') : CheckpointsStable s s' := by
  unfold process_pending_consolidations at h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨_, -, h⟩ := specM_bind_ok h
  obtain ⟨⟨_, st⟩, hr, h⟩ := specM_bind_ok h
  have hP := forIn_ok_invariant (fun r : MProd Nat BeaconState => CheckpointsStable s r.snd) _ ?_
    _ _ _ (CheckpointsStable.refl s) hr
  · cases h
    exact hP
  · intro _ ⟨_, _⟩ r hst hb
    simp only [bind, Except.bind, pure, Except.pure] at hb
    repeat' split at hb
    all_goals first
      | contradiction
      | (cases hb; exact hst)

/-- `process_builder_pending_payments` keeps the slot and the checkpoints. -/
theorem process_builder_pending_payments_checkpointsStable (p : Preset)
    (total_active_balance : Gwei) (s s' : BeaconState)
    (h : process_builder_pending_payments p total_active_balance s = .ok s') :
    CheckpointsStable s s' := by
  unfold process_builder_pending_payments at h
  cs_tac h

/-- `process_effective_balance_updates` keeps the slot and the checkpoints. -/
theorem process_effective_balance_updates_checkpointsStable (p : Preset) (s s' : BeaconState)
    (h : process_effective_balance_updates p s = .ok s') : CheckpointsStable s s' := by
  unfold process_effective_balance_updates at h
  cs_tac h

/-- `process_slashings_reset` keeps the slot and the checkpoints. -/
theorem process_slashings_reset_checkpointsStable (p : Preset) (s s' : BeaconState)
    (h : process_slashings_reset p s = .ok s') : CheckpointsStable s s' := by
  unfold process_slashings_reset at h
  cs_tac h

/-- `process_randao_mixes_reset` keeps the slot and the checkpoints. -/
theorem process_randao_mixes_reset_checkpointsStable (p : Preset) (s s' : BeaconState)
    (h : process_randao_mixes_reset p s = .ok s') : CheckpointsStable s s' := by
  unfold process_randao_mixes_reset at h
  cs_tac h

/-- `process_historical_summaries_update` keeps the slot and the checkpoints. -/
theorem process_historical_summaries_update_checkpointsStable (p : Preset) (o : Oracle)
    (s s' : BeaconState) (h : process_historical_summaries_update p o s = .ok s') :
    CheckpointsStable s s' := by
  unfold process_historical_summaries_update at h
  cs_tac h

/-- `process_participation_flag_updates` keeps the slot and the checkpoints. -/
theorem process_participation_flag_updates_checkpointsStable (s : BeaconState) :
    CheckpointsStable s (process_participation_flag_updates s) :=
  ⟨rfl, rfl, rfl, rfl⟩

/-! ## `process_epoch` -/

/-- After `process_justification_and_finalization`, the other steps of `process_epoch` keep
the slot and the checkpoints. -/
theorem process_epoch_split (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_epoch p o s = .ok s') :
    ∃ s1, process_justification_and_finalization p s = .ok s1 ∧ CheckpointsStable s1 s' := by
  unfold process_epoch at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  refine ⟨s1, h1, ?_⟩
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
  refine (process_inactivity_updates_checkpointsStable p _ _ h2).trans
    ((process_rewards_and_penalties_checkpointsStable p _ _ _ h3).trans
    ((process_registry_updates_checkpointsStable p _ _ _ h4).trans
    ((process_slashings_checkpointsStable p _ _ _ h5).trans
    ((process_eth1_data_reset_checkpointsStable p _ _ h6).trans
    ((process_pending_deposits_checkpointsStable p o _ _ _ h7).trans
    ((process_pending_consolidations_checkpointsStable p _ _ h8).trans
    ((process_builder_pending_payments_checkpointsStable p _ _ _ h9).trans
    ((process_effective_balance_updates_checkpointsStable p _ _ h10).trans
    ((process_slashings_reset_checkpointsStable p _ _ h11).trans
    ((process_randao_mixes_reset_checkpointsStable p _ _ h12).trans
    ((process_historical_summaries_update_checkpointsStable p o _ _ h13).trans
    ((process_participation_flag_updates_checkpointsStable _).trans
    ((process_sync_committee_updates_frame p o _ _ h14).2.trans
    ((process_proposer_lookahead_frame p o _ _ h15).2.trans
    (process_ptc_window_frame p o _ _ h).2))))))))))))))

/-- `process_epoch` keeps the slot. -/
theorem process_epoch_slot (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_epoch p o s = .ok s') : s'.slot = s.slot := by
  obtain ⟨s1, h1, hs⟩ := process_epoch_split p o s s' h
  exact hs.1.trans (process_justification_and_finalization_frame p s s1 h1).2

/-- `process_epoch` turns `FfgInvariant` into `FfgInvariantAfter` and keeps the slot. -/
theorem process_epoch_ffg (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_epoch p o s = .ok s') (hinv : FfgInvariant p s) :
    FfgInvariantAfter p s' ∧ s'.slot = s.slot := by
  obtain ⟨s1, h1, hs⟩ := process_epoch_split p o s s' h
  exact ⟨(process_justification_and_finalization_ffg p s s1 h1 hinv).1.of_checkpointsStable hs,
    process_epoch_slot p o s s' h⟩

/-- `process_slots` with the real `process_epoch` ends at the target slot. -/
theorem process_slots_process_epoch_slot (p : Preset) (o : Oracle) (s s' : BeaconState)
    (slot : Slot) (h : process_slots p o (process_epoch p o) s slot = .ok s') :
    s'.slot = slot :=
  process_slots_slot p o (process_epoch p o) (process_epoch_slot p o) s s' slot h

/-- `process_slots` with the real `process_epoch` keeps `FfgInvariant`. -/
theorem process_slots_process_epoch_ffg (p : Preset) (o : Oracle) (s s' : BeaconState)
    (slot : Slot) (hinv : FfgInvariant p s)
    (h : process_slots p o (process_epoch p o) s slot = .ok s') : FfgInvariant p s' :=
  process_slots_ffg p o (process_epoch p o) (fun s s' hs hP => process_epoch_ffg p o s s' hs hP)
    s s' slot hinv h

/-! ## Block steps -/

/-- `process_block_header` keeps the slot and the checkpoints. -/
theorem process_block_header_checkpointsStable (p : Preset) (o : Oracle) (s s' : BeaconState)
    (block : BeaconBlock) (h : process_block_header p o s block = .ok s') :
    CheckpointsStable s s' := by
  unfold process_block_header at h
  simp only [throw, throwThe, MonadExceptOf.throw] at h
  cs_tac h

/-- `process_randao` keeps the slot and the checkpoints. -/
theorem process_randao_checkpointsStable (p : Preset) (o : Oracle) (s s' : BeaconState)
    (body : BeaconBlockBody) (h : process_randao p o s body = .ok s') :
    CheckpointsStable s s' := by
  unfold process_randao at h
  cs_tac h

/-- `process_eth1_data` keeps the slot and the checkpoints. -/
theorem process_eth1_data_checkpointsStable (p : Preset) (s s' : BeaconState)
    (body : BeaconBlockBody) (h : process_eth1_data p s body = .ok s') :
    CheckpointsStable s s' := by
  unfold process_eth1_data at h
  cs_tac h

/-- `process_operations` keeps the slot and the checkpoints. -/
theorem process_operations_checkpointsStable (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (s s' : BeaconState) (body : BeaconBlockBody) (parent_slot : Slot)
    (h : process_operations p o GLOAS_FORK_EPOCH s body parent_slot = .ok s') :
    CheckpointsStable s s' := by
  unfold process_operations at h
  simp only [throw, throwThe, MonadExceptOf.throw, bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals try contradiction
  have h1 := foldlM_checkpointsStable _
    (fun s a s' h => (process_proposer_slashing_effects p o s s' a h).2.1) _ _ _
    ‹List.foldlM (process_proposer_slashing p o) _ _ = _›
  have h2 := foldlM_checkpointsStable _
    (fun s a s' h => (process_attester_slashing_effects p o s s' a h).2.1) _ _ _
    ‹List.foldlM (process_attester_slashing p o) _ _ = _›
  have h3 := foldlM_checkpointsStable _
    (fun s a s' h => process_attestation_checkpointsStable p o s s' a parent_slot h) _ _ _
    ‹List.foldlM (fun state attestation => process_attestation p o state attestation _) _ _ = _›
  have h4 := foldlM_checkpointsStable _
    (fun s a s' h => (process_voluntary_exit_effects p o s s' a h).2.1) _ _ _
    ‹List.foldlM (process_voluntary_exit p o) _ _ = _›
  have h5 := foldlM_checkpointsStable _
    (fun s a s' h => (process_bls_to_execution_change_effects p o s s' a h).2.1) _ _ _
    ‹List.foldlM (process_bls_to_execution_change p o) _ _ = _›
  have h6 := foldlM_checkpointsStable _
    (fun s a s' h => process_payload_attestation_checkpointsStable p o GLOAS_FORK_EPOCH s s' a h)
    _ _ _ h
  exact h1.trans (h2.trans (h3.trans (h4.trans (h5.trans h6))))

/-- `process_block` keeps the slot and the checkpoints. -/
theorem process_block_checkpointsStable (p : Preset) (o : Oracle)
    (max_blobs_per_block : Epoch → Uint64) (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState)
    (block : BeaconBlock)
    (h : process_block p o max_blobs_per_block GLOAS_FORK_EPOCH s block = .ok s') :
    CheckpointsStable s s' := by
  unfold process_block at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  obtain ⟨s2, h2, h⟩ := specM_bind_ok h
  obtain ⟨s3, h3, h⟩ := specM_bind_ok h
  obtain ⟨s4, h4, h⟩ := specM_bind_ok h
  obtain ⟨s5, h5, h⟩ := specM_bind_ok h
  obtain ⟨s6, h6, h⟩ := specM_bind_ok h
  obtain ⟨s7, h7, h⟩ := specM_bind_ok h
  exact (process_parent_execution_payload_stable p o _ _ block h1).2.trans
    ((process_block_header_checkpointsStable p o _ _ block h2).trans
    ((process_withdrawals_checkpointsStable p _ _ h3).trans
    ((process_execution_payload_bid_stable p o max_blobs_per_block _ _ _ h4).2.2.1.trans
    ((process_randao_checkpointsStable p o _ _ _ h5).trans
    ((process_eth1_data_checkpointsStable p _ _ _ h6).trans
    ((process_operations_checkpointsStable p o GLOAS_FORK_EPOCH _ _ _ _ h7).trans
    (process_sync_aggregate_checkpointsStable p o _ _ _ h)))))))

/-! ## `state_transition` -/

/-- `state_transition` keeps `FfgInvariant`. -/
theorem state_transition_ffg (p : Preset) (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState) (signed_block : SignedBeaconBlock)
    (validate_result : Bool) (hinv : FfgInvariant p s)
    (h : state_transition p o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
      validate_result = .ok s') :
    FfgInvariant p s' := by
  unfold state_transition at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h
       exact (process_slots_process_epoch_ffg p o _ _ _ hinv
         ‹process_slots _ _ _ _ _ = _›).of_checkpointsStable
         (process_block_checkpointsStable p o max_blobs_per_block GLOAS_FORK_EPOCH _ _ _
           ‹process_block _ _ _ _ _ _ = _›))

end EpochProofs.Spec
