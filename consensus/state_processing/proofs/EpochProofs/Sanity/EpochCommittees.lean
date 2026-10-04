import EpochProofs.Sanity.Effects
import EpochProofs.Spec.EpochCommittees

/-!
# Effects of the sync committee, proposer lookahead and PTC window updates

Each step writes only its own fields. So it keeps `validators`, `balances`,
`inactivity_scores`, the slot and the checkpoints.
-/

namespace EpochProofs.Spec

/-- `process_sync_committee_updates` writes only the two sync committees. -/
theorem process_sync_committee_updates_shape (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_sync_committee_updates p o s = .ok s') :
    ∃ current next : SyncCommittee,
      s' = { s with current_sync_committee := current, next_sync_committee := next } := by
  unfold process_sync_committee_updates at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨_, _, rfl⟩)
    | (cases h; exact ⟨s.current_sync_committee, s.next_sync_committee, rfl⟩)

/-- `process_proposer_lookahead` writes only `proposer_lookahead`. -/
theorem process_proposer_lookahead_shape (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_proposer_lookahead p o s = .ok s') :
    ∃ proposer_lookahead : List ValidatorIndex, s' = { s with proposer_lookahead } := by
  unfold process_proposer_lookahead at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨_, rfl⟩)

/-- `process_ptc_window` writes only `ptc_window`. -/
theorem process_ptc_window_shape (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_ptc_window p o s = .ok s') :
    ∃ ptc_window : List (List ValidatorIndex), s' = { s with ptc_window } := by
  unfold process_ptc_window at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨_, rfl⟩)

/-- `process_sync_committee_updates` keeps the registry, the slot and the checkpoints. -/
theorem process_sync_committee_updates_frame (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_sync_committee_updates p o s = .ok s') :
    RegistryFrame s s' ∧ CheckpointsStable s s' := by
  obtain ⟨_, _, rfl⟩ := process_sync_committee_updates_shape p o s s' h
  exact ⟨⟨rfl, rfl, rfl⟩, ⟨rfl, rfl, rfl, rfl⟩⟩

/-- `process_proposer_lookahead` keeps the registry, the slot and the checkpoints. -/
theorem process_proposer_lookahead_frame (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_proposer_lookahead p o s = .ok s') :
    RegistryFrame s s' ∧ CheckpointsStable s s' := by
  obtain ⟨_, rfl⟩ := process_proposer_lookahead_shape p o s s' h
  exact ⟨⟨rfl, rfl, rfl⟩, ⟨rfl, rfl, rfl, rfl⟩⟩

/-- `process_ptc_window` keeps the registry, the slot and the checkpoints. -/
theorem process_ptc_window_frame (p : Preset) (o : Oracle) (s s' : BeaconState)
    (h : process_ptc_window p o s = .ok s') :
    RegistryFrame s s' ∧ CheckpointsStable s s' := by
  obtain ⟨_, rfl⟩ := process_ptc_window_shape p o s s' h
  exact ⟨⟨rfl, rfl, rfl⟩, ⟨rfl, rfl, rfl, rfl⟩⟩

end EpochProofs.Spec
