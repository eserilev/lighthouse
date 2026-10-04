import EpochProofs.Spec.Slots
import EpochProofs.Spec.Block.Header
import EpochProofs.Spec.Block.Randao
import EpochProofs.Spec.Block.Eth1Data
import EpochProofs.Spec.EpochRest

/-!
# Frame theorems for slot, block header, RANDAO, eth1 and epoch reset steps

Each step below keeps `validators`, `balances`, `inactivity_scores` and `slot`.
`process_slots` keeps any property that `process_slot`, `processEpoch` and the slot increment
keep. These facts hold for every `Oracle`.
-/

namespace EpochProofs.Spec

/-- `s'` has the `validators`, `balances` and `inactivity_scores` of `s`. -/
def RegistryFrame (s s' : BeaconState) : Prop :=
  s'.validators = s.validators ∧ s'.balances = s.balances ∧
    s'.inactivity_scores = s.inactivity_scores

theorem RegistryFrame.refl (s : BeaconState) : RegistryFrame s s := ⟨rfl, rfl, rfl⟩

theorem RegistryFrame.trans {s1 s2 s3 : BeaconState} (h12 : RegistryFrame s1 s2)
    (h23 : RegistryFrame s2 s3) : RegistryFrame s1 s3 :=
  ⟨h23.1.trans h12.1, h23.2.1.trans h12.2.1, h23.2.2.trans h12.2.2⟩

theorem specM_bind_ok {α β : Type} {x : SpecM α} {f : α → SpecM β} {b : β}
    (h : (x >>= f) = .ok b) : ∃ a, x = .ok a ∧ f a = .ok b := by
  cases x with
  | error e => cases h
  | ok a => exact ⟨a, rfl, h⟩

private theorem uint64Add_ok {a b c : Uint64} (h : uint64Add a b = .ok c) : c = a + b := by
  unfold uint64Add at h
  split at h
  · cases h; rfl
  · cases h

theorem process_slots_loop_invariant (p : Preset) (o : Oracle)
    (processEpoch : BeaconState → SpecM BeaconState) (slot : Slot) (P : BeaconState → Prop)
    (hslot : ∀ s s', process_slot p o s = .ok s' → P s → P s')
    (hepoch : ∀ s s', processEpoch s = .ok s' → P s → P s')
    (hbump : ∀ s n, P s → P { s with slot := n }) :
    ∀ fuel state state', P state →
      process_slots_loop p o processEpoch slot fuel state = .ok state' → P state' := by
  intro fuel
  induction fuel with
  | zero =>
    intro state state' hP h
    cases h
    exact hP
  | succ n ih =>
    intro state state' hP h
    simp only [process_slots_loop] at h
    split at h
    · obtain ⟨s1, h1, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      split at h
      · obtain ⟨s2, h2, h⟩ := specM_bind_ok h
        obtain ⟨_, -, h⟩ := specM_bind_ok h
        exact ih _ _ (hbump _ _ (hepoch _ _ h2 (hslot _ _ h1 hP))) h
      · obtain ⟨_, h2, h⟩ := specM_bind_ok h
        cases h2
        obtain ⟨_, -, h⟩ := specM_bind_ok h
        exact ih _ _ (hbump _ _ (hslot _ _ h1 hP)) h
    · cases h
      exact hP

/-- Splits each `match` and `if` of a step that only writes other fields, then closes each
success branch by `rfl` and each error branch by `contradiction`. -/
macro "frame_tac" h:ident : tactic => `(tactic| (
  simp only [bind, Except.bind, pure, Except.pure] at $h:ident
  repeat' split at $h:ident
  all_goals first
    | (cases $h:ident; exact ⟨⟨rfl, rfl, rfl⟩, rfl⟩)
    | contradiction))

theorem process_slot_frame (p : Preset) (o : Oracle) (state state' : BeaconState)
    (h : process_slot p o state = .ok state') :
    RegistryFrame state state' ∧ state'.slot = state.slot := by
  unfold process_slot at h
  frame_tac h

theorem process_block_header_frame (p : Preset) (o : Oracle) (state state' : BeaconState)
    (block : BeaconBlock) (h : process_block_header p o state block = .ok state') :
    RegistryFrame state state' ∧ state'.slot = state.slot := by
  unfold process_block_header at h
  simp only [throw, throwThe, MonadExceptOf.throw] at h
  frame_tac h

theorem process_randao_frame (p : Preset) (o : Oracle) (state state' : BeaconState)
    (body : BeaconBlockBody) (h : process_randao p o state body = .ok state') :
    RegistryFrame state state' ∧ state'.slot = state.slot := by
  unfold process_randao at h
  frame_tac h

theorem process_eth1_data_frame (p : Preset) (state state' : BeaconState)
    (body : BeaconBlockBody) (h : process_eth1_data p state body = .ok state') :
    RegistryFrame state state' ∧ state'.slot = state.slot := by
  unfold process_eth1_data at h
  frame_tac h

theorem process_eth1_data_reset_frame (p : Preset) (state state' : BeaconState)
    (h : process_eth1_data_reset p state = .ok state') :
    RegistryFrame state state' ∧ state'.slot = state.slot := by
  unfold process_eth1_data_reset at h
  frame_tac h

theorem process_slashings_reset_frame (p : Preset) (state state' : BeaconState)
    (h : process_slashings_reset p state = .ok state') :
    RegistryFrame state state' ∧ state'.slot = state.slot := by
  unfold process_slashings_reset at h
  frame_tac h

theorem process_randao_mixes_reset_frame (p : Preset) (state state' : BeaconState)
    (h : process_randao_mixes_reset p state = .ok state') :
    RegistryFrame state state' ∧ state'.slot = state.slot := by
  unfold process_randao_mixes_reset at h
  frame_tac h

theorem process_historical_summaries_update_frame (p : Preset) (o : Oracle)
    (state state' : BeaconState) (h : process_historical_summaries_update p o state = .ok state') :
    RegistryFrame state state' ∧ state'.slot = state.slot := by
  unfold process_historical_summaries_update at h
  frame_tac h

theorem process_participation_flag_updates_frame (state : BeaconState) :
    RegistryFrame state (process_participation_flag_updates state) ∧
      (process_participation_flag_updates state).slot = state.slot :=
  ⟨⟨rfl, rfl, rfl⟩, rfl⟩

/-- `process_slots` keeps every property that `process_slot`, `processEpoch` and the slot
increment keep. -/
theorem process_slots_invariant (p : Preset) (o : Oracle)
    (processEpoch : BeaconState → SpecM BeaconState) (P : BeaconState → Prop)
    (hslot : ∀ s s', process_slot p o s = .ok s' → P s → P s')
    (hepoch : ∀ s s', processEpoch s = .ok s' → P s → P s')
    (hbump : ∀ s n, P s → P { s with slot := n })
    (state state' : BeaconState) (slot : Slot) (hP : P state)
    (h : process_slots p o processEpoch state slot = .ok state') : P state' := by
  unfold process_slots at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · cases h
  · exact process_slots_loop_invariant p o processEpoch slot P hslot hepoch hbump _ _ _ hP h

/-- If each `processEpoch` call keeps `validators`, `balances` and `inactivity_scores`, then
`process_slots` keeps them. -/
theorem process_slots_frame (p : Preset) (o : Oracle)
    (processEpoch : BeaconState → SpecM BeaconState)
    (hepoch : ∀ s s', processEpoch s = .ok s' → RegistryFrame s s')
    (state state' : BeaconState) (slot : Slot)
    (h : process_slots p o processEpoch state slot = .ok state') :
    RegistryFrame state state' :=
  process_slots_invariant p o processEpoch (RegistryFrame state)
    (fun _ _ hs hP => hP.trans (process_slot_frame p o _ _ hs).1)
    (fun _ _ hs hP => hP.trans (hepoch _ _ hs))
    (fun _ _ hP => hP) state state' slot (RegistryFrame.refl state) h

private theorem slot_step {a b c fuel : Nat} (hlt : a < b) (hfuel : b - a ≤ fuel + 1)
    (hc : c = a + 1) : c ≤ b ∧ b - c ≤ fuel := by
  omega

private theorem slot_done {a b : Nat} (hle : a ≤ b) (h : ¬ a < b ∨ b - a ≤ 0) :
    a = b := by
  omega

theorem process_slots_loop_slot (p : Preset) (o : Oracle)
    (processEpoch : BeaconState → SpecM BeaconState) (slot : Slot)
    (hepoch : ∀ s s', processEpoch s = .ok s' → s'.slot = s.slot) :
    ∀ fuel (state state' : BeaconState), state.slot ≤ slot → slot - state.slot ≤ fuel →
      process_slots_loop p o processEpoch slot fuel state = .ok state' → state'.slot = slot := by
  intro fuel
  induction fuel with
  | zero =>
    intro state state' hle hfuel h
    cases h
    exact slot_done hle (.inr hfuel)
  | succ n ih =>
    intro state state' hle hfuel h
    simp only [process_slots_loop] at h
    split at h
    · obtain ⟨s1, h1, h⟩ := specM_bind_ok h
      have hs1 := (process_slot_frame p o _ _ h1).2
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      obtain ⟨_, -, h⟩ := specM_bind_ok h
      split at h
      · obtain ⟨s2, h2, h⟩ := specM_bind_ok h
        obtain ⟨c, hc, h⟩ := specM_bind_ok h
        have hstep := slot_step (fuel := n) ‹state.slot < slot› hfuel
          ((uint64Add_ok hc).trans (by rw [hepoch _ _ h2, hs1]))
        exact ih _ _ hstep.1 hstep.2 h
      · obtain ⟨_, h2, h⟩ := specM_bind_ok h
        cases h2
        obtain ⟨c, hc, h⟩ := specM_bind_ok h
        have hstep := slot_step (fuel := n) ‹state.slot < slot› hfuel
          ((uint64Add_ok hc).trans (by rw [hs1]))
        exact ih _ _ hstep.1 hstep.2 h
    · cases h
      exact slot_done hle (.inl ‹¬ state.slot < slot›)

/-- If each `processEpoch` call keeps `slot`, then `process_slots` ends at the target slot. -/
theorem process_slots_slot (p : Preset) (o : Oracle)
    (processEpoch : BeaconState → SpecM BeaconState)
    (hepoch : ∀ s s', processEpoch s = .ok s' → s'.slot = s.slot)
    (state state' : BeaconState) (slot : Slot)
    (h : process_slots p o processEpoch state slot = .ok state') : state'.slot = slot := by
  unfold process_slots at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · cases h
  · exact process_slots_loop_slot p o processEpoch slot hepoch _ _ _ (Nat.le_of_lt (Decidable.of_not_not ‹¬ ¬ state.slot < slot›)) (Nat.le_refl _) h

end EpochProofs.Spec
