import EpochProofs.Spec.JustificationFinalization
import EpochProofs.Sanity.Effects

/-!
# Justification and finalization: frame and checkpoint invariants

`process_justification_and_finalization` keeps `validators`, `balances`, `inactivity_scores`
and `slot`. `FfgInvariant` orders the three checkpoints below the current epoch. It holds on every
state outside epoch processing, so the finalized epoch is never in the future.
-/

namespace EpochProofs.Spec

/-- `omega` after unfolding `Epoch`, `Slot` and `Uint64` to `Nat`. -/
macro "epoch_omega" : tactic => `(tactic| ((try simp only [Epoch, Slot, Uint64] at *); omega))

/-! ## One run of `weigh_justification_and_finalization` -/

/-- The facts that each step of `weigh_justification_and_finalization` keeps, from `s` to `t`.
The new current justified epoch is the old one, the previous epoch or the current epoch. The new
finalized checkpoint is one of the three old checkpoints. -/
def WeighStep (p : Preset) (s t : BeaconState) : Prop :=
  RegistryFrame s t ∧ t.slot = s.slot ∧
    t.previous_justified_checkpoint = s.current_justified_checkpoint ∧
    (t.current_justified_checkpoint = s.current_justified_checkpoint ∨
      get_previous_epoch p s = .ok t.current_justified_checkpoint.epoch ∨
      get_current_epoch p s = .ok t.current_justified_checkpoint.epoch) ∧
    (t.finalized_checkpoint = s.finalized_checkpoint ∨
      t.finalized_checkpoint = s.previous_justified_checkpoint ∨
      t.finalized_checkpoint = s.current_justified_checkpoint)

/-- Splits one `let state ← ...` step of `weigh_justification_and_finalization`. Each success
branch keeps `WeighStep`, sets the current justified epoch, or sets the finalized checkpoint. -/
macro "weigh_step" ht:ident q:ident hpe:ident hce:ident : tactic => `(tactic| (
  simp only [bind, Except.bind, pure, Except.pure] at $ht:ident
  repeat' split at $ht:ident
  all_goals first
    | contradiction
    | (cases $ht:ident
       first
        | exact $q:ident
        | exact ⟨$q:ident.1, $q:ident.2.1, $q:ident.2.2.1, .inr (.inl $hpe:ident),
            $q:ident.2.2.2.2⟩
        | exact ⟨$q:ident.1, $q:ident.2.1, $q:ident.2.2.1, .inr (.inr $hce:ident),
            $q:ident.2.2.2.2⟩
        | exact ⟨$q:ident.1, $q:ident.2.1, $q:ident.2.2.1, $q:ident.2.2.2.1, .inr (.inl rfl)⟩
        | exact ⟨$q:ident.1, $q:ident.2.1, $q:ident.2.2.1, $q:ident.2.2.2.1, .inr (.inr rfl)⟩)))

/-- The shape of one successful `weigh_justification_and_finalization` run. -/
theorem weigh_justification_and_finalization_step (p : Preset) (state state' : BeaconState)
    (total_active_balance previous_epoch_target_balance current_epoch_target_balance : Gwei)
    (h : weigh_justification_and_finalization p state total_active_balance
      previous_epoch_target_balance current_epoch_target_balance = .ok state') :
    WeighStep p state state' := by
  unfold weigh_justification_and_finalization at h
  obtain ⟨pe, hpe, h⟩ := specM_bind_ok h
  obtain ⟨ce, hce, h⟩ := specM_bind_ok h
  obtain ⟨b0, -, h⟩ := specM_bind_ok h
  have q0 : WeighStep p state { state with
      previous_justified_checkpoint := state.current_justified_checkpoint,
      justification_bits := b0 } :=
    ⟨⟨rfl, rfl, rfl⟩, rfl, rfl, .inl rfl, .inl rfl⟩
  obtain ⟨t1, ht1, h⟩ := specM_bind_ok h
  have q1 : WeighStep p state t1 := by weigh_step ht1 q0 hpe hce
  obtain ⟨t2, ht2, h⟩ := specM_bind_ok h
  have q2 : WeighStep p state t2 := by weigh_step ht2 q1 hpe hce
  obtain ⟨t3, ht3, h⟩ := specM_bind_ok h
  have q3 : WeighStep p state t3 := by weigh_step ht3 q2 hpe hce
  obtain ⟨t4, ht4, h⟩ := specM_bind_ok h
  have q4 : WeighStep p state t4 := by weigh_step ht4 q3 hpe hce
  obtain ⟨t5, ht5, h⟩ := specM_bind_ok h
  have q5 : WeighStep p state t5 := by weigh_step ht5 q4 hpe hce
  weigh_step h q5 hpe hce

/-- On success, `process_justification_and_finalization` returns `state` or one
`weigh_justification_and_finalization` result. In the second case the current epoch is at
least 2. -/
theorem process_justification_and_finalization_step (p : Preset) (state state' : BeaconState)
    (h : process_justification_and_finalization p state = .ok state') :
    state' = state ∨
      (2 ≤ state.slot / p.SLOTS_PER_EPOCH ∧ WeighStep p state state') := by
  unfold process_justification_and_finalization at h
  obtain ⟨ce, hce, h⟩ := specM_bind_ok h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · cases h
    exact .inl rfl
  · rename_i hle
    have hce' : ce = state.slot / p.SLOTS_PER_EPOCH := by
      simp only [get_current_epoch, compute_epoch_at_slot, uint64Div] at hce
      split at hce
      · cases hce
      · cases hce; rfl
    refine .inr ⟨?_, ?_⟩
    · simp only [GENESIS_EPOCH] at hle
      epoch_omega
    · repeat' split at h
      all_goals first
        | contradiction
        | exact weigh_justification_and_finalization_step p _ _ _ _ _ h

/-- `process_justification_and_finalization` keeps `validators`, `balances`,
`inactivity_scores` and `slot`. -/
theorem process_justification_and_finalization_frame (p : Preset) (state state' : BeaconState)
    (h : process_justification_and_finalization p state = .ok state') :
    RegistryFrame state state' ∧ state'.slot = state.slot := by
  rcases process_justification_and_finalization_step p state state' h with rfl | ⟨-, hw⟩
  · exact ⟨RegistryFrame.refl _, rfl⟩
  · exact ⟨hw.1, hw.2.1⟩

/-- `process_justification_and_finalization` keeps effective balances. -/
theorem process_justification_and_finalization_ebStable (p : Preset) (state state' : BeaconState)
    (h : process_justification_and_finalization p state = .ok state') : EBStable state state' :=
  (process_justification_and_finalization_frame p state state' h).1.ebStable

/-- `process_justification_and_finalization` keeps every balance. -/
theorem process_justification_and_finalization_balancesUp (p : Preset)
    (state state' : BeaconState) (h : process_justification_and_finalization p state = .ok state') :
    BalancesUp state state' :=
  (process_justification_and_finalization_frame p state state' h).1.balancesUp

/-! ## Checkpoint invariants -/

/-- The epoch of `s.slot`. It equals `get_current_epoch` when the division succeeds. -/
def epochOf (p : Preset) (s : BeaconState) : Nat := s.slot / p.SLOTS_PER_EPOCH

/-- A successful `get_current_epoch` is `epochOf`. -/
private theorem get_current_epoch_ok {p : Preset} {s : BeaconState} {e : Epoch}
    (h : get_current_epoch p s = .ok e) : e = epochOf p s := by
  simp only [get_current_epoch, compute_epoch_at_slot, uint64Div] at h
  split at h
  · cases h
  · cases h; rfl

/-- A successful `get_previous_epoch` is one below `epochOf`, or zero at the genesis epoch. -/
private theorem get_previous_epoch_ok {p : Preset} {s : BeaconState} {e : Epoch}
    (h : get_previous_epoch p s = .ok e) : e + 1 = epochOf p s ∨ (e = 0 ∧ epochOf p s = 0) := by
  simp only [get_previous_epoch, bind, Except.bind] at h
  split at h
  · cases h
  · rename_i c hc
    cases h
    have := get_current_epoch_ok hc
    simp only [saturating_sub]
    split <;> epoch_omega

/-- No checkpoint epoch is above the current epoch. -/
def CheckpointsBelow (p : Preset) (s : BeaconState) : Prop :=
  s.finalized_checkpoint.epoch ≤ epochOf p s ∧
    s.previous_justified_checkpoint.epoch ≤ epochOf p s ∧
    s.current_justified_checkpoint.epoch ≤ epochOf p s

/-- `process_justification_and_finalization` keeps `CheckpointsBelow`. -/
theorem process_justification_and_finalization_checkpointsBelow (p : Preset)
    (state state' : BeaconState) (h : process_justification_and_finalization p state = .ok state')
    (hinv : CheckpointsBelow p state) : CheckpointsBelow p state' := by
  rcases process_justification_and_finalization_step p state state' h with rfl | ⟨-, hw⟩
  · exact hinv
  obtain ⟨-, hslot, hpj, hcj, hfin⟩ := hw
  obtain ⟨hf, hp, hc⟩ := hinv
  have he : epochOf p state' = epochOf p state := by simp only [epochOf, hslot]
  refine ⟨?_, ?_, ?_⟩
  · rw [he]
    rcases hfin with h1 | h1 | h1 <;> rw [h1] <;> assumption
  · rw [he, hpj]
    exact hc
  · rw [he]
    rcases hcj with h1 | h1 | h1
    · rw [h1]; exact hc
    · have := get_previous_epoch_ok h1; epoch_omega
    · have := get_current_epoch_ok h1; epoch_omega

/-- The finalized epoch is at most the previous justified epoch, and that is at most the current
justified epoch. -/
def CheckpointOrder (s : BeaconState) : Prop :=
  s.finalized_checkpoint.epoch ≤ s.previous_justified_checkpoint.epoch ∧
    s.previous_justified_checkpoint.epoch ≤ s.current_justified_checkpoint.epoch

/-- The FFG invariant on states outside epoch processing: the checkpoints are in order, and the
current justified epoch is below the current epoch or is the genesis epoch. -/
def FfgInvariant (p : Preset) (s : BeaconState) : Prop :=
  CheckpointOrder s ∧
    (s.current_justified_checkpoint.epoch < epochOf p s ∨ s.current_justified_checkpoint.epoch = 0)

/-- The invariant right after `process_justification_and_finalization`: the checkpoints are in
order, and the current justified epoch is at most the current epoch. -/
def FfgInvariantAfter (p : Preset) (s : BeaconState) : Prop :=
  CheckpointOrder s ∧ s.current_justified_checkpoint.epoch ≤ epochOf p s

/-- Under `FfgInvariant`, the finalized epoch is at most the current epoch. -/
theorem FfgInvariant.finalized_le {p : Preset} {s : BeaconState} (h : FfgInvariant p s) :
    s.finalized_checkpoint.epoch ≤ epochOf p s := by
  obtain ⟨⟨h1, h2⟩, h3⟩ := h
  epoch_omega

/-- `FfgInvariant` gives `CheckpointsBelow`. -/
theorem FfgInvariant.checkpointsBelow {p : Preset} {s : BeaconState} (h : FfgInvariant p s) :
    CheckpointsBelow p s := by
  obtain ⟨⟨h1, h2⟩, h3⟩ := h
  refine ⟨?_, ?_, ?_⟩ <;> epoch_omega

/-- `FfgInvariantAfter` gives `CheckpointsBelow`. -/
theorem FfgInvariantAfter.checkpointsBelow {p : Preset} {s : BeaconState}
    (h : FfgInvariantAfter p s) : CheckpointsBelow p s := by
  obtain ⟨⟨h1, h2⟩, h3⟩ := h
  refine ⟨?_, ?_, ?_⟩ <;> epoch_omega

/-- A state that keeps the slot and the checkpoints keeps `FfgInvariant`. -/
theorem FfgInvariant.of_checkpointsStable {p : Preset} {s s' : BeaconState}
    (h : FfgInvariant p s) (hs : CheckpointsStable s s') : FfgInvariant p s' := by
  obtain ⟨hslot, hf, hc, hp⟩ := hs
  simp only [FfgInvariant, CheckpointOrder, epochOf, hslot, hf, hc, hp]
  exact h

/-- A state that keeps the slot and the checkpoints keeps `FfgInvariantAfter`. -/
theorem FfgInvariantAfter.of_checkpointsStable {p : Preset} {s s' : BeaconState}
    (h : FfgInvariantAfter p s) (hs : CheckpointsStable s s') : FfgInvariantAfter p s' := by
  obtain ⟨hslot, hf, hc, hp⟩ := hs
  simp only [FfgInvariantAfter, CheckpointOrder, epochOf, hslot, hf, hc, hp]
  exact h

/-- `process_justification_and_finalization` turns `FfgInvariant` into `FfgInvariantAfter`. The
finalized epoch does not go down. -/
theorem process_justification_and_finalization_ffg (p : Preset) (state state' : BeaconState)
    (h : process_justification_and_finalization p state = .ok state')
    (hinv : FfgInvariant p state) :
    FfgInvariantAfter p state' ∧
      state.finalized_checkpoint.epoch ≤ state'.finalized_checkpoint.epoch := by
  rcases process_justification_and_finalization_step p state state' h with rfl | ⟨h2, hw⟩
  · obtain ⟨⟨h1, h2⟩, h3⟩ := hinv
    exact ⟨⟨⟨h1, h2⟩, by epoch_omega⟩, Nat.le_refl _⟩
  obtain ⟨-, hslot, hpj, hcj, hfin⟩ := hw
  obtain ⟨⟨hfp, hpc⟩, hc⟩ := hinv
  have h2' : 2 ≤ epochOf p state := h2
  have he : epochOf p state' = epochOf p state := by simp only [epochOf, hslot]
  have hc' : state.current_justified_checkpoint.epoch + 1 ≤ epochOf p state := by epoch_omega
  have hcj' : state.current_justified_checkpoint.epoch ≤
      state'.current_justified_checkpoint.epoch ∧
      state'.current_justified_checkpoint.epoch ≤ epochOf p state := by
    rcases hcj with h1 | h1 | h1
    · rw [h1]; epoch_omega
    · have := get_previous_epoch_ok h1; epoch_omega
    · have := get_current_epoch_ok h1; epoch_omega
  have hfin' : state.finalized_checkpoint.epoch ≤ state'.finalized_checkpoint.epoch ∧
      state'.finalized_checkpoint.epoch ≤ state.current_justified_checkpoint.epoch := by
    rcases hfin with h1 | h1 | h1 <;> rw [h1] <;> epoch_omega
  refine ⟨⟨⟨?_, ?_⟩, ?_⟩, hfin'.1⟩
  · rw [hpj]; exact hfin'.2
  · rw [hpj]; exact hcj'.1
  · rw [he]; exact hcj'.2

/-! ## `process_slots` -/

/-- A successful `uint64Add` is the `Nat` sum. -/
private theorem uint64Add_ok_eq {a b c : Uint64} (h : uint64Add a b = .ok c) : c = a + b := by
  unfold uint64Add at h
  split at h
  · cases h; rfl
  · cases h

/-- A successful `uint64Mod` is the `Nat` remainder. -/
private theorem uint64Mod_ok_eq {a b c : Uint64} (h : uint64Mod a b = .ok c) : c = a % b := by
  unfold uint64Mod at h
  split at h
  · cases h
  · cases h; rfl

/-- The `process_slots` loop keeps `P` when the slot only grows by one. `processEpoch` runs on
the last slot of an epoch. Its result satisfies a weaker `Q`, and the step to the next slot
turns `Q` back into `P`. -/
theorem process_slots_loop_invariant_mono (p : Preset) (o : Oracle)
    (processEpoch : BeaconState → SpecM BeaconState) (slot : Slot) (P Q : BeaconState → Prop)
    (hslot : ∀ s s', process_slot p o s = .ok s' → P s → P s')
    (hepoch : ∀ s s', (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 → processEpoch s = .ok s' → P s →
      Q s' ∧ s'.slot = s.slot)
    (hbump_epoch : ∀ s, (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 → Q s →
      P { s with slot := s.slot + 1 })
    (hbump : ∀ s, P s → P { s with slot := s.slot + 1 }) :
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
      have hP1 := hslot _ _ h1 hP
      obtain ⟨a, ha, h⟩ := specM_bind_ok h
      obtain ⟨m, hm, h⟩ := specM_bind_ok h
      rw [uint64Add_ok_eq ha] at hm
      have hm' := uint64Mod_ok_eq hm
      split at h
      · rename_i hz
        have hz' : (s1.slot + 1) % p.SLOTS_PER_EPOCH = 0 := by
          rw [← hm']; simpa using hz
        obtain ⟨s2, h2, h⟩ := specM_bind_ok h
        obtain ⟨hQ, hs2⟩ := hepoch _ _ hz' h2 hP1
        obtain ⟨c, hc, h⟩ := specM_bind_ok h
        rw [uint64Add_ok_eq hc] at h
        rw [← hs2] at hz'
        exact ih _ _ (hbump_epoch _ hz' hQ) h
      · obtain ⟨_, h2, h⟩ := specM_bind_ok h
        cases h2
        obtain ⟨c, hc, h⟩ := specM_bind_ok h
        rw [uint64Add_ok_eq hc] at h
        exact ih _ _ (hbump _ hP1) h
    · cases h
      exact hP

/-- `process_slots` keeps every property that the loop keeps by
`process_slots_loop_invariant_mono`. -/
theorem process_slots_invariant_mono (p : Preset) (o : Oracle)
    (processEpoch : BeaconState → SpecM BeaconState) (P Q : BeaconState → Prop)
    (hslot : ∀ s s', process_slot p o s = .ok s' → P s → P s')
    (hepoch : ∀ s s', (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 → processEpoch s = .ok s' → P s →
      Q s' ∧ s'.slot = s.slot)
    (hbump_epoch : ∀ s, (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 → Q s →
      P { s with slot := s.slot + 1 })
    (hbump : ∀ s, P s → P { s with slot := s.slot + 1 })
    (state state' : BeaconState) (slot : Slot) (hP : P state)
    (h : process_slots p o processEpoch state slot = .ok state') : P state' := by
  unfold process_slots at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  split at h
  · cases h
  · exact process_slots_loop_invariant_mono p o processEpoch slot P Q hslot hepoch hbump_epoch
      hbump _ _ _ hP h

/-- `process_slot` keeps the slot and the checkpoints. -/
theorem process_slot_checkpointsStable (p : Preset) (o : Oracle) (state state' : BeaconState)
    (h : process_slot p o state = .ok state') : CheckpointsStable state state' := by
  unfold process_slot at h
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact ⟨rfl, rfl, rfl, rfl⟩)

/-- One more slot does not lower the epoch. -/
private theorem epochOf_succ_le (p : Preset) (s : BeaconState) :
    epochOf p s ≤ epochOf p { s with slot := s.slot + 1 } :=
  Nat.div_le_div_right (Nat.le_succ _)

/-- From the last slot of an epoch, one more slot raises the epoch. -/
private theorem epochOf_succ_of_mod (p : Preset) (s : BeaconState)
    (hz : (s.slot + 1) % p.SLOTS_PER_EPOCH = 0) :
    epochOf p s < epochOf p { s with slot := s.slot + 1 } := by
  simp only [epochOf]
  by_cases h0 : p.SLOTS_PER_EPOCH = 0
  · rw [h0, Nat.mod_zero] at hz
    epoch_omega
  · have hpos : 0 < p.SLOTS_PER_EPOCH := Nat.pos_of_ne_zero h0
    have h1 := Nat.div_add_mod (s.slot + 1) p.SLOTS_PER_EPOCH
    have h2 := Nat.div_add_mod s.slot p.SLOTS_PER_EPOCH
    have h3 := Nat.mod_lt s.slot hpos
    rw [hz, Nat.add_zero] at h1
    refine Nat.lt_of_not_le fun hle => ?_
    have := Nat.mul_le_mul_left p.SLOTS_PER_EPOCH hle
    epoch_omega

/-- If each `processEpoch` call keeps the slot and keeps `CheckpointsBelow`, then
`process_slots` keeps `CheckpointsBelow`. -/
theorem process_slots_checkpointsBelow (p : Preset) (o : Oracle)
    (processEpoch : BeaconState → SpecM BeaconState)
    (hepoch : ∀ s s', processEpoch s = .ok s' → CheckpointsBelow p s →
      CheckpointsBelow p s' ∧ s'.slot = s.slot)
    (state state' : BeaconState) (slot : Slot) (hinv : CheckpointsBelow p state)
    (h : process_slots p o processEpoch state slot = .ok state') : CheckpointsBelow p state' := by
  have hbump : ∀ s, CheckpointsBelow p s → CheckpointsBelow p { s with slot := s.slot + 1 } := by
    intro s ⟨h1, h2, h3⟩
    have := epochOf_succ_le p s
    exact ⟨Nat.le_trans h1 this, Nat.le_trans h2 this, Nat.le_trans h3 this⟩
  refine process_slots_invariant_mono p o processEpoch (CheckpointsBelow p) (CheckpointsBelow p)
    ?_ (fun s s' _ hs hP => hepoch s s' hs hP) (fun s _ hQ => hbump s hQ) hbump state state' slot
    hinv h
  intro s s' hs ⟨h1, h2, h3⟩
  obtain ⟨hslot, hf, hc, hp⟩ := process_slot_checkpointsStable p o s s' hs
  simp only [CheckpointsBelow, epochOf, hslot, hf, hc, hp]
  exact ⟨h1, h2, h3⟩

/-- `process_slots` keeps `FfgInvariant`. Each `processEpoch` call starts from a state with
`FfgInvariant` and must end in a state with `FfgInvariantAfter` at the same slot.
`process_justification_and_finalization_ffg` gives this for the justification step. The other
epoch steps keep the slot and the checkpoints. -/
theorem process_slots_ffg (p : Preset) (o : Oracle)
    (processEpoch : BeaconState → SpecM BeaconState)
    (hepoch : ∀ s s', processEpoch s = .ok s' → FfgInvariant p s →
      FfgInvariantAfter p s' ∧ s'.slot = s.slot)
    (state state' : BeaconState) (slot : Slot) (hinv : FfgInvariant p state)
    (h : process_slots p o processEpoch state slot = .ok state') : FfgInvariant p state' := by
  refine process_slots_invariant_mono p o processEpoch (FfgInvariant p) (FfgInvariantAfter p)
    (fun s s' hs hP => hP.of_checkpointsStable (process_slot_checkpointsStable p o s s' hs))
    (fun s s' _ hs hP => hepoch s s' hs hP) ?_ ?_ state state' slot hinv h
  · intro s hz ⟨hord, hc⟩
    exact ⟨hord, .inl (Nat.lt_of_le_of_lt hc (epochOf_succ_of_mod p s hz))⟩
  · intro s ⟨hord, hc⟩
    refine ⟨hord, ?_⟩
    rcases hc with hc | hc
    · exact .inl (Nat.lt_of_lt_of_le hc (epochOf_succ_le p s))
    · exact .inr hc

end EpochProofs.Spec
