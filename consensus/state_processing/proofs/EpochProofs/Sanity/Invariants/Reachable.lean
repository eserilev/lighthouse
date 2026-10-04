import EpochProofs.Sanity.Invariants.BalanceFloor
import EpochProofs.Sanity.Invariants.EpochEnd
import EpochProofs.Sanity.Invariants.Lengths
import EpochProofs.Sanity.Invariants.ExitEpochs
import EpochProofs.Sanity.Invariants.ConsolidationIndices
import EpochProofs.Sanity.Invariants.WithdrawableEpochs
import EpochProofs.Sanity.SinglePass

/-!
# The balance floor on every reachable state

`Reachable` states come from an initial state by `state_transition` steps. On mainnet, each
`process_epoch` call of such a step starts from a state with `BoundaryOk`: the finalized epoch
is at most the current epoch, and every validator that is eligible for rewards has
`effective_balance ≤ 256 * balance`. The proof adds the check
`BoundaryOk` before each `process_epoch` and shows that the checked transition equals the
real one.
-/

namespace EpochProofs.Spec

open Classical in
/-- `process_epoch` behind a check `C` on its input state. -/
noncomputable def process_epoch_checked (p : Preset) (o : Oracle) (C : BeaconState → Prop)
    (s : BeaconState) : SpecM BeaconState :=
  if C s then process_epoch p o s else throw .assertionFailed

/-- A successful `uint64Mod` is the `Nat` remainder. -/
private theorem uint64Mod_ok'' {a b c : Nat} (h : uint64Mod a b = .ok c) : c = a % b := by
  unfold uint64Mod at h
  split at h
  · cases h
  · cases h; rfl

/-- The `process_slots` loop with a checked `process_epoch` equals the loop with the real one,
and keeps `P`, if `P` gives the check `C` before each `process_epoch`. `Q` holds after
`process_epoch`, and the step to the next slot turns it back into `P`. -/
theorem process_slots_loop_checked (p : Preset) (o : Oracle) (slot : Slot)
    (P Q C : BeaconState → Prop)
    (hslot : ∀ s s', process_slot p o s = .ok s' → s.slot < slot → P s → P s')
    (hepoch : ∀ s s', s.slot < slot → (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 →
      process_epoch p o s = .ok s' → P s → Q s')
    (hcheck : ∀ s, s.slot < slot → (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 → P s → C s)
    (hbump_epoch : ∀ s, (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 → Q s →
      P { s with slot := s.slot + 1 })
    (hbump : ∀ s, s.slot < slot → (s.slot + 1) % p.SLOTS_PER_EPOCH ≠ 0 → P s →
      P { s with slot := s.slot + 1 }) :
    ∀ fuel st, P st →
      process_slots_loop p o (process_epoch_checked p o C) slot fuel st =
        process_slots_loop p o (process_epoch p o) slot fuel st ∧
      ∀ st', process_slots_loop p o (process_epoch p o) slot fuel st = .ok st' → P st' := by
  intro fuel
  induction fuel with
  | zero => intro st hP; exact ⟨rfl, fun st' h => by cases h; exact hP⟩
  | succ n ih =>
    intro st hP
    by_cases hlt : st.slot < slot
    · simp only [process_slots_loop, hlt, if_true]
      cases hps : process_slot p o st with
      | error e =>
        exact ⟨by simp [bind, Except.bind], fun st' h => by simp [bind, Except.bind] at h⟩
      | ok s1 =>
        have hs1 := (process_slot_frame p o _ _ hps).2
        have hP1 := hslot _ _ hps hlt hP
        have hlt1 : s1.slot < slot := hs1 ▸ hlt
        simp only [bind, Except.bind]
        cases ha : uint64Add s1.slot 1 with
        | error e => simp
        | ok a =>
        have ha' := uint64Add_ok ha
        dsimp only
        cases hm : uint64Mod a p.SLOTS_PER_EPOCH with
        | error e => simp
        | ok m =>
        have hm' := uint64Mod_ok'' hm
        dsimp only
        by_cases hz : (m == 0) = true
        · have hz' : (s1.slot + 1) % p.SLOTS_PER_EPOCH = 0 := by
            rw [← ha', ← hm']; simpa using hz
          have hC := hcheck s1 hlt1 hz' hP1
          simp only [hz, if_true, process_epoch_checked, if_pos hC]
          cases he : process_epoch p o s1 with
          | error e => simp
          | ok s2 =>
            dsimp only
            have hQ := hepoch _ _ hlt1 hz' he hP1
            have hs2 := process_epoch_slot p o _ _ he
            cases hc : uint64Add s2.slot 1 with
            | error e => simp
            | ok c =>
              dsimp only
              rw [uint64Add_ok hc]
              exact ih _ (hbump_epoch s2 (hs2 ▸ hz') hQ)
        · have hz' : (s1.slot + 1) % p.SLOTS_PER_EPOCH ≠ 0 := by
            rw [← ha', ← hm']; simpa using hz
          simp only [hz, Bool.false_eq_true, if_false, pure, Except.pure]
          cases hc : uint64Add s1.slot 1 with
          | error e => simp
          | ok c =>
            dsimp only
            rw [uint64Add_ok hc]
            exact ih _ (hbump s1 hlt1 hz' hP1)
    · simp only [process_slots_loop, hlt, if_false]
      refine ⟨trivial, fun st' h => ?_⟩
      cases h
      exact hP

/-! ## The invariant -/

/-- The effective balance part of `EpochEntry` for the previous epoch
`slot / SLOTS_PER_EPOCH - 1`. -/
def EntryEB (s : BeaconState) : Prop :=
  ∀ i (hi : i < s.validators.length),
    rewardsEligible (s.slot / Preset.mainnet.SLOTS_PER_EPOCH - 1) s.validators[i] = .ok true →
      s.validators[i].effective_balance ≤ 256 * s.balances.getD i 0

/-- The facts at the end of an epoch: `FfgInvariant`, `RowsOk` and `EntryEB`. -/
def BoundaryOk (s : BeaconState) : Prop :=
  FfgInvariant Preset.mainnet s ∧ RowsOk s ∧ EntryEB s

/-- At the end of an epoch, the finalized epoch is at most the current epoch. -/
theorem BoundaryOk.finalized_le {s : BeaconState} (h : BoundaryOk s) :
    s.finalized_checkpoint.epoch ≤ s.slot / Preset.mainnet.SLOTS_PER_EPOCH :=
  h.1.finalized_le

/-- The loop invariant of `process_slots` from slot `start` on mainnet. `FfgInvariant`,
`RowsOk`, `ExitDelay` and `ExitEpochsU64` hold, and the budget of the current epoch holds
from a state `s0` with `AfterEB`. `n` sync aggregates ran in the epoch, at most one for each
slot so far. After the first slot step, the current slot has no block yet. -/
def LoopInv (start : Slot) (t : BeaconState) : Prop :=
  FfgInvariant Preset.mainnet t ∧ RowsOk t ∧ ExitDelay Preset.mainnet t ∧ ExitEpochsU64 t ∧
    start ≤ t.slot ∧
    ∃ (s0 : BeaconState) (n : Nat), AfterEB Preset.mainnet s0 ∧
      Budget Preset.mainnet s0 (t.slot / Preset.mainnet.SLOTS_PER_EPOCH) (fun _ => 23307800) n t ∧
      n + t.slot / Preset.mainnet.SLOTS_PER_EPOCH * Preset.mainnet.SLOTS_PER_EPOCH ≤ t.slot + 1 ∧
      (start < t.slot →
        n + t.slot / Preset.mainnet.SLOTS_PER_EPOCH * Preset.mainnet.SLOTS_PER_EPOCH ≤ t.slot)

/-- The invariant of a reachable state. -/
def ReachInv (t : BeaconState) : Prop := LoopInv t.slot t

/-- After `process_epoch` inside `process_slots` from slot `start`. -/
def PostEpoch (start : Slot) (t : BeaconState) : Prop :=
  FfgInvariantAfter Preset.mainnet t ∧ RowsOk t ∧ ExitDelay Preset.mainnet t ∧
    ExitEpochsU64 t ∧ AfterEB Preset.mainnet t ∧ start ≤ t.slot

/-- The budget keeps the effective balances of `s0`, so `AfterEB s0` gives `EBOk`. -/
theorem Budget.ebOk {s0 t : BeaconState} {E : Epoch} {L : Nat → Gwei} {n : Nat}
    (h0 : AfterEB Preset.mainnet s0) (hb : Budget Preset.mainnet s0 E L n t) :
    EBOk Preset.mainnet t := by
  intro v hv
  obtain ⟨i, hi, rfl⟩ := List.getElem_of_mem hv
  have hi0 : i < s0.validators.length := hb.1 ▸ hi
  have hib : i < s0.balances.length := Nat.lt_of_lt_of_le hi0 h0.1
  obtain ⟨u, b, hu, -, heb, -⟩ :=
    hb.2 i _ _ (List.getElem?_eq_getElem hi0) (List.getElem?_eq_getElem hib)
  rw [List.getElem?_eq_getElem hi] at hu
  cases hu
  obtain ⟨hmod, -, hmax⟩ := h0.2 i hi0
  exact ⟨heb ▸ hmod, heb ▸ hmax⟩

/-- `EBOk` and `EffectiveBalanceFloor` give `AfterEB`. -/
theorem AfterEB.of_floor {s : BeaconState} (he : EBOk Preset.mainnet s)
    (hf : EffectiveBalanceFloor Preset.mainnet s) : AfterEB Preset.mainnet s :=
  ⟨hf.1, fun i hi => ⟨(hf.2 i hi).1, (hf.2 i hi).2, (he _ (List.getElem_mem hi)).2⟩⟩

/-- A genesis-like state with `FfgInvariant`, `RowsOk`, `ExitDelay`, `ExitEpochsU64` and
`AfterEB` has the invariant. -/
theorem ReachInv.of_start {s : BeaconState} (hffg : FfgInvariant Preset.mainnet s)
    (hrows : RowsOk s) (hd : ExitDelay Preset.mainnet s) (hx : ExitEpochsU64 s)
    (h0 : AfterEB Preset.mainnet s) : ReachInv s := by
  refine ⟨hffg, hrows, hd, hx, Nat.le_refl _, s, 0, h0, Budget.init _ _ _ _, ?_,
    fun h => absurd h (Nat.lt_irrefl _)⟩
  have := Nat.div_mul_le_self s.slot Preset.mainnet.SLOTS_PER_EPOCH
  omega

/-- The invariant before a `process_epoch` gives `BoundaryOk`. -/
theorem LoopInv.boundaryOk {start : Slot} {s : BeaconState}
    (hfar : s.slot / Preset.mainnet.SLOTS_PER_EPOCH < FAR_FUTURE_EPOCH)
    (h : LoopInv start s) : BoundaryOk s := by
  obtain ⟨hffg, hrows, -, -, -, s0, n, h0, hb, hn, -⟩ := h
  refine ⟨hffg, hrows, ?_⟩
  have hS : Preset.mainnet.SLOTS_PER_EPOCH = 32 := rfl
  have hn32 : n ≤ 32 := by
    rw [hS] at hn
    have := Nat.lt_div_mul_add (a := s.slot) (b := 32) (by decide)
    simp only [Slot] at *
    omega
  exact epochEntry_effective_balance_mainnet s0 s _ _ n h0 hb hn32 hfar (by omega)

/-- The five step facts of `LoopInv` in the `process_slots` loop with target `slot`: the slot
step, `process_epoch` (to `PostEpoch`), the check `BoundaryOk`, and the two slot increments. -/
theorem loopInv_steps (o : Oracle) (slot : Slot) (start : Slot) (hslot64 : slot < 2 ^ 64) :
    (∀ s s', process_slot Preset.mainnet o s = .ok s' → s.slot < slot →
      LoopInv start s → LoopInv start s') ∧
    (∀ s s', s.slot < slot → (s.slot + 1) % Preset.mainnet.SLOTS_PER_EPOCH = 0 →
      process_epoch Preset.mainnet o s = .ok s' → LoopInv start s → PostEpoch start s') ∧
    (∀ s, s.slot < slot → (s.slot + 1) % Preset.mainnet.SLOTS_PER_EPOCH = 0 →
      LoopInv start s → BoundaryOk s) ∧
    (∀ s, (s.slot + 1) % Preset.mainnet.SLOTS_PER_EPOCH = 0 → PostEpoch start s →
      LoopInv start { s with slot := s.slot + 1 }) ∧
    (∀ s, s.slot < slot → (s.slot + 1) % Preset.mainnet.SLOTS_PER_EPOCH ≠ 0 →
      LoopInv start s → LoopInv start { s with slot := s.slot + 1 }) := by
  have hS : Preset.mainnet.SLOTS_PER_EPOCH = 32 := rfl
  refine ⟨?_, ?_, ?_, ?_, ?_⟩
  · intro t t' ht _ hP
    obtain ⟨hf, hsl⟩ := process_slot_frame _ o t t' ht
    obtain ⟨hffg, hrows, hd, hx, hst, s0, n, h0, hb, hn, hstrict⟩ := hP
    exact ⟨hffg.of_checkpointsStable (process_slot_checkpointsStable _ o t t' ht),
      (process_slot_len _ o t t' ht).rowsOk hrows, ExitDelay.of_validators_eq hf.1 hd,
      ExitEpochsU64.of_validators_eq hf.1 hx, hsl ▸ hst,
      s0, n, h0,
      Budget.of_eq hf.1 hf.2.1 (hsl ▸ hb), hsl ▸ hn, hsl ▸ hstrict⟩
  · intro t t' _ _ he hP
    obtain ⟨hffg, hrows, hd, hx, hst, s0, n, h0, hb, -⟩ := hP
    obtain ⟨he', hf⟩ := process_epoch_effective_balances o t t' he (hb.ebOk h0)
    obtain ⟨hffg', hsl⟩ := process_epoch_ffg _ o t t' he hffg
    exact ⟨hffg', process_epoch_rowsOk _ o t t' he hrows, process_epoch_exitDelay _ o t t' he hd,
      process_epoch_exitEpochsU64 _ o t t' he hx, AfterEB.of_floor he' hf, hsl ▸ hst⟩
  · intro t hlt _ hP
    refine hP.boundaryOk ?_
    simp only [FAR_FUTURE_EPOCH, hS, Slot] at *
    omega
  · intro t hz hQ
    obtain ⟨⟨hord, hcj⟩, hrows, hd, hx, h0, hst⟩ := hQ
    have hle := Nat.div_mul_le_self (t.slot + 1) Preset.mainnet.SLOTS_PER_EPOCH
    have hffg : FfgInvariant Preset.mainnet { t with slot := t.slot + 1 } := by
      refine ⟨hord, .inl ?_⟩
      show t.current_justified_checkpoint.epoch < (t.slot + 1) / Preset.mainnet.SLOTS_PER_EPOCH
      have hcj' : t.current_justified_checkpoint.epoch ≤ t.slot / Preset.mainnet.SLOTS_PER_EPOCH :=
        hcj
      rw [hS] at hz hcj' ⊢
      simp only [Slot, Epoch] at *
      omega
    refine ⟨hffg, hrows, hd, hx, Nat.le_succ_of_le hst, { t with slot := t.slot + 1 }, 0,
      AfterEB.of_eq rfl rfl h0, Budget.init _ _ _ _, ?_, fun _ => ?_⟩
    · show 0 + (t.slot + 1) / Preset.mainnet.SLOTS_PER_EPOCH * Preset.mainnet.SLOTS_PER_EPOCH ≤
        t.slot + 1 + 1
      simp only [Slot] at *
      omega
    · show 0 + (t.slot + 1) / Preset.mainnet.SLOTS_PER_EPOCH * Preset.mainnet.SLOTS_PER_EPOCH ≤
        t.slot + 1
      simp only [Slot] at *
      omega
  · intro t _ hz hP
    obtain ⟨hffg, hrows, hd, hx, hst, s0, n, h0, hb, hn, -⟩ := hP
    have hE : (t.slot + 1) / Preset.mainnet.SLOTS_PER_EPOCH =
        t.slot / Preset.mainnet.SLOTS_PER_EPOCH := by
      rw [hS] at hz ⊢
      simp only [Slot] at *
      omega
    refine ⟨?_, hrows, hd, hx, Nat.le_succ_of_le hst, s0, n, h0, ?_, ?_, fun _ => ?_⟩
    · obtain ⟨hord, hcj⟩ := hffg
      refine ⟨hord, ?_⟩
      show _ < (t.slot + 1) / Preset.mainnet.SLOTS_PER_EPOCH ∨ _
      rw [hE]
      exact hcj
    · show Budget _ s0 ((t.slot + 1) / _) _ n { t with slot := t.slot + 1 }
      rw [hE]
      exact Budget.of_eq rfl rfl hb
    · show n + (t.slot + 1) / _ * _ ≤ t.slot + 1 + 1
      rw [hE]
      omega
    · show n + (t.slot + 1) / _ * _ ≤ t.slot + 1
      rw [hE]
      exact hn

/-- `process_slots` from a state with the invariant: the checked loop equals the real loop, and
it keeps `LoopInv`. -/
theorem process_slots_loop_reach (o : Oracle) (slot : Slot) (start : Slot)
    (hslot64 : slot < 2 ^ 64) :
    ∀ fuel st, LoopInv start st →
      process_slots_loop Preset.mainnet o (process_epoch_checked Preset.mainnet o BoundaryOk)
          slot fuel st =
        process_slots_loop Preset.mainnet o (process_epoch Preset.mainnet o) slot fuel st ∧
      ∀ st', process_slots_loop Preset.mainnet o (process_epoch Preset.mainnet o) slot fuel st =
        .ok st' → LoopInv start st' :=
  have h := loopInv_steps o slot start hslot64
  process_slots_loop_checked _ o slot (LoopInv start) (PostEpoch start) BoundaryOk
    h.1 h.2.1 h.2.2.1 h.2.2.2.1 h.2.2.2.2

/-- `process_slots` from a state with the invariant: the checked call equals the real call,
and the result has `LoopInv` from the old slot, at a later slot. -/
theorem process_slots_reach (o : Oracle) (s : BeaconState) (slot : Slot)
    (hslot64 : slot < 2 ^ 64) (hs : ReachInv s) :
    process_slots Preset.mainnet o (process_epoch_checked Preset.mainnet o BoundaryOk) s slot =
        process_slots Preset.mainnet o (process_epoch Preset.mainnet o) s slot ∧
      ∀ s1, process_slots Preset.mainnet o (process_epoch Preset.mainnet o) s slot = .ok s1 →
        LoopInv s.slot s1 ∧ s.slot < s1.slot := by
  unfold process_slots
  simp only [bind, Except.bind, pure, Except.pure]
  split
  · exact ⟨rfl, fun s1 h => by cases h⟩
  · rename_i hlt
    obtain ⟨heq, hinv⟩ := process_slots_loop_reach o slot s.slot hslot64 _ s hs
    refine ⟨heq, fun s1 h => ⟨hinv s1 h, ?_⟩⟩
    have := process_slots_loop_slot _ o (process_epoch _ o) slot (process_epoch_slot _ o) _ s s1
      (Nat.le_of_lt (Decidable.of_not_not hlt)) (Nat.le_refl _) h
    rw [this]
    exact Decidable.of_not_not hlt

/-- `state_transition` with the check `BoundaryOk` before each `process_epoch`. -/
noncomputable def state_transition_checked (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (state : BeaconState) (signed_block : SignedBeaconBlock)
    (validate_result : Bool := true) : SpecM BeaconState := do
  let block := signed_block.message
  let state ← process_slots Preset.mainnet o (process_epoch_checked Preset.mainnet o BoundaryOk)
    state block.slot
  if validate_result then
    if ¬ (← verify_block_signature Preset.mainnet o state signed_block) then
      throw .assertionFailed
  let state ← process_block Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH state block
  if validate_result then
    if ¬ block.state_root = o.hash_tree_root_BeaconState state then throw .assertionFailed
  pure state

/-- From a state with the invariant, the checked transition equals the real one. -/
theorem state_transition_checked_eq (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (s : BeaconState) (signed_block : SignedBeaconBlock)
    (validate_result : Bool) (hslot64 : signed_block.message.slot < 2 ^ 64) (hs : ReachInv s) :
    state_transition_checked o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
        validate_result =
      state_transition Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
        validate_result := by
  unfold state_transition_checked state_transition
  dsimp only
  rw [(process_slots_reach o s _ hslot64 hs).1]

/-- One `state_transition` keeps the invariant. The block's slot is a `u64`, its sync aggregate
has at most 512 bits, and the total active balance before the block is at most 139 million
ETH. -/
theorem state_transition_reach (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (s s' : BeaconState) (signed_block : SignedBeaconBlock)
    (validate_result : Bool) (hslot64 : signed_block.message.slot < 2 ^ 64)
    (hbits : signed_block.message.body.sync_aggregate.sync_committee_bits.length ≤ 512)
    (htab : ∀ s1, process_slots Preset.mainnet o (process_epoch Preset.mainnet o) s
      signed_block.message.slot = .ok s1 → ∀ tab,
        get_total_active_balance Preset.mainnet s1 = .ok tab → tab ≤ 139000000000000000)
    (hs : ReachInv s)
    (h : state_transition Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
      validate_result = .ok s') : ReachInv s' := by
  have hS : Preset.mainnet.SLOTS_PER_EPOCH = 32 := rfl
  unfold state_transition at h
  obtain ⟨s1, h1, h⟩ := specM_bind_ok h
  obtain ⟨hinv1, hlt1⟩ := (process_slots_reach o s _ hslot64 hs).2 s1 h1
  have hs1 := process_slots_process_epoch_slot _ o s s1 _ h1
  obtain ⟨hffg1, hrows1, hd1, hx1, -, s0, n, h0, hb1, -, hstrict⟩ := hinv1
  have hn1 := hstrict hlt1
  have hblock : ∀ s2, process_block Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s1
      signed_block.message = .ok s2 → ReachInv s2 := by
    intro s2 h2
    have hfar : s1.slot / Preset.mainnet.SLOTS_PER_EPOCH < FAR_FUTURE_EPOCH := by
      rw [hs1]
      simp only [FAR_FUTURE_EPOCH, hS, Slot] at *
      omega
    obtain ⟨hb2, hd2, hsl2, -⟩ := process_block_budget_mainnet o _ _ s0 _ n s1 s2 _ h2 rfl hfar
      hd1 hb1 hbits (htab s1 h1)
    refine ⟨hffg1.of_checkpointsStable (process_block_checkpointsStable _ o _ _ s1 s2 _ h2),
      (process_block_len _ o _ _ s1 s2 _ h2).rowsOk hrows1, hd2,
      process_block_exitEpochsU64 _ o _ _ s1 s2 _ h2 hx1, Nat.le_refl _, s0, n + 1, h0,
      hsl2 ▸ hb2, ?_,
      fun h => absurd h (Nat.lt_irrefl _)⟩
    rw [hsl2]
    omega
  simp only [bind, Except.bind, pure, Except.pure] at h
  repeat' split at h
  all_goals first
    | contradiction
    | (cases h; exact hblock _ ‹_›)

/-! ## Reachable states -/

/-- The states that `state_transition` steps reach from `init` on mainnet. Each block's slot is
a `u64` and its sync aggregate has at most `SYNC_COMMITTEE_SIZE = 512` bits, as their SSZ types
say. -/
inductive Reachable (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (init : BeaconState) : BeaconState → Prop
  | init : Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init init
  | step {s s' : BeaconState} (signed_block : SignedBeaconBlock) (validate_result : Bool) :
      Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s →
      signed_block.message.slot < 2 ^ 64 →
      signed_block.message.body.sync_aggregate.sync_committee_bits.length ≤ 512 →
      state_transition Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
        validate_result = .ok s' →
      Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s'

/-- The supply hypothesis: before each block of a reachable run, the total active balance is
at most 139 million ETH. The total ETH supply is about 120.7 million ETH. -/
def SupplyBound (o : Oracle) (max_blobs_per_block : Epoch → Uint64) (GLOAS_FORK_EPOCH : Epoch)
    (init : BeaconState) : Prop :=
  ∀ (s s1 : BeaconState) (signed_block : SignedBeaconBlock),
    Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s →
    process_slots Preset.mainnet o (process_epoch Preset.mainnet o) s signed_block.message.slot =
      .ok s1 →
    ∀ tab, get_total_active_balance Preset.mainnet s1 = .ok tab → tab ≤ 139000000000000000

/-- Every reachable state has the invariant. -/
theorem reachable_inv (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (init : BeaconState)
    (hffg : FfgInvariant Preset.mainnet init) (hrows : RowsOk init)
    (hd : ExitDelay Preset.mainnet init) (hx : ExitEpochsU64 init)
    (h0 : AfterEB Preset.mainnet init)
    (hsupply : SupplyBound o max_blobs_per_block GLOAS_FORK_EPOCH init) :
    ∀ s, Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s → ReachInv s := by
  intro s hr
  induction hr with
  | init => exact ReachInv.of_start hffg hrows hd hx h0
  | step signed_block validate_result hr hslot64 hbits h ih =>
    exact state_transition_reach o _ _ _ _ signed_block validate_result hslot64 hbits
      (fun s1 h1 => hsupply _ s1 signed_block hr h1) ih h

/-- On mainnet, from a start state with `FfgInvariant`, `RowsOk`, `ExitDelay` and `AfterEB`,
and under the supply bound,
every `state_transition` from a reachable state equals the one that checks `BoundaryOk`
before each `process_epoch`. So at every epoch boundary of every run, the finalized epoch is
at most the current epoch, and `effective_balance ≤ 256 * balance` for each validator that is
eligible for rewards. -/
theorem reachable_boundaryOk (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (init : BeaconState)
    (hffg : FfgInvariant Preset.mainnet init) (hrows : RowsOk init)
    (hd : ExitDelay Preset.mainnet init) (hx : ExitEpochsU64 init)
    (h0 : AfterEB Preset.mainnet init)
    (hsupply : SupplyBound o max_blobs_per_block GLOAS_FORK_EPOCH init)
    (s : BeaconState) (hr : Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s)
    (signed_block : SignedBeaconBlock) (validate_result : Bool)
    (hslot64 : signed_block.message.slot < 2 ^ 64) :
    state_transition_checked o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
        validate_result =
      state_transition Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
        validate_result :=
  state_transition_checked_eq o _ _ s signed_block validate_result hslot64
    (reachable_inv o _ _ init hffg hrows hd hx h0 hsupply s hr)

/-- After `process_justification_and_finalization` on a state with `BoundaryOk`, the state where
`EpochEntry` applies: the checkpoints are below the current epoch, so the finalized epoch is,
and `RowsOk` and `EntryEB` still hold. The step keeps the slot, validators and balances. -/
theorem BoundaryOk.of_justification {s s' : BeaconState} (hb : BoundaryOk s)
    (h : process_justification_and_finalization Preset.mainnet s = .ok s') :
    FfgInvariantAfter Preset.mainnet s' ∧
      s'.finalized_checkpoint.epoch ≤ s'.slot / Preset.mainnet.SLOTS_PER_EPOCH ∧ RowsOk s' ∧
      EntryEB s' := by
  obtain ⟨⟨hv, hbal, -⟩, hsl⟩ := process_justification_and_finalization_frame _ s s' h
  have hafter := (process_justification_and_finalization_ffg _ s s' h hb.1).1
  refine ⟨hafter, hafter.checkpointsBelow.1,
    (process_justification_and_finalization_len _ s s' h).rowsOk hb.2.1, ?_⟩
  have he := hb.2.2
  unfold EntryEB at he ⊢
  rw [hv, hbal, hsl]
  exact he

/-! ## Any epoch function that agrees on `BoundaryOk` states -/

/-- `SameOk` goes through a bind. -/
theorem sameOk_bind {α β : Type} {x y : SpecM α} {k k' : α → SpecM β} (hxy : SameOk x y)
    (hk : ∀ a, y = .ok a → SameOk (k a) (k' a)) : SameOk (x >>= k) (y >>= k') := by
  intro v
  cases hy : y with
  | error e =>
    cases hx : x with
    | error e' => simp [bind, Except.bind]
    | ok a =>
      have := (hxy a).mp hx
      rw [hy] at this
      cases this
  | ok a =>
    have hx := (hxy a).mpr hy
    simp only [hx, bind, Except.bind]
    exact hk a hy v

/-- The `process_slots` loop with an epoch function `f` has the same `ok` results as the loop
with the real `process_epoch`, if `f` has the same `ok` results on states with `C`, and `P`
gives `C` before each `process_epoch`. -/
theorem process_slots_loop_sameOk (p : Preset) (o : Oracle) (slot : Slot)
    (P Q C : BeaconState → Prop)
    (hslot : ∀ s s', process_slot p o s = .ok s' → s.slot < slot → P s → P s')
    (hepoch : ∀ s s', s.slot < slot → (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 →
      process_epoch p o s = .ok s' → P s → Q s')
    (hcheck : ∀ s, s.slot < slot → (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 → P s → C s)
    (hbump_epoch : ∀ s, (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 → Q s →
      P { s with slot := s.slot + 1 })
    (hbump : ∀ s, s.slot < slot → (s.slot + 1) % p.SLOTS_PER_EPOCH ≠ 0 → P s →
      P { s with slot := s.slot + 1 })
    (f : BeaconState → SpecM BeaconState) (hf : ∀ s, C s → SameOk (f s) (process_epoch p o s)) :
    ∀ fuel st, P st →
      SameOk (process_slots_loop p o f slot fuel st)
        (process_slots_loop p o (process_epoch p o) slot fuel st) := by
  intro fuel
  induction fuel with
  | zero => intro st _; exact SameOk.refl _
  | succ n ih =>
    intro st hP
    by_cases hlt : st.slot < slot
    · simp only [process_slots_loop, hlt, if_true]
      refine sameOk_bind (SameOk.refl _) fun s1 h1 => ?_
      have hs1 := (process_slot_frame p o _ _ h1).2
      have hP1 := hslot _ _ h1 hlt hP
      have hlt1 : s1.slot < slot := hs1 ▸ hlt
      refine sameOk_bind (SameOk.refl _) fun a ha => ?_
      refine sameOk_bind (SameOk.refl _) fun m hm => ?_
      have hm' : m = (s1.slot + 1) % p.SLOTS_PER_EPOCH := by
        rw [← uint64Add_ok ha]; exact uint64Mod_ok'' hm
      by_cases hz : (m == 0) = true
      · have hz' : (s1.slot + 1) % p.SLOTS_PER_EPOCH = 0 := by rw [← hm']; simpa using hz
        rw [if_pos hz, if_pos hz]
        refine sameOk_bind (hf s1 (hcheck s1 hlt1 hz' hP1)) fun s2 h2 => ?_
        refine sameOk_bind (SameOk.refl _) fun c hc => ?_
        rw [uint64Add_ok hc]
        exact ih _ (hbump_epoch s2 ((process_epoch_slot p o _ _ h2) ▸ hz')
          (hepoch _ _ hlt1 hz' h2 hP1))
      · have hz' : (s1.slot + 1) % p.SLOTS_PER_EPOCH ≠ 0 := by rw [← hm']; simpa using hz
        rw [if_neg hz, if_neg hz]
        refine sameOk_bind (SameOk.refl _) fun s2 h2 => ?_
        cases h2
        refine sameOk_bind (SameOk.refl _) fun c hc => ?_
        rw [uint64Add_ok hc]
        exact ih _ (hbump s1 hlt1 hz' hP1)
    · simp only [process_slots_loop, hlt, if_false]
      exact SameOk.refl _

/-- `state_transition` with an epoch function `f` in place of `process_epoch`. -/
def state_transition_with (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (f : BeaconState → SpecM BeaconState) (state : BeaconState)
    (signed_block : SignedBeaconBlock) (validate_result : Bool := true) : SpecM BeaconState := do
  let block := signed_block.message
  let state ← process_slots Preset.mainnet o f state block.slot
  if validate_result then
    if ¬ (← verify_block_signature Preset.mainnet o state signed_block) then
      throw .assertionFailed
  let state ← process_block Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH state block
  if validate_result then
    if ¬ block.state_root = o.hash_tree_root_BeaconState state then throw .assertionFailed
  pure state

/-- On mainnet, from a reachable state, `state_transition` with any epoch function `f` that has
the same `ok` results as `process_epoch` on states with `BoundaryOk` has the same `ok` results
as `state_transition`. So a Lighthouse epoch function that matches the spec on `BoundaryOk`
states matches it on every reachable run. -/
theorem reachable_state_transition_sameOk (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (init : BeaconState)
    (hffg : FfgInvariant Preset.mainnet init) (hrows : RowsOk init)
    (hd : ExitDelay Preset.mainnet init) (hx : ExitEpochsU64 init)
    (h0 : AfterEB Preset.mainnet init)
    (hsupply : SupplyBound o max_blobs_per_block GLOAS_FORK_EPOCH init)
    (f : BeaconState → SpecM BeaconState)
    (hf : ∀ t, BoundaryOk t → SameOk (f t) (process_epoch Preset.mainnet o t))
    (s : BeaconState) (hr : Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s)
    (signed_block : SignedBeaconBlock) (validate_result : Bool)
    (hslot64 : signed_block.message.slot < 2 ^ 64) :
    SameOk (state_transition_with o max_blobs_per_block GLOAS_FORK_EPOCH f s signed_block
        validate_result)
      (state_transition Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
        validate_result) := by
  have hs := reachable_inv o _ _ init hffg hrows hd hx h0 hsupply s hr
  have hslots : SameOk (process_slots Preset.mainnet o f s signed_block.message.slot)
      (process_slots Preset.mainnet o (process_epoch Preset.mainnet o) s
        signed_block.message.slot) := by
    unfold process_slots
    simp only [bind, Except.bind, pure, Except.pure]
    split
    · exact SameOk.refl _
    · have h := loopInv_steps o _ s.slot hslot64
      exact process_slots_loop_sameOk _ o _ (LoopInv s.slot) (PostEpoch s.slot) BoundaryOk
        h.1 h.2.1 h.2.2.1 h.2.2.2.1 h.2.2.2.2 f hf _ s hs
  unfold state_transition_with state_transition
  dsimp only
  exact sameOk_bind hslots fun _ _ => SameOk.refl _

/-! ## Epoch inputs with the economic facts -/

/-- The states that the `process_slots` loop reaches from `s` on mainnet, one slot at a time.
Each step runs `process_slot`, then `process_epoch` at the last slot of an epoch, then moves
to the next slot. -/
inductive SlotReach (o : Oracle) (s : BeaconState) : BeaconState → Prop
  | refl : SlotReach o s s
  | skip {t t1 : BeaconState} : SlotReach o s t → process_slot Preset.mainnet o t = .ok t1 →
      (t1.slot + 1) % Preset.mainnet.SLOTS_PER_EPOCH ≠ 0 →
      SlotReach o s { t1 with slot := t1.slot + 1 }
  | epoch {t t1 t2 : BeaconState} : SlotReach o s t → process_slot Preset.mainnet o t = .ok t1 →
      (t1.slot + 1) % Preset.mainnet.SLOTS_PER_EPOCH = 0 →
      process_epoch Preset.mainnet o t1 = .ok t2 →
      SlotReach o s { t2 with slot := t2.slot + 1 }

/-- The economic hypothesis on a run: every `process_epoch` input `t1` of a reachable run
satisfies `Econ`. An input comes from a state `t` that the slot loop reaches, by
`process_slot`, at the last slot of an epoch. -/
def EpochEcon (o : Oracle) (max_blobs_per_block : Epoch → Uint64) (GLOAS_FORK_EPOCH : Epoch)
    (init : BeaconState) (Econ : BeaconState → Prop) : Prop :=
  ∀ (s t t1 : BeaconState), Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s →
    SlotReach o s t → process_slot Preset.mainnet o t = .ok t1 →
    (t1.slot + 1) % Preset.mainnet.SLOTS_PER_EPOCH = 0 → Econ t1

/-- The `process_slots` loop with an epoch function `f` has the same `ok` results as the loop
with the real `process_epoch`. `P` holds at each loop state, `M` after `process_slot`, and `Q`
after `process_epoch`. `M` gives the check `C`, and `f` has the same `ok` results as
`process_epoch` on states with `C`. -/
theorem process_slots_loop_sameOk' (p : Preset) (o : Oracle) (slot : Slot)
    (P M Q C : BeaconState → Prop)
    (hslot : ∀ s s', process_slot p o s = .ok s' → s.slot < slot → P s → M s')
    (hepoch : ∀ s s', s.slot < slot → (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 →
      process_epoch p o s = .ok s' → M s → Q s')
    (hcheck : ∀ s, s.slot < slot → (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 → M s → C s)
    (hbump_epoch : ∀ s, (s.slot + 1) % p.SLOTS_PER_EPOCH = 0 → Q s →
      P { s with slot := s.slot + 1 })
    (hbump : ∀ s, s.slot < slot → (s.slot + 1) % p.SLOTS_PER_EPOCH ≠ 0 → M s →
      P { s with slot := s.slot + 1 })
    (f : BeaconState → SpecM BeaconState) (hf : ∀ s, C s → SameOk (f s) (process_epoch p o s)) :
    ∀ fuel st, P st →
      SameOk (process_slots_loop p o f slot fuel st)
        (process_slots_loop p o (process_epoch p o) slot fuel st) := by
  intro fuel
  induction fuel with
  | zero => intro st _; exact SameOk.refl _
  | succ n ih =>
    intro st hP
    by_cases hlt : st.slot < slot
    · simp only [process_slots_loop, hlt, if_true]
      refine sameOk_bind (SameOk.refl _) fun s1 h1 => ?_
      have hs1 := (process_slot_frame p o _ _ h1).2
      have hM1 := hslot _ _ h1 hlt hP
      have hlt1 : s1.slot < slot := hs1 ▸ hlt
      refine sameOk_bind (SameOk.refl _) fun a ha => ?_
      refine sameOk_bind (SameOk.refl _) fun m hm => ?_
      have hm' : m = (s1.slot + 1) % p.SLOTS_PER_EPOCH := by
        rw [← uint64Add_ok ha]; exact uint64Mod_ok'' hm
      by_cases hz : (m == 0) = true
      · have hz' : (s1.slot + 1) % p.SLOTS_PER_EPOCH = 0 := by rw [← hm']; simpa using hz
        rw [if_pos hz, if_pos hz]
        refine sameOk_bind (hf s1 (hcheck s1 hlt1 hz' hM1)) fun s2 h2 => ?_
        refine sameOk_bind (SameOk.refl _) fun c hc => ?_
        rw [uint64Add_ok hc]
        exact ih _ (hbump_epoch s2 ((process_epoch_slot p o _ _ h2) ▸ hz')
          (hepoch _ _ hlt1 hz' h2 hM1))
      · have hz' : (s1.slot + 1) % p.SLOTS_PER_EPOCH ≠ 0 := by rw [← hm']; simpa using hz
        rw [if_neg hz, if_neg hz]
        refine sameOk_bind (SameOk.refl _) fun s2 h2 => ?_
        cases h2
        refine sameOk_bind (SameOk.refl _) fun c hc => ?_
        rw [uint64Add_ok hc]
        exact ih _ (hbump s1 hlt1 hz' hM1)
    · simp only [process_slots_loop, hlt, if_false]
      exact SameOk.refl _

/-- The facts at each `process_epoch` input of a reachable run: `BoundaryOk`, a `u64` slot,
`EBOk`, `ExitEpochsU64` and the economic facts `Econ`. -/
def EpochInputOk (Econ : BeaconState → Prop) (t : BeaconState) : Prop :=
  BoundaryOk t ∧ t.slot < 2 ^ 64 ∧ EBOk Preset.mainnet t ∧ ExitEpochsU64 t ∧ Econ t

/-- On mainnet, from a reachable state, `state_transition` with any epoch function `f` has the
same `ok` results as `state_transition`, if `f` has the same `ok` results as `process_epoch`
on every state with `EpochInputOk Econ`, and `Econ` holds at every `process_epoch` input of
the run (`EpochEcon`). -/
theorem reachable_state_transition_sameOk' (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (init : BeaconState)
    (hffg : FfgInvariant Preset.mainnet init) (hrows : RowsOk init)
    (hd : ExitDelay Preset.mainnet init) (hx : ExitEpochsU64 init)
    (h0 : AfterEB Preset.mainnet init)
    (hsupply : SupplyBound o max_blobs_per_block GLOAS_FORK_EPOCH init)
    (Econ : BeaconState → Prop)
    (hecon : EpochEcon o max_blobs_per_block GLOAS_FORK_EPOCH init Econ)
    (f : BeaconState → SpecM BeaconState)
    (hf : ∀ t, EpochInputOk Econ t → SameOk (f t) (process_epoch Preset.mainnet o t))
    (s : BeaconState) (hr : Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s)
    (signed_block : SignedBeaconBlock) (validate_result : Bool)
    (hslot64 : signed_block.message.slot < 2 ^ 64) :
    SameOk (state_transition_with o max_blobs_per_block GLOAS_FORK_EPOCH f s signed_block
        validate_result)
      (state_transition Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
        validate_result) := by
  have hs := reachable_inv o _ _ init hffg hrows hd hx h0 hsupply s hr
  have hslots : SameOk (process_slots Preset.mainnet o f s signed_block.message.slot)
      (process_slots Preset.mainnet o (process_epoch Preset.mainnet o) s
        signed_block.message.slot) := by
    unfold process_slots
    simp only [bind, Except.bind, pure, Except.pure]
    split
    · exact SameOk.refl _
    · have h := loopInv_steps o signed_block.message.slot s.slot hslot64
      refine process_slots_loop_sameOk' _ o _
        (fun t => LoopInv s.slot t ∧ SlotReach o s t)
        (fun t => LoopInv s.slot t ∧ ∃ t0, SlotReach o s t0 ∧
          process_slot Preset.mainnet o t0 = .ok t)
        (fun t => PostEpoch s.slot t ∧ ∃ t0 t1, SlotReach o s t0 ∧
          process_slot Preset.mainnet o t0 = .ok t1 ∧
          (t1.slot + 1) % Preset.mainnet.SLOTS_PER_EPOCH = 0 ∧
          process_epoch Preset.mainnet o t1 = .ok t)
        (EpochInputOk Econ) ?_ ?_ ?_ ?_ ?_ f hf _ s ⟨hs, .refl⟩
      · intro t t' ht hlt hP
        exact ⟨h.1 t t' ht hlt hP.1, t, hP.2, ht⟩
      · intro t t' hlt hz he hM
        obtain ⟨t0, h0', ht0⟩ := hM.2
        exact ⟨h.2.1 t t' hlt hz he hM.1, t0, t, h0', ht0, hz, he⟩
      · intro t hlt hz hM
        obtain ⟨hinv, t0, h0', ht0⟩ := hM
        have hok := h.2.2.1 t hlt hz hinv
        obtain ⟨-, -, -, hx', -, s0, n, hA, hb, -⟩ := hinv
        exact ⟨hok, by simp only [Slot] at *; omega, hb.ebOk hA, hx',
          hecon s t0 t hr h0' ht0 hz⟩
      · intro t hz hQ
        obtain ⟨hpost, t0, t1, h0', ht0, hz1, he⟩ := hQ
        exact ⟨h.2.2.2.1 t hz hpost, .epoch h0' ht0 hz1 he⟩
      · intro t hlt hz hM
        obtain ⟨hinv, t0, h0', ht0⟩ := hM
        exact ⟨h.2.2.2.2 t hlt hz hinv, .skip h0' ht0 hz⟩
  unfold state_transition_with state_transition
  dsimp only
  exact sameOk_bind hslots fun _ _ => SameOk.refl _

/-! ## Unique pubkeys and in-range consolidations -/

/-- Every reachable state has unique pubkeys and in-range pending consolidations, if the start
state has them. -/
theorem reachable_tail (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (init : BeaconState) (hpk : PubkeysUnique init)
    (hcr : ConsolidationsInRange init) :
    ∀ s, Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s →
      PubkeysUnique s ∧ ConsolidationsInRange s := by
  intro s hr
  induction hr with
  | init => exact ⟨hpk, hcr⟩
  | step signed_block validate_result _ _ _ h ih =>
    exact ⟨state_transition_pubkeysUnique _ o _ _ _ _ _ _ ih.1 h,
      state_transition_inRange _ o _ _ _ _ _ _ ih.2 h⟩

/-- The slot loop keeps unique pubkeys and in-range pending consolidations. -/
theorem SlotReach.tail {o : Oracle} {s t : BeaconState} (hr : SlotReach o s t)
    (hs : PubkeysUnique s ∧ ConsolidationsInRange s) :
    PubkeysUnique t ∧ ConsolidationsInRange t := by
  induction hr with
  | refl => exact hs
  | skip _ ht _ ih =>
    exact ⟨(PkSame.of_eq (process_slot_frame _ o _ _ ht).1.1).pubkeysUnique ih.1,
      (process_slot_cstep _ o _ _ ht).inRange ih.2⟩
  | epoch _ ht _ he ih =>
    have h1 : PubkeysUnique _ ∧ ConsolidationsInRange _ :=
      ⟨(PkSame.of_eq (process_slot_frame _ o _ _ ht).1.1).pubkeysUnique ih.1,
        (process_slot_cstep _ o _ _ ht).inRange ih.2⟩
    have h2 := process_epoch_pubkeysUnique _ o _ _ he h1.1
    have h3 := process_epoch_inRange _ o _ _ he h1.2
    exact ⟨h2, h3⟩

/-- The facts at each `process_epoch` input, with unique pubkeys and in-range pending
consolidations. -/
def EpochInputOk' (Econ : BeaconState → Prop) (t : BeaconState) : Prop :=
  EpochInputOk Econ t ∧ PubkeysUnique t ∧ ConsolidationsInRange t

/-- `reachable_state_transition_sameOk'` for an epoch function `f` that needs, in addition,
unique pubkeys and in-range pending consolidations at its input. The start state has them. -/
theorem reachable_state_transition_sameOk'' (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (init : BeaconState)
    (hffg : FfgInvariant Preset.mainnet init) (hrows : RowsOk init)
    (hd : ExitDelay Preset.mainnet init) (hx : ExitEpochsU64 init)
    (h0 : AfterEB Preset.mainnet init) (hpk : PubkeysUnique init)
    (hcr : ConsolidationsInRange init)
    (hsupply : SupplyBound o max_blobs_per_block GLOAS_FORK_EPOCH init)
    (Econ : BeaconState → Prop)
    (hecon : EpochEcon o max_blobs_per_block GLOAS_FORK_EPOCH init Econ)
    (f : BeaconState → SpecM BeaconState)
    (hf : ∀ t, EpochInputOk' Econ t → SameOk (f t) (process_epoch Preset.mainnet o t))
    (s : BeaconState) (hr : Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s)
    (signed_block : SignedBeaconBlock) (validate_result : Bool)
    (hslot64 : signed_block.message.slot < 2 ^ 64) :
    SameOk (state_transition_with o max_blobs_per_block GLOAS_FORK_EPOCH f s signed_block
        validate_result)
      (state_transition Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
        validate_result) := by
  have hecon' : EpochEcon o max_blobs_per_block GLOAS_FORK_EPOCH init
      (fun t => Econ t ∧ PubkeysUnique t ∧ ConsolidationsInRange t) := by
    intro s' t t1 hr' hreach ht hz
    have htail := hreach.tail (reachable_tail o _ _ init hpk hcr s' hr')
    exact ⟨hecon s' t t1 hr' hreach ht hz,
      (PkSame.of_eq (process_slot_frame _ o _ _ ht).1.1).pubkeysUnique htail.1,
      (process_slot_cstep _ o _ _ ht).inRange htail.2⟩
  exact reachable_state_transition_sameOk' o _ _ init hffg hrows hd hx h0 hsupply _ hecon' f
    (fun t ⟨hb, hslot, heb, hex, hec, hp, hc⟩ => hf t ⟨⟨hb, hslot, heb, hex, hec⟩, hp, hc⟩)
    s hr signed_block validate_result hslot64

/-! ## Withdrawable epochs -/

/-- Every reachable state has `u64` withdrawable epochs, if the start state has them. -/
theorem reachable_withdrawableU64 (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (init : BeaconState) (hw : WithdrawableU64 init) :
    ∀ s, Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s → WithdrawableU64 s := by
  intro s hr
  induction hr with
  | init => exact hw
  | step signed_block validate_result _ _ _ h ih =>
    exact state_transition_withdrawableU64 _ o _ _ _ _ _ _ ih h

/-- The slot loop keeps `ExitDelay` and `WithdrawableU64`. -/
theorem SlotReach.withdrawable {o : Oracle} {s t : BeaconState} (hr : SlotReach o s t)
    (hs : ExitDelay Preset.mainnet s ∧ WithdrawableU64 s) :
    ExitDelay Preset.mainnet t ∧ WithdrawableU64 t := by
  induction hr with
  | refl => exact hs
  | skip _ ht _ ih =>
    have hv := (process_slot_frame _ o _ _ ht).1.1
    have h1 := ExitDelay.of_validators_eq hv ih.1
    have h2 := WithdrawableU64.of_validators_eq hv ih.2
    exact ⟨h1, h2⟩
  | epoch _ ht _ he ih =>
    have hv := (process_slot_frame _ o _ _ ht).1.1
    have h1 := process_epoch_exitDelay _ o _ _ he (ExitDelay.of_validators_eq hv ih.1)
    have h2 := process_epoch_withdrawableU64 _ o _ _ he (WithdrawableU64.of_validators_eq hv ih.2)
    exact ⟨h1, h2⟩

/-- The facts at each `process_epoch` input, with `WithdrawableU64` and `WithdrawableValid`. -/
def EpochInputOk'' (Econ : BeaconState → Prop) (t : BeaconState) : Prop :=
  EpochInputOk' Econ t ∧ WithdrawableU64 t ∧ WithdrawableValid t

/-- `reachable_state_transition_sameOk''` for an epoch function `f` that needs, in addition,
`u64` withdrawable epochs and `WithdrawableValid` at its input. The start state has
`WithdrawableU64`. -/
theorem reachable_state_transition_sameOk''' (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (init : BeaconState)
    (hffg : FfgInvariant Preset.mainnet init) (hrows : RowsOk init)
    (hd : ExitDelay Preset.mainnet init) (hx : ExitEpochsU64 init)
    (h0 : AfterEB Preset.mainnet init) (hpk : PubkeysUnique init)
    (hcr : ConsolidationsInRange init) (hw : WithdrawableU64 init)
    (hsupply : SupplyBound o max_blobs_per_block GLOAS_FORK_EPOCH init)
    (Econ : BeaconState → Prop)
    (hecon : EpochEcon o max_blobs_per_block GLOAS_FORK_EPOCH init Econ)
    (f : BeaconState → SpecM BeaconState)
    (hf : ∀ t, EpochInputOk'' Econ t → SameOk (f t) (process_epoch Preset.mainnet o t))
    (s : BeaconState) (hr : Reachable o max_blobs_per_block GLOAS_FORK_EPOCH init s)
    (signed_block : SignedBeaconBlock) (validate_result : Bool)
    (hslot64 : signed_block.message.slot < 2 ^ 64) :
    SameOk (state_transition_with o max_blobs_per_block GLOAS_FORK_EPOCH f s signed_block
        validate_result)
      (state_transition Preset.mainnet o max_blobs_per_block GLOAS_FORK_EPOCH s signed_block
        validate_result) := by
  have hecon' : EpochEcon o max_blobs_per_block GLOAS_FORK_EPOCH init
      (fun t => Econ t ∧ WithdrawableU64 t ∧ WithdrawableValid t) := by
    intro s' t t1 hr' hreach ht hz
    have hstart : ExitDelay Preset.mainnet s' ∧ WithdrawableU64 s' :=
      ⟨(reachable_inv o _ _ init hffg hrows hd hx h0 hsupply s' hr').2.2.1,
        reachable_withdrawableU64 o _ _ init hw s' hr'⟩
    have hwd := hreach.withdrawable hstart
    have hv := (process_slot_frame _ o _ _ ht).1.1
    have h1 := ExitDelay.of_validators_eq hv hwd.1
    have h2 := WithdrawableU64.of_validators_eq hv hwd.2
    exact ⟨hecon s' t t1 hr' hreach ht hz, h2, withdrawableValid_of h1 h2⟩
  exact reachable_state_transition_sameOk'' o _ _ init hffg hrows hd hx h0 hpk hcr hsupply _
    hecon' f
    (fun t ⟨⟨hb, hslot, heb, hex, hec, hwu, hwv⟩, hp, hc⟩ =>
      hf t ⟨⟨⟨hb, hslot, heb, hex, hec⟩, hp, hc⟩, hwu, hwv⟩)
    s hr signed_block validate_result hslot64

end EpochProofs.Spec
