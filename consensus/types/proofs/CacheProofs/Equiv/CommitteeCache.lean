import CacheProofs.Generated
import CacheProofs.Spec.CommitteeCache

/-!
# `CommitteeCache` equals the reference

Main theorems:

- `committee_count_per_slot_equiv`: the builder computes `get_committee_count_per_slot`.
- `get_beacon_committee_equiv`: `get_beacon_committee` returns the spec committee. It does not
  return `None` for a valid slot and index.
- `shuffling_positions_spec`, `shuffled_position_inverse`: the positions that the builder
  stores invert the shuffling.
- `get_attestation_duties_equiv`: `get_attestation_duties` agrees with `get_committee_assignment`.

`IsShuffling` states that the cached shuffling is the spec shuffling. The `lh*` definitions
model the glue in `committee_cache.rs`. They are written by hand.
-/

open Aeneas Aeneas.Std Aeneas.Std.WP Result types

namespace CacheProofs.CommitteeCache

abbrev ArithError := safe_arith.ArithError
abbrev RResult := core.result.Result

@[simp] theorem branch_ok {T E : Type} (v : T) :
    core.result.Result.Insts.CoreOpsTry.branch (core.result.Result.Ok v : RResult T E) =
      ok (.Continue v) := rfl

@[simp] theorem branch_err {T E : Type} (e : E) :
    core.result.Result.Insts.CoreOpsTry.branch (core.result.Result.Err e : RResult T E) =
      ok (.Break (.Err e)) := rfl

@[simp] theorem from_residual_err {T E : Type} (e : E) :
    core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual T
      (core.convert.FromSame E) (.Err e) = ok (.Err e) := rfl

theorem safe_add_eq (x y : Usize) :
    ∃ r, Usize.Insts.Safe_arithSafeArithUsize.safe_add x y = ok r ∧
      (match r with
       | .Ok z => z.val = x.val + y.val ∧ x.val + y.val ≤ Usize.max
       | .Err e => e = .Overflow ∧ Usize.max < x.val + y.val) := by
  unfold Usize.Insts.Safe_arithSafeArithUsize.safe_add
  have hs := Usize.checked_add_bv_spec x y
  cases hc : Usize.checked_add x y with
  | none => simp only [hc] at hs; exact ⟨_, rfl, rfl, hs⟩
  | some z => simp only [hc] at hs; exact ⟨_, rfl, hs.2.1, hs.1⟩

theorem safe_mul_eq (x y : Usize) :
    ∃ r, Usize.Insts.Safe_arithSafeArithUsize.safe_mul x y = ok r ∧
      (match r with
       | .Ok z => z.val = x.val * y.val ∧ x.val * y.val ≤ Usize.max
       | .Err e => e = .Overflow ∧ Usize.max < x.val * y.val) := by
  unfold Usize.Insts.Safe_arithSafeArithUsize.safe_mul
  have hs := Usize.checked_mul_bv_spec x y
  cases hc : Usize.checked_mul x y with
  | none => simp only [hc] at hs; exact ⟨_, rfl, rfl, hs⟩
  | some z => simp only [hc] at hs; exact ⟨_, rfl, hs.2.1, hs.1⟩

theorem safe_sub_eq (x y : Usize) :
    ∃ r, Usize.Insts.Safe_arithSafeArithUsize.safe_sub x y = ok r ∧
      (match r with
       | .Ok z => z.val = x.val - y.val ∧ y.val ≤ x.val
       | .Err e => e = .Overflow ∧ x.val < y.val) := by
  unfold Usize.Insts.Safe_arithSafeArithUsize.safe_sub
  have hs := Usize.checked_sub_bv_spec x y
  cases hc : Usize.checked_sub x y with
  | none => simp only [hc] at hs; exact ⟨_, rfl, rfl, hs⟩
  | some z => simp only [hc] at hs; exact ⟨_, rfl, hs.2.1, hs.1⟩

theorem safe_div_eq (x y : Usize) :
    ∃ r, Usize.Insts.Safe_arithSafeArithUsize.safe_div x y = ok r ∧
      (match r with
       | .Ok z => z.val = x.val / y.val ∧ y.val ≠ 0
       | .Err e => e = .DivisionByZero ∧ y.val = 0) := by
  unfold Usize.Insts.Safe_arithSafeArithUsize.safe_div
  have hs := Usize.checked_div_bv_spec x y
  cases hc : Usize.checked_div x y with
  | none => simp only [hc] at hs; exact ⟨_, rfl, rfl, hs⟩
  | some z => simp only [hc] at hs; exact ⟨_, rfl, hs.2.1, hs.1⟩

theorem safe_rem_eq (x y : Usize) :
    ∃ r, Usize.Insts.Safe_arithSafeArithUsize.safe_rem x y = ok r ∧
      (match r with
       | .Ok z => z.val = x.val % y.val ∧ y.val ≠ 0
       | .Err e => e = .DivisionByZero ∧ y.val = 0) := by
  unfold Usize.Insts.Safe_arithSafeArithUsize.safe_rem
  have hs := Usize.checked_rem_bv_spec x y
  cases hc : Usize.checked_rem x y with
  | none => simp only [hc] at hs; exact ⟨_, rfl, rfl, hs⟩
  | some z => simp only [hc] at hs; exact ⟨_, rfl, hs.2.1, hs.1⟩


namespace Spec
export CacheProofs.Spec.CommitteeCache (Preset get_committee_count_per_slot uint64Div uint64Mul uint64Mod)
end Spec

def absArithError : ArithError → CacheProofs.Spec.SpecError
  | .Overflow => .overflow
  | .DivisionByZero => .divisionByZero

def absResult {α β : Type} (f : α → β) : RResult α ArithError → CacheProofs.Spec.SpecM β
  | .Ok x => .ok (f x)
  | .Err e => .error (absArithError e)

open state.committee_assignment in
theorem committee_count_per_slot_equiv (p : Spec.Preset)
    (active_count spe max_cps target : Usize)
    (hspe : spe.val = p.SLOTS_PER_EPOCH) (hmax : max_cps.val = p.MAX_COMMITTEES_PER_SLOT)
    (htarget : target.val = p.TARGET_COMMITTEE_SIZE) :
    committee_count_per_slot active_count spe max_cps target ⦃ r =>
      absResult (·.val) r = Spec.get_committee_count_per_slot p active_count.val ⦄ := by
  unfold committee_count_per_slot
  obtain ⟨r1, h1, hr1⟩ := safe_div_eq active_count spe
  rw [h1]
  cases r1 with
  | Err e =>
    obtain ⟨rfl, h0⟩ := hr1
    simp [Spec.get_committee_count_per_slot, Spec.uint64Div, ← hspe, h0, absResult,
      absArithError]; rfl
  | Ok a =>
    obtain ⟨ha, h0⟩ := hr1
    obtain ⟨r2, h2, hr2⟩ := safe_div_eq a target
    simp only [bind_tc_ok, branch_ok, h2]
    cases r2 with
    | Err e =>
      obtain ⟨rfl, h0'⟩ := hr2
      simp [Spec.get_committee_count_per_slot, Spec.uint64Div, ← hspe, ← htarget, h0, h0',
        absResult, absArithError]; rfl
    | Ok b =>
      obtain ⟨hb, h0'⟩ := hr2
      simp [Spec.get_committee_count_per_slot, Spec.uint64Div, ← hspe, ← htarget, h0, h0',
        absResult, hb, ha, hmax]; rfl


open state.committee_assignment

theorem committee_index_in_epoch_ok (slot spe cps index : Usize) (hspe : 0 < spe.val)
    (hfit : slot.val % spe.val * cps.val + index.val ≤ Usize.max) :
    ∃ z : Usize, committee_index_in_epoch slot spe cps index = ok (.Ok z) ∧
      z.val = slot.val % spe.val * cps.val + index.val := by
  unfold committee_index_in_epoch
  obtain ⟨r1, h1, hr1⟩ := safe_rem_eq slot spe
  rw [h1]
  cases r1 with
  | Err e => omega
  | Ok a =>
    obtain ⟨ha, -⟩ := hr1
    obtain ⟨r2, h2, hr2⟩ := safe_mul_eq a cps
    simp only [bind_tc_ok, branch_ok, h2]
    cases r2 with
    | Err e =>
      rw [ha] at hr2
      have := hr2.2
      have : slot.val % spe.val * cps.val ≤ Usize.max := by omega
      omega
    | Ok b =>
      obtain ⟨hb, -⟩ := hr2
      obtain ⟨r3, h3, hr3⟩ := safe_add_eq b index
      simp only [bind_tc_ok, branch_ok, h3]
      cases r3 with
      | Err e => rw [hb, ha] at hr3; omega
      | Ok c => exact ⟨c, rfl, by rw [hr3.1, hb, ha]⟩

theorem epoch_committee_count_ok (cps spe : Usize) (hfit : cps.val * spe.val ≤ Usize.max) :
    ∃ z : Usize, epoch_committee_count cps spe = ok (.Ok z) ∧ z.val = cps.val * spe.val := by
  unfold epoch_committee_count
  obtain ⟨r1, h1, hr1⟩ := safe_mul_eq cps spe
  rw [h1]
  cases r1 with
  | Err e => omega
  | Ok a => exact ⟨a, rfl, hr1.1⟩

theorem committee_range_in_epoch_none (c k n : Usize) (h : c.val = 0 ∨ c.val ≤ k.val) :
    committee_range_in_epoch c k n = ok (.Ok none) := by
  unfold committee_range_in_epoch
  rcases h with h | h
  · have : c = 0#usize := by scalar_tac
    simp [this]
  · by_cases h0 : c = 0#usize
    · simp [h0]
    · have : k ≥ c := by scalar_tac
      simp only [h0, this, if_true, if_false]

theorem committee_range_in_epoch_ok (c k n : Usize) (hk : k.val < c.val)
    (hfit : n.val * c.val ≤ Usize.max) :
    ∃ s e : Usize, committee_range_in_epoch c k n = ok (.Ok (some (s, e))) ∧
      s.val = n.val * k.val / c.val ∧ e.val = n.val * (k.val + 1) / c.val := by
  unfold committee_range_in_epoch
  have h0 : c ≠ 0#usize := by intro h; subst h; simp at hk
  have hge : ¬ k ≥ c := by scalar_tac
  simp only [h0, hge, if_false]
  have hmul : n.val * (k.val + 1) ≤ n.val * c.val := Nat.mul_le_mul_left _ hk
  obtain ⟨r1, h1, hr1⟩ := safe_mul_eq n k
  rw [h1]
  cases r1 with
  | Err e =>
    have : n.val * k.val ≤ n.val * (k.val + 1) := Nat.mul_le_mul_left _ (Nat.le_succ _)
    omega
  | Ok a =>
    obtain ⟨r2, h2, hr2⟩ := safe_div_eq a c
    simp only [bind_tc_ok, branch_ok, h2]
    cases r2 with
    | Err e => have : c.val ≠ 0 := by scalar_tac
               exact absurd hr2.2 this
    | Ok s =>
      obtain ⟨r3, h3, hr3⟩ := safe_add_eq k 1#usize
      simp only [bind_tc_ok, branch_ok, h3]
      cases r3 with
      | Err e => have := hr3.2; simp at this; scalar_tac
      | Ok k1 =>
        obtain ⟨r4, h4, hr4⟩ := safe_mul_eq n k1
        simp only [bind_tc_ok, branch_ok, h4]
        have hk1 : k1.val = k.val + 1 := by rw [hr3.1]; simp
        cases r4 with
        | Err e => rw [hk1] at hr4; omega
        | Ok m =>
          obtain ⟨r5, h5, hr5⟩ := safe_div_eq m c
          simp only [bind_tc_ok, branch_ok, h5]
          cases r5 with
          | Err e => exact absurd hr5.2 (by scalar_tac)
          | Ok e =>
            refine ⟨s, e, rfl, ?_, ?_⟩
            · rw [hr2.1, hr1.1]
            · rw [hr5.1, hr4.1, hk1]


@[simp] theorem except_ok_bind {ε α β : Type} (a : α) (f : α → Except ε β) :
    (Except.ok a >>= f) = f a := rfl

theorem usize_max_lt : Usize.max < 18446744073709551616 := by
  have h := Usize.max_succ_eq_pow
  rcases System.Platform.numBits_eq with h' | h' <;> rw [h'] at h <;> omega

open CacheProofs.Spec.CommitteeCache in
/-- Python `indices[compute_shuffled_index(i, len(indices), seed)]`. -/
def shuffledAt (p : Preset) (sha256 : List Nat → List Nat) (indices : List Nat) (seed : List Nat)
    (i : Nat) : CacheProofs.Spec.SpecM Nat := do
  listGet indices (← compute_shuffled_index p sha256 i indices.length seed)

/-- `shuffling` is the spec shuffling of `indices`: entry `i` is
`indices[compute_shuffled_index(i, len(indices), seed)]`. -/
def IsShuffling (p : Spec.Preset) (sha256 : List Nat → List Nat) (indices seed : List Nat)
    (shuffling : List Nat) : Prop :=
  shuffling.length = indices.length ∧
    ∀ i, i < indices.length → shuffledAt p sha256 indices seed i = .ok (shuffling.getD i 0)

theorem mapM_except_ok {α β ε : Type} (f : α → Except ε β) (g : α → β) (l : List α)
    (h : ∀ x ∈ l, f x = .ok (g x)) : l.mapM f = .ok (l.map g) := by
  induction l with
  | nil => rfl
  | cons a l ih =>
    rw [List.mapM_cons, h a (by simp), ih (fun x hx => h x (by simp [hx]))]
    rfl

theorem range'_map_getD (l : List Nat) (s k : Nat) (h : s + k ≤ l.length) :
    (List.range' s k).map (fun i => l.getD i 0) = (l.drop s).take k := by
  apply List.ext_getElem
  · simp; omega
  · intro i h1 h2
    simp at h1
    simp [List.getElem_range', List.getD_eq_getElem?_getD]
    rw [List.getElem?_eq_getElem (by omega)]
    simp

open CacheProofs.Spec.CommitteeCache in
/-- If `shuffling` is the spec shuffling, `compute_committee` is the slice `[start, end)`. -/
theorem compute_committee_slice (p : Preset) (sha256 : List Nat → List Nat)
    (indices seed shuffling : List Nat) (hshuf : IsShuffling p sha256 indices seed shuffling)
    (index count : Nat) (hindex : index < count)
    (hcount : count < CacheProofs.Spec.UINT64_SIZE)
    (hfit : indices.length * count < CacheProofs.Spec.UINT64_SIZE) :
    compute_committee p sha256 indices seed index count =
      .ok ((shuffling.drop (indices.length * index / count)).take
        (indices.length * (index + 1) / count - indices.length * index / count)) := by
  have hn := hshuf.1
  have h1 : indices.length * index ≤ indices.length * count :=
    Nat.mul_le_mul_left _ (Nat.le_of_lt hindex)
  have h2 : indices.length * (index + 1) ≤ indices.length * count := Nat.mul_le_mul_left _ hindex
  have hc0 : count ≠ 0 := by omega
  have hend : indices.length * (index + 1) / count ≤ indices.length := by
    apply Nat.div_le_of_le_mul; rw [Nat.mul_comm count]; exact h2
  have hse : indices.length * index / count ≤ indices.length * (index + 1) / count :=
    Nat.div_le_div_right (Nat.mul_le_mul_left _ (Nat.le_succ _))
  unfold compute_committee
  simp only [uint64Mul, uint64Div, CacheProofs.Spec.uint64Add, hc0,
    show indices.length * index < CacheProofs.Spec.UINT64_SIZE by omega,
    show index + 1 < CacheProofs.Spec.UINT64_SIZE by omega,
    if_true, if_false, pure_bind]
  simp only [show indices.length * (index + 1) < CacheProofs.Spec.UINT64_SIZE by omega, if_true,
    pure_bind]
  show List.mapM (shuffledAt p sha256 indices seed) _ = _
  rw [mapM_except_ok _ (fun i => shuffling.getD i 0)]
  · rw [range'_map_getD]; omega
  · intro x hx
    simp only [List.mem_range'_1] at hx
    exact hshuf.2 x (by omega)


/-- Rust `slice.get(start..end)`. -/
def lhSliceGet {α : Type} (l : List α) (start «end» : Nat) : Option (List α) :=
  if start ≤ «end» ∧ «end» ≤ l.length then some ((l.drop start).take («end» - start)) else none

/-- `CommitteeCache::get_beacon_committee`, modeled by hand. `slot` is `slot.as_usize()`.
`slot.epoch(slots_per_epoch)` is `slot / slots_per_epoch`. -/
def lhGetBeaconCommittee (initialized_epoch : Option Nat) (shuffling : Slice Usize)
    (committees_per_slot slots_per_epoch slot index : Usize) : Result (Option (List Usize)) :=
  if initialized_epoch = none ∨ initialized_epoch ≠ some (slot.val / slots_per_epoch.val)
      ∨ committees_per_slot.val ≤ index.val then
    ok none
  else do
    let r ← committee_index_in_epoch slot slots_per_epoch committees_per_slot index
    match r with
    | .Err _ => ok none
    | .Ok committee_index =>
      let r ← epoch_committee_count committees_per_slot slots_per_epoch
      match r with
      | .Err _ => ok none
      | .Ok count =>
        let r ← committee_range_in_epoch count committee_index (Slice.len shuffling)
        match r with
        | .Ok (some (start, «end»)) => ok (lhSliceGet shuffling.val start.val «end».val)
        | _ => ok none

open CacheProofs.Spec.CommitteeCache in
/-- If `shuffling` is the spec shuffling, `get_beacon_committee` is the slice of committee
`slot % SLOTS_PER_EPOCH * committees_per_slot + index`. -/
theorem get_beacon_committee_slice (p : Preset) (sha256 : List Nat → List Nat)
    (indices seed shuffling : List Nat) (hshuf : IsShuffling p sha256 indices seed shuffling)
    (committees_per_slot slot index : Nat)
    (hcps : get_committee_count_per_slot p indices.length = .ok committees_per_slot)
    (hspe0 : 0 < p.SLOTS_PER_EPOCH) (hindex : index < committees_per_slot)
    (hn : 0 < indices.length)
    (hfit : indices.length * (committees_per_slot * p.SLOTS_PER_EPOCH) < 18446744073709551616) :
    get_beacon_committee p sha256 indices seed slot index =
      let k := slot % p.SLOTS_PER_EPOCH * committees_per_slot + index
      let c := committees_per_slot * p.SLOTS_PER_EPOCH
      .ok ((shuffling.drop (indices.length * k / c)).take
        (indices.length * (k + 1) / c - indices.length * k / c)) := by
  have hcount : committees_per_slot * p.SLOTS_PER_EPOCH < 18446744073709551616 := by
    have := Nat.le_mul_of_pos_left (committees_per_slot * p.SLOTS_PER_EPOCH) hn
    omega
  have hmod : slot % p.SLOTS_PER_EPOCH < p.SLOTS_PER_EPOCH := Nat.mod_lt _ hspe0
  have hidx : slot % p.SLOTS_PER_EPOCH * committees_per_slot + index <
      committees_per_slot * p.SLOTS_PER_EPOCH := by
    have : (slot % p.SLOTS_PER_EPOCH + 1) * committees_per_slot ≤
        p.SLOTS_PER_EPOCH * committees_per_slot := Nat.mul_le_mul_right _ hmod
    rw [Nat.add_mul, Nat.one_mul] at this
    rw [Nat.mul_comm committees_per_slot]
    omega
  unfold get_beacon_committee
  have hm1 : slot % p.SLOTS_PER_EPOCH * committees_per_slot < 2 ^ 64 := by omega
  have hm2 : slot % p.SLOTS_PER_EPOCH * committees_per_slot + index < 2 ^ 64 := by omega
  have hm3 : committees_per_slot * p.SLOTS_PER_EPOCH < 2 ^ 64 := by omega
  simp only [hcps, uint64Mod, uint64Mul, CacheProofs.Spec.uint64Add,
    Nat.ne_of_gt hspe0, if_false, CacheProofs.Spec.UINT64_SIZE, except_ok_bind, pure_bind]
  simp only [hm1, if_true, pure_bind]
  simp only [hm2, hm3, if_true, pure_bind]
  exact compute_committee_slice p sha256 indices seed _ hshuf _ _ hidx
    (by unfold CacheProofs.Spec.UINT64_SIZE; omega)
    (by unfold CacheProofs.Spec.UINT64_SIZE; omega)

open CacheProofs.Spec.CommitteeCache in
theorem get_beacon_committee_equiv (p : Preset) (sha256 : List Nat → List Nat)
    (indices seed : List Nat) (shuffling : Slice Usize)
    (hshuf : IsShuffling p sha256 indices seed (shuffling.val.map (·.val)))
    (committees_per_slot slots_per_epoch slot index : Usize)
    (hcps : get_committee_count_per_slot p indices.length = .ok committees_per_slot.val)
    (hspe : slots_per_epoch.val = p.SLOTS_PER_EPOCH) (hspe0 : 0 < slots_per_epoch.val)
    (hindex : index.val < committees_per_slot.val) (hn : 0 < indices.length)
    (hfit : indices.length * (committees_per_slot.val * slots_per_epoch.val) ≤ Usize.max) :
    ∃ committee,
      lhGetBeaconCommittee (some (slot.val / slots_per_epoch.val)) shuffling committees_per_slot
        slots_per_epoch slot index = ok (some committee) ∧
      get_beacon_committee p sha256 indices seed slot.val index.val =
        .ok (committee.map (·.val)) := by
  have hlen : shuffling.val.length = indices.length := by simpa using hshuf.1
  have hcount : committees_per_slot.val * slots_per_epoch.val ≤ Usize.max := by
    calc _ ≤ indices.length * (committees_per_slot.val * slots_per_epoch.val) :=
          Nat.le_mul_of_pos_left _ hn
      _ ≤ _ := hfit
  have hmod : slot.val % slots_per_epoch.val < slots_per_epoch.val := Nat.mod_lt _ hspe0
  have hidx : slot.val % slots_per_epoch.val * committees_per_slot.val + index.val <
      committees_per_slot.val * slots_per_epoch.val := by
    have : (slot.val % slots_per_epoch.val + 1) * committees_per_slot.val ≤
        slots_per_epoch.val * committees_per_slot.val := Nat.mul_le_mul_right _ hmod
    rw [Nat.add_mul, Nat.one_mul] at this
    rw [Nat.mul_comm committees_per_slot.val]
    omega
  obtain ⟨ci, hci, hcival⟩ := committee_index_in_epoch_ok slot slots_per_epoch committees_per_slot
    index hspe0 (by omega)
  obtain ⟨c, hc, hcval⟩ := epoch_committee_count_ok committees_per_slot slots_per_epoch hcount
  have hfit' : (Slice.len shuffling).val * c.val ≤ Usize.max := by
    simp only [Slice.len_val, hlen, hcval]; exact hfit
  obtain ⟨s, e, hr, hs, he⟩ := committee_range_in_epoch_ok c ci (Slice.len shuffling)
    (by omega) hfit'
  simp only [Slice.len_val, hlen] at hs he
  have hc0 : 0 < c.val := by omega
  have hend : e.val ≤ indices.length := by
    rw [he]; apply Nat.div_le_of_le_mul; rw [Nat.mul_comm c.val]
    exact Nat.mul_le_mul_left _ (by omega)
  have hse : s.val ≤ e.val := by
    rw [hs, he]; exact Nat.div_le_div_right (Nat.mul_le_mul_left _ (Nat.le_succ _))
  refine ⟨(shuffling.val.drop s.val).take (e.val - s.val), ?_, ?_⟩
  · have hidx' : ¬ committees_per_slot.val ≤ index.val := by omega
    simp only [lhGetBeaconCommittee, hidx', hci, hc, hr, bind_tc_ok, lhSliceGet, hse, hlen, hend]
    simp
  · have h64 := usize_max_lt
    rw [get_beacon_committee_slice p sha256 indices seed _ hshuf _ _ _ hcps (hspe ▸ hspe0)
      hindex hn (by rw [← hspe]; omega)]
    simp only [← hspe]
    rw [hs, he, hcival, hcval, List.map_take, List.map_drop]


/-! ## Shuffling positions -/

/-- `i + 1` if `v` is at index `i` of `l`, else `0`. -/
def posOf (l : List Nat) (v : Nat) : Nat := if v ∈ l then l.idxOf v + 1 else 0

/-- `CommitteeCache::shuffled_position` on the encoded positions. `NonZeroUsize::new(p)` is
`None` if and only if `p = 0`, and `p.get() - 1` is `p - 1`. -/
def lhShuffledPosition (positions : List Usize) (v : Nat) : Option Nat :=
  match positions[v]? with
  | none => none
  | some p => if p.val = 0 then none else some (p.val - 1)

theorem posOf_append_single (l : List Nat) (v w : Nat) (hv : v ∉ l) :
    posOf (l ++ [v]) w = if w = v then l.length + 1 else posOf l w := by
  unfold posOf
  by_cases hw : w = v
  · subst hw
    simp [hv, List.idxOf_append_of_notMem hv]
  · by_cases hm : w ∈ l
    · simp [hw, hm, List.idxOf_append_of_mem hm]
    · simp [hw, hm]

theorem usize_saturating_add_one (i : Usize) (h : i.val < Usize.max) :
    (core.num.Usize.saturating_add i 1#usize).val = i.val + 1 := by
  have hmax : Usize.max < 2 ^ UScalarTy.Usize.numBits := by
    have := Nat.one_le_two_pow (n := UScalarTy.Usize.numBits)
    rw [Usize.max_def]; simp [Usize.numBits]
  simp only [core.num.Usize.saturating_add, UScalar.saturating_add, UScalar.val,
    BitVec.toNat_ofNat, UScalar.max_USize_eq]
  rw [Nat.mod_eq_of_lt (by omega)]
  simp at h ⊢; omega

theorem set_opt_some {α : Type} (l : List α) (i : Nat) (x : α) :
    l.set_opt i (some x) = l.set i x := by
  induction l generalizing i with
  | nil => simp [List.set_opt]
  | cons hd tl ih =>
    cases i with
    | zero => simp [List.set_opt]
    | succ i => simp [List.set_opt, ih]

theorem not_mem_take_of_nodup (l : List Nat) (hl : l.Nodup) (j : Nat) (hj : j < l.length) :
    l[j] ∉ l.take j := by
  intro hm
  obtain ⟨k, hk, hkeq⟩ := List.getElem_of_mem hm
  simp only [List.length_take] at hk
  rw [List.getElem_take] at hkeq
  have := (List.Nodup.getElem_inj_iff hl).mp hkeq
  omega

theorem shuffling_positions_loop_spec (shuffling : Slice Usize) (vc : Nat)
    (hnodup : (shuffling.val.map (·.val)).Nodup) (hrange : ∀ x ∈ shuffling.val, x.val < vc)
    (positions : alloc.vec.Vec Usize) (i : Usize) (hi : i.val ≤ shuffling.val.length)
    (hpos : positions.val.map (·.val) =
      (List.range vc).map (posOf ((shuffling.val.take i.val).map (·.val)))) :
    shuffling_positions_loop shuffling positions i ⦃ r =>
      r.2 = none ∧ r.1.val.map (·.val) =
        (List.range vc).map (posOf (shuffling.val.map (·.val))) ⦄ := by
  unfold shuffling_positions_loop
  apply loop.spec_decr_nat
    (measure := fun (x : alloc.vec.Vec Usize × Usize) => shuffling.val.length - x.2.val)
    (inv := fun (x : alloc.vec.Vec Usize × Usize) =>
      x.2.val ≤ shuffling.val.length ∧ x.1.val.map (·.val) =
        (List.range vc).map (posOf ((shuffling.val.take x.2.val).map (·.val))))
  · rintro ⟨u, j⟩ ⟨hj, hu⟩
    dsimp only at hj hu
    dsimp only
    unfold shuffling_positions_loop.body
    have hlenu : u.val.length = vc := by
      have := congrArg List.length hu; simpa using this
    by_cases hlt : j.val < shuffling.val.length
    · have hlt' : j < shuffling.len := by scalar_tac
      have hjl := hlt
      have hjmax : j.val < Usize.max := by have := shuffling.property; omega
      have hsat := usize_saturating_add_one j hjmax
      have hget : shuffling[j]? = some shuffling.val[j.val] := by
        rw [Slice.getElem?_Usize_eq, List.getElem?_eq_getElem hjl]
      have hv := hrange _ (List.getElem_mem hjl)
      have hgetu : (alloc.vec.Vec.deref_mut u).1[shuffling.val[j.val]]? =
          some u.val[shuffling.val[j.val].val] := by
        simp only [alloc.vec.Vec.deref_mut, Slice.getElem?_Usize_eq]
        rw [List.getElem?_eq_getElem (by omega)]
      simp only [hlt', if_true, core.slice.Slice.get,
        core.slice.index.Usize.get, Slice.getElem?_Usize_eq, List.getElem?_eq_getElem hjl,
        bind_tc_ok, lift, alloc.vec.Vec.deref_mut, core.slice.Slice.get_mut,
        core.slice.index.Usize.get_mut]
      simp only [Std.uncurry_apply_pair]
      rw [List.getElem?_eq_getElem (by omega)]
      apply (spec_ok _).mpr
      refine ⟨⟨by simp [hsat]; omega, ?_⟩, by simp [hsat]; omega⟩
      simp only [Slice.set_opt_val_eq, set_opt_some, List.map_set, hsat, hu]
      have hjl' : j.val < (shuffling.val.map (·.val)).length := by simpa using hjl
      have hnm := not_mem_take_of_nodup _ hnodup j.val hjl'
      rw [← List.map_take] at hnm
      simp only [List.getElem_map] at hnm
      rw [List.take_add_one, List.getElem?_eq_getElem hjl]
      simp only [Option.toList_some, List.map_append, List.map_cons, List.map_nil]
      apply List.ext_getElem
      · simp
      · intro w h1 h2
        simp only [List.getElem_set, List.getElem_map, List.getElem_range]
        rw [posOf_append_single _ _ _ hnm]
        simp only [List.length_map, List.length_take]
        by_cases hw : shuffling.val[j.val].val = w
        · simp [hw]; omega
        · simp [hw, Ne.symm hw]
    · have hge : ¬ j < shuffling.len := by scalar_tac
      have hjl : j.val = shuffling.val.length := by omega
      simp only [hge, if_false]
      apply (spec_ok _).mpr
      refine ⟨rfl, ?_⟩
      rw [hu, hjl, List.take_length]
  · exact ⟨hi, hpos⟩


theorem lhShuffledPosition_of_map (positions : List Usize) (vc : Nat) (L : List Nat)
    (hrange : ∀ x ∈ L, x < vc)
    (h : positions.map (·.val) = (List.range vc).map (posOf L)) (v : Nat) :
    lhShuffledPosition positions v = if v ∈ L then some (L.idxOf v) else none := by
  have hlen : positions.length = vc := by simpa using congrArg List.length h
  unfold lhShuffledPosition
  by_cases hv : v < vc
  · rw [List.getElem?_eq_getElem (by omega)]
    have hval : positions[v].val = posOf L v := by
      have := congrArg (fun l => l[v]?) h
      simp only [List.getElem?_map, List.getElem?_range hv] at this
      rw [List.getElem?_eq_getElem (by omega)] at this
      simpa using this
    simp only [hval, posOf]
    by_cases hm : v ∈ L <;> simp [hm]
  · rw [List.getElem?_eq_none (by omega)]
    have : v ∉ L := fun hm => hv (hrange v hm)
    simp [this]

/-- Build: the positions that `initialized_unchecked` stores invert the shuffling. -/
theorem shuffling_positions_spec (shuffling : Slice Usize) (validator_count : Usize)
    (hnodup : (shuffling.val.map (·.val)).Nodup)
    (hrange : ∀ x ∈ shuffling.val, x.val < validator_count.val) :
    shuffling_positions shuffling validator_count ⦃ r =>
      ∃ positions, r = .Ok positions ∧ positions.val.length = validator_count.val ∧
        ∀ v, lhShuffledPosition positions.val v =
          if v ∈ shuffling.val.map (·.val) then some ((shuffling.val.map (·.val)).idxOf v)
          else none ⦄ := by
  unfold shuffling_positions
  step with alloc.vec.from_elem_spec as ⟨pos0, hpos0, hlen0⟩
  step with shuffling_positions_loop_spec shuffling validator_count.val hnodup hrange
    as ⟨pos1, oor, hoor, hpos1⟩
  · simp only [hpos0, List.map_replicate]
    apply List.ext_getElem <;> simp [posOf]
  rcases oor with _ | v
  swap
  · simp at hoor
  have hmap : ∀ x ∈ shuffling.val.map (·.val), x < validator_count.val := by
    intro x hx
    simp only [List.mem_map] at hx
    obtain ⟨y, hy, rfl⟩ := hx
    exact hrange y hy
  exact (spec_ok _).mpr ⟨pos1, rfl, by simpa using congrArg List.length hpos1,
    lhShuffledPosition_of_map _ _ _ hmap hpos1⟩


/-! ## Attestation duties -/

/-- Shuffled position `pos` is in committee `k` of `c` committees over `n` validators. -/
def InCommittee (n c k pos : Nat) : Prop := n * k / c ≤ pos ∧ pos < n * (k + 1) / c

theorem inCommittee_unique (n c k1 k2 pos : Nat) (h1 : InCommittee n c k1 pos)
    (h2 : InCommittee n c k2 pos) : k1 = k2 := by
  by_contra hne
  rcases Nat.lt_or_gt_of_ne hne with h | h
  · have := Nat.div_le_div_right (c := c) (Nat.mul_le_mul_left n (show k1 + 1 ≤ k2 by omega))
    unfold InCommittee at h1 h2; omega
  · have := Nat.div_le_div_right (c := c) (Nat.mul_le_mul_left n (show k2 + 1 ≤ k1 by omega))
    unfold InCommittee at h1 h2; omega

theorem inCommittee_exists (n c pos : Nat) (hc : 0 < c) (hpos : pos < n) :
    ∃ k, k < c ∧ InCommittee n c k pos := by
  have hex : ∃ k, pos < n * (k + 1) / c := ⟨c - 1, by
    rw [show c - 1 + 1 = c by omega, Nat.mul_div_cancel _ hc]; exact hpos⟩
  classical
  refine ⟨Nat.find hex, ?_, ?_, Nat.find_spec hex⟩
  · have : Nat.find hex ≤ c - 1 := Nat.find_min' hex (by
      rw [show c - 1 + 1 = c by omega, Nat.mul_div_cancel _ hc]; exact hpos)
    omega
  · rcases Nat.eq_zero_or_pos (Nat.find hex) with h0 | h0
    · rw [h0]; simp
    · have := Nat.find_min hex (show Nat.find hex - 1 < Nat.find hex by omega)
      rw [show Nat.find hex - 1 + 1 = Nat.find hex by omega] at this
      omega

theorem attestation_duty_loop_spec (pos c n nth : Usize) (k0 : Nat) (hk0 : k0 < c.val)
    (hin : InCommittee n.val c.val k0 pos.val) (hnth : nth.val ≤ k0)
    (hfit : n.val * c.val ≤ Usize.max) :
    attestation_duty_loop pos c n nth ⦃ r =>
      ∃ k s e : Usize, r = .Ok (some (k, s, e)) ∧ k.val = k0 ∧ s.val = n.val * k0 / c.val ∧
        e.val = n.val * (k0 + 1) / c.val ⦄ := by
  unfold attestation_duty_loop
  apply loop.spec_decr_nat
    (measure := fun (x : Usize) => c.val - x.val)
    (inv := fun (x : Usize) => x.val ≤ k0)
  · intro j hj
    unfold attestation_duty_loop.body
    have hjc : j < c := by scalar_tac
    have hjmax : j.val < Usize.max := by scalar_tac
    have hsat := usize_saturating_add_one j hjmax
    obtain ⟨s, e, hr, hs, he⟩ := committee_range_in_epoch_ok c j n hjc hfit
    simp only [hjc, if_true, hr, bind_tc_ok, Std.uncurry_apply_pair]
    by_cases hin' : InCommittee n.val c.val j.val pos.val
    · have hjk := inCommittee_unique _ _ _ _ _ hin' hin
      have h1 : s ≤ pos := by unfold InCommittee at hin'; scalar_tac
      have h2 : e > pos := by unfold InCommittee at hin'; scalar_tac
      simp only [h1, h2, if_true]
      apply (spec_ok _).mpr
      exact ⟨j, s, e, rfl, hjk, by rw [hs, hjk], by rw [he, hjk]⟩
    · have hjk : j.val ≠ k0 := fun h => hin' (h ▸ hin)
      have hnext : (ok (ControlFlow.cont (core.num.Usize.saturating_add j 1#usize)) :
          Result (ControlFlow Usize (RResult (Option (Usize × Usize × Usize)) ArithError))) ⦃ r =>
          match r with
          | ControlFlow.done y => ∃ k s e : Usize, y = .Ok (some (k, s, e)) ∧ k.val = k0 ∧
              s.val = n.val * k0 / c.val ∧ e.val = n.val * (k0 + 1) / c.val
          | ControlFlow.cont x' => x'.val ≤ k0 ∧ c.val - x'.val < c.val - j.val ⦄ := by
        apply (spec_ok _).mpr
        simp only [hsat]
        omega
      by_cases h1 : s ≤ pos
      · have h2 : ¬ e > pos := by
          intro h2; apply hin'; unfold InCommittee; rw [← hs, ← he]; scalar_tac
        simp only [h1, h2, if_true, if_false, lift, bind_tc_ok]
        exact hnext
      · simp only [h1, if_false, lift, bind_tc_ok]
        exact hnext
  · exact hnth


theorem spec_exists {α : Type} {x : Result α} {p : α → Prop} (h : x ⦃ v => p v ⦄) :
    ∃ v, x = ok v ∧ p v := by
  cases x with
  | ok v => exact ⟨v, rfl, (spec_ok v).mp h⟩
  | fail e => exact ((spec_fail e).mp h).elim
  | div => exact (spec_div.mp h).elim

theorem attestation_duty_spec (pos c n : Usize) (hc : 0 < c.val) (hpos : pos.val < n.val)
    (hfit : n.val * c.val ≤ Usize.max) :
    ∃ k cp cl : Usize, attestation_duty pos c n = ok (.Ok (some (k, cp, cl))) ∧ k.val < c.val ∧
      InCommittee n.val c.val k.val pos.val ∧ cp.val = pos.val - n.val * k.val / c.val ∧
      cl.val = n.val * (k.val + 1) / c.val - n.val * k.val / c.val := by
  obtain ⟨k0, hk0, hin⟩ := inCommittee_exists n.val c.val pos.val hc hpos
  obtain ⟨r, hloop, k, s, e, hr, hk, hs, he⟩ := spec_exists
    (attestation_duty_loop_spec pos c n 0#usize k0 hk0 hin (by simp) hfit)
  subst hr
  unfold attestation_duty
  simp only [hloop, bind_tc_ok, branch_ok, Std.uncurry_apply_pair]
  obtain ⟨r1, h1, hr1⟩ := safe_sub_eq pos s
  rw [h1]
  have hsle : s.val ≤ pos.val := by rw [hs]; exact hin.1
  have hsle' : s.val ≤ e.val := by
    rw [hs, he]; exact Nat.div_le_div_right (Nat.mul_le_mul_left _ (Nat.le_succ _))
  cases r1 with
  | Err _ => omega
  | Ok cp =>
    obtain ⟨r2, h2, hr2⟩ := safe_sub_eq e s
    simp only [bind_tc_ok, branch_ok, h2]
    cases r2 with
    | Err _ => omega
    | Ok cl =>
      refine ⟨k, cp, cl, rfl, by omega, hk ▸ hin, ?_, ?_⟩
      · rw [hr1.1, hs, hk]
      · rw [hr2.1, he, hs, hk]


/-- A `for` loop that returns the first match, in the `Except` monad. -/
theorem forIn_search {ε α β : Type} (l : List α)
    (f : α → MProd (Option β) PUnit → Except ε (ForInStep (MProd (Option β) PUnit)))
    (P : α → Option β)
    (hf : ∀ x ∈ l, ∀ r, f x r = .ok (match P x with
      | some b => .done ⟨some b, PUnit.unit⟩
      | none => .yield ⟨none, PUnit.unit⟩)) :
    forIn l ⟨none, PUnit.unit⟩ f = .ok ⟨l.findSome? P, PUnit.unit⟩ := by
  induction l with
  | nil => rfl
  | cons a l ih =>
    rw [List.forIn_cons, hf a (by simp)]
    cases hP : P a with
    | some b => simp [hP]; rfl
    | none =>
      simp only [List.findSome?_cons, hP]
      exact ih (fun x hx r => hf x (by simp [hx]) r)

theorem findSome?_unique {α β : Type} (l : List α) (P : α → Option β) (a : α) (b : β)
    (ha : a ∈ l) (hPa : P a = some b) (hother : ∀ x ∈ l, x ≠ a → P x = none) :
    l.findSome? P = some b := by
  induction l with
  | nil => simp at ha
  | cons x l ih =>
    by_cases hx : x = a
    · subst hx; simp [hPa]
    · rw [List.findSome?_cons, hother x (by simp) hx]
      exact ih (by simpa [Ne.symm hx] using ha) (fun y hy => hother y (by simp [hy]))

theorem mem_slice_iff (L : List Nat) (hL : L.Nodup) (s e pos : Nat) (hpos : pos < L.length)
    (he : e ≤ L.length) : L[pos] ∈ (L.drop s).take (e - s) ↔ s ≤ pos ∧ pos < e := by
  constructor
  · intro hm
    obtain ⟨i, hi, hieq⟩ := List.getElem_of_mem hm
    simp only [List.length_take, List.length_drop] at hi
    rw [List.getElem_take, List.getElem_drop] at hieq
    have := (List.Nodup.getElem_inj_iff hL).mp hieq
    omega
  · rintro ⟨h1, h2⟩
    have : (L.drop s).take (e - s) = (L.drop s).take (e - s) := rfl
    apply List.mem_iff_getElem.mpr
    refine ⟨pos - s, by simp; omega, ?_⟩
    rw [List.getElem_take, List.getElem_drop]
    congr 1; omega


open CacheProofs.Spec.CommitteeCache in
/-- The committee that `get_beacon_committee` returns at `slot` and `index`, in closed form. -/
def committeeSlice (p : Preset) (n committees_per_slot : Nat) (L : List Nat) (slot index : Nat) :
    List Nat :=
  let k := slot % p.SLOTS_PER_EPOCH * committees_per_slot + index
  let c := committees_per_slot * p.SLOTS_PER_EPOCH
  (L.drop (n * k / c)).take (n * (k + 1) / c - n * k / c)

open CacheProofs.Spec.CommitteeCache in
theorem get_committee_assignment_search (p : Preset) (sha256 : List Nat → List Nat)
    (indices seed L : List Nat) (hshuf : IsShuffling p sha256 indices seed L)
    (committees_per_slot : Nat)
    (hcps : get_committee_count_per_slot p indices.length = .ok committees_per_slot)
    (hspe0 : 0 < p.SLOTS_PER_EPOCH) (hn : 0 < indices.length)
    (hfit : indices.length * (committees_per_slot * p.SLOTS_PER_EPOCH) < 18446744073709551616)
    (current_epoch epoch : Nat) (hcur : current_epoch + 1 < 18446744073709551616)
    (hepoch : epoch ≤ current_epoch + 1)
    (hslots : (epoch + 1) * p.SLOTS_PER_EPOCH < 18446744073709551616) (v : Nat) :
    get_committee_assignment p sha256 indices seed current_epoch epoch v =
      .ok (match (List.range' (epoch * p.SLOTS_PER_EPOCH) p.SLOTS_PER_EPOCH).findSome?
          (fun slot => (List.range committees_per_slot).findSome? (fun index =>
            if v ∈ committeeSlice p indices.length committees_per_slot L slot index then
              some (some (committeeSlice p indices.length committees_per_slot L slot index,
                index, slot))
            else none)) with
        | none => none
        | some a => a) := by
  unfold get_committee_assignment
  have h1 : epoch * p.SLOTS_PER_EPOCH < 18446744073709551616 := by
    have : epoch * p.SLOTS_PER_EPOCH ≤ (epoch + 1) * p.SLOTS_PER_EPOCH :=
      Nat.mul_le_mul_right _ (Nat.le_succ _)
    omega
  have h2 : epoch * p.SLOTS_PER_EPOCH + p.SLOTS_PER_EPOCH < 18446744073709551616 := by
    rw [Nat.add_mul, Nat.one_mul] at hslots; exact hslots
  simp only [CacheProofs.Spec.uint64Add, uint64Mul, CacheProofs.Spec.UINT64_SIZE,
    show current_epoch + 1 < 2 ^ 64 by omega, if_true, pure_bind, hepoch, not_true_eq_false,
    if_false, show epoch * p.SLOTS_PER_EPOCH < 2 ^ 64 by omega, hcps, except_ok_bind,
    show epoch * p.SLOTS_PER_EPOCH + p.SLOTS_PER_EPOCH < 2 ^ 64 by omega,
    Nat.add_sub_cancel_left]
  rw [forIn_search _ _ (fun slot => (List.range committees_per_slot).findSome? (fun index =>
            if v ∈ committeeSlice p indices.length committees_per_slot L slot index then
              some (some (committeeSlice p indices.length committees_per_slot L slot index,
                index, slot))
            else none))]
  · simp only [except_ok_bind]
    split <;> (rename_i heq; rw [heq]; rfl)
  · intro slot _ r
    rw [forIn_search (P := fun index =>
            if v ∈ committeeSlice p indices.length committees_per_slot L slot index then
              some (some (committeeSlice p indices.length committees_per_slot L slot index,
                index, slot))
            else none)]
    · simp only [except_ok_bind]
      split <;> (rename_i heq; rw [heq]; rfl)
    · intro index hindex r
      rw [List.mem_range] at hindex
      rw [get_beacon_committee_slice p sha256 indices seed L hshuf committees_per_slot slot index
        hcps hspe0 hindex hn hfit]
      simp only [except_ok_bind]
      by_cases hm : v ∈ committeeSlice p indices.length committees_per_slot L slot index
      · simp only [committeeSlice] at hm
        simp [committeeSlice, hm]; rfl
      · simp only [committeeSlice] at hm
        simp [committeeSlice, hm]; rfl


theorem mul_add_div_mod (t i c : Nat) (hi : i < c) : (t * c + i) / c = t ∧ (t * c + i) % c = i := by
  have hc : 0 < c := by omega
  constructor
  · rw [Nat.add_comm, Nat.add_mul_div_right _ _ hc, Nat.div_eq_of_lt hi, Nat.zero_add]
  · rw [Nat.add_comm, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hi]

theorem committeeSlice_mem_iff (p : Spec.Preset) (L : List Nat) (hL : L.Nodup) (cps : Nat)
    (epoch t index pos : Nat) (ht : t < p.SLOTS_PER_EPOCH)
    (hindex : index < cps) (hpos : pos < L.length) :
    L[pos] ∈ committeeSlice p L.length cps L (epoch * p.SLOTS_PER_EPOCH + t) index ↔
      InCommittee L.length (cps * p.SLOTS_PER_EPOCH) (t * cps + index) pos := by
  have hmod : (epoch * p.SLOTS_PER_EPOCH + t) % p.SLOTS_PER_EPOCH = t := by
    rw [Nat.add_comm, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt ht]
  unfold committeeSlice
  simp only [hmod]
  have hk : t * cps + index + 1 ≤ cps * p.SLOTS_PER_EPOCH := by
    have : (t + 1) * cps ≤ p.SLOTS_PER_EPOCH * cps := Nat.mul_le_mul_right _ ht
    rw [Nat.add_mul, Nat.one_mul] at this
    rw [Nat.mul_comm cps]; omega
  have hc : 0 < cps * p.SLOTS_PER_EPOCH := by omega
  have he : L.length * (t * cps + index + 1) / (cps * p.SLOTS_PER_EPOCH) ≤ L.length := by
    apply Nat.div_le_of_le_mul; rw [Nat.mul_comm (cps * _)]
    exact Nat.mul_le_mul_left _ hk
  rw [mem_slice_iff L hL _ _ pos hpos he]
  rfl


/-- The inner search of `get_committee_assignment` at one slot. -/
def assignmentAt (p : Spec.Preset) (n cps : Nat) (L : List Nat) (v slot : Nat) :
    Option (Option (List Nat × Nat × Nat)) :=
  (List.range cps).findSome? (fun index =>
    if v ∈ committeeSlice p n cps L slot index then
      some (some (committeeSlice p n cps L slot index, index, slot))
    else none)

theorem findSome_assignment (p : Spec.Preset) (L : List Nat) (hL : L.Nodup) (cps : Nat)
    (epoch pos k0 : Nat) (hpos : pos < L.length)
    (hk0 : k0 < cps * p.SLOTS_PER_EPOCH)
    (hin : InCommittee L.length (cps * p.SLOTS_PER_EPOCH) k0 pos) :
    (List.range' (epoch * p.SLOTS_PER_EPOCH) p.SLOTS_PER_EPOCH).findSome?
        (assignmentAt p L.length cps L L[pos]) =
      some (some (committeeSlice p L.length cps L (epoch * p.SLOTS_PER_EPOCH + k0 / cps)
          (k0 % cps), k0 % cps, epoch * p.SLOTS_PER_EPOCH + k0 / cps)) := by
  have hcps : 0 < cps := by
    rcases Nat.eq_zero_or_pos cps with h | h
    · simp [h] at hk0
    · exact h
  have hq : k0 / cps < p.SLOTS_PER_EPOCH := by
    rw [Nat.div_lt_iff_lt_mul hcps, Nat.mul_comm]; exact hk0
  have hr : k0 % cps < cps := Nat.mod_lt _ hcps
  have hdecomp : k0 / cps * cps + k0 % cps = k0 := by
    rw [Nat.mul_comm]; exact Nat.div_add_mod k0 cps
  -- membership at slot `epoch * SPE + t` and `index`
  have hmem : ∀ t index, t < p.SLOTS_PER_EPOCH → index < cps →
      (L[pos] ∈ committeeSlice p L.length cps L (epoch * p.SLOTS_PER_EPOCH + t) index ↔
        t = k0 / cps ∧ index = k0 % cps) := by
    intro t index ht hi
    rw [committeeSlice_mem_iff p L hL cps epoch t index pos ht hi hpos]
    constructor
    · intro h
      have := inCommittee_unique _ _ _ _ _ h hin
      have h2 := mul_add_div_mod t index cps hi
      rw [this] at h2
      exact ⟨h2.1.symm, h2.2.symm⟩
    · rintro ⟨rfl, rfl⟩; rw [hdecomp]; exact hin
  apply findSome?_unique _ _ (epoch * p.SLOTS_PER_EPOCH + k0 / cps)
  · rw [List.mem_range'_1]; exact ⟨Nat.le_add_right _ _, Nat.add_lt_add_left hq _⟩
  · unfold assignmentAt
    apply findSome?_unique _ _ (k0 % cps)
    · simp [hr]
    · simp [(hmem _ _ hq hr).mpr ⟨rfl, rfl⟩]
    · intro index hindex hne
      rw [List.mem_range] at hindex
      have : ¬ L[pos] ∈ committeeSlice p L.length cps L
          (epoch * p.SLOTS_PER_EPOCH + k0 / cps) index := fun h =>
        hne ((hmem _ _ hq hindex).mp h).2
      simp [this]
  · intro slot hslot hne
    simp only [List.mem_range'_1] at hslot
    obtain ⟨t, rfl⟩ : ∃ t, slot = epoch * p.SLOTS_PER_EPOCH + t :=
      ⟨slot - epoch * p.SLOTS_PER_EPOCH, by omega⟩
    have ht : t < p.SLOTS_PER_EPOCH := by omega
    have htne : t ≠ k0 / cps := fun h => hne (by rw [h])
    unfold assignmentAt
    rw [List.findSome?_eq_none_iff]
    intro index hindex
    rw [List.mem_range] at hindex
    have : ¬ L[pos] ∈ committeeSlice p L.length cps L (epoch * p.SLOTS_PER_EPOCH + t) index :=
      fun h => htne ((hmem _ _ ht hindex).mp h).1
    simp [this]

theorem findSome_assignment_none (p : Spec.Preset) (L : List Nat) (cps : Nat) (epoch v : Nat)
    (hv : v ∉ L) :
    (List.range' (epoch * p.SLOTS_PER_EPOCH) p.SLOTS_PER_EPOCH).findSome?
        (assignmentAt p L.length cps L v) = none := by
  rw [List.findSome?_eq_none_iff]
  intro slot _
  unfold assignmentAt
  rw [List.findSome?_eq_none_iff]
  intro index _
  have : v ∉ committeeSlice p L.length cps L slot index := fun h =>
    hv (List.mem_of_mem_drop (List.mem_of_mem_take h))
  simp [this]


theorem committeeSlice_at (p : Spec.Preset) (n cps : Nat) (L : List Nat) (epoch k : Nat)
    (hk : k < cps * p.SLOTS_PER_EPOCH) :
    committeeSlice p n cps L (epoch * p.SLOTS_PER_EPOCH + k / cps) (k % cps) =
      (L.drop (n * k / (cps * p.SLOTS_PER_EPOCH))).take
        (n * (k + 1) / (cps * p.SLOTS_PER_EPOCH) - n * k / (cps * p.SLOTS_PER_EPOCH)) := by
  have hcps : 0 < cps := by
    rcases Nat.eq_zero_or_pos cps with h | h
    · simp [h] at hk
    · exact h
  have hq : k / cps < p.SLOTS_PER_EPOCH := by
    rw [Nat.div_lt_iff_lt_mul hcps, Nat.mul_comm]; exact hk
  have hmod : (epoch * p.SLOTS_PER_EPOCH + k / cps) % p.SLOTS_PER_EPOCH = k / cps := by
    rw [Nat.add_comm, Nat.add_mul_mod_self_right, Nat.mod_eq_of_lt hq]
  have hdecomp : k / cps * cps + k % cps = k := by
    rw [Nat.mul_comm]; exact Nat.div_add_mod k cps
  unfold committeeSlice
  simp only [hmod, hdecomp]

open CacheProofs.Spec.CommitteeCache in
theorem get_committee_count_per_slot_pos (p : Preset) (n x : Nat)
    (h : get_committee_count_per_slot p n = .ok x) : 1 ≤ x := by
  unfold get_committee_count_per_slot uint64Div at h
  by_cases h1 : p.SLOTS_PER_EPOCH = 0
  · simp [h1] at h; cases h
  · by_cases h2 : p.TARGET_COMMITTEE_SIZE = 0
    · simp [h1, h2] at h; cases h
    · simp [h1, h2] at h
      cases h
      exact Nat.le_max_left _ _

/-- `CommitteeCache::get_attestation_duties`, modeled by hand. `position` is
`self.shuffled_position(validator_index)`. `convert_to_slot_and_index` gives
`(epoch * slots_per_epoch + nth / committees_per_slot, nth % committees_per_slot)`. The result is
`(slot, index, committee_position, committee_len, committees_at_slot)`. -/
def lhGetAttestationDuties (position : Option Usize) (shuffling_len committees_per_slot
    slots_per_epoch : Usize) (epoch : Nat) :
    Result (RResult (Option (Nat × Nat × Nat × Nat × Nat)) ArithError) :=
  match position with
  | none => ok (.Ok none)
  | some i => do
    let r ← epoch_committee_count committees_per_slot slots_per_epoch
    match r with
    | .Err e => ok (.Err e)
    | .Ok count =>
      let r ← attestation_duty i count shuffling_len
      match r with
      | .Err e => ok (.Err e)
      | .Ok none => ok (.Ok none)
      | .Ok (some (nth, cp, cl)) =>
        ok (.Ok (some (epoch * slots_per_epoch.val + nth.val / committees_per_slot.val,
          nth.val % committees_per_slot.val, cp.val, cl.val, committees_per_slot.val)))

open CacheProofs.Spec.CommitteeCache in
theorem get_attestation_duties_equiv (p : Preset) (sha256 : List Nat → List Nat)
    (indices seed : List Nat) (shuffling : Slice Usize)
    (hshuf : IsShuffling p sha256 indices seed (shuffling.val.map (·.val)))
    (hnodup : (shuffling.val.map (·.val)).Nodup)
    (committees_per_slot slots_per_epoch : Usize)
    (hcps : get_committee_count_per_slot p indices.length = .ok committees_per_slot.val)
    (hspe : slots_per_epoch.val = p.SLOTS_PER_EPOCH) (hspe0 : 0 < slots_per_epoch.val)
    (hn : 0 < indices.length)
    (hfit : indices.length * (committees_per_slot.val * slots_per_epoch.val) ≤ Usize.max)
    (current_epoch epoch : Nat) (hcur : current_epoch + 1 < 18446744073709551616)
    (hepoch : epoch ≤ current_epoch + 1)
    (hslots : (epoch + 1) * p.SLOTS_PER_EPOCH < 18446744073709551616)
    (v : Nat) (position : Option Usize)
    (hposition : position.map (·.val) =
      if v ∈ shuffling.val.map (·.val) then some ((shuffling.val.map (·.val)).idxOf v)
      else none) :
    ∃ duty, lhGetAttestationDuties position (Slice.len shuffling) committees_per_slot
        slots_per_epoch epoch = ok (.Ok duty) ∧
      match duty with
      | none => get_committee_assignment p sha256 indices seed current_epoch epoch v = .ok none
      | some (slot, index, committee_position, committee_len, committees_at_slot) =>
        committees_at_slot = committees_per_slot.val ∧
        ∃ committee,
          get_committee_assignment p sha256 indices seed current_epoch epoch v =
            .ok (some (committee, index, slot)) ∧
          committee.length = committee_len ∧ committee[committee_position]? = some v := by
  set L := shuffling.val.map (·.val) with hLdef
  have hlen : L.length = indices.length := hshuf.1
  have h64 := usize_max_lt
  have hsearch := get_committee_assignment_search p sha256 indices seed L hshuf
    committees_per_slot.val hcps (hspe ▸ hspe0) hn (by rw [← hspe]; omega) current_epoch epoch
    hcur hepoch hslots v
  rw [← hlen] at hsearch
  by_cases hv : v ∈ L
  · simp only [hv, if_true] at hposition
    obtain ⟨i, rfl, hi⟩ : ∃ i : Usize, position = some i ∧ i.val = L.idxOf v := by
      cases position with
      | none => simp at hposition
      | some i => exact ⟨i, rfl, by simpa using hposition⟩
    have hipos : i.val < L.length := by rw [hi]; exact List.idxOf_lt_length_iff.mpr hv
    have hLi : L[i.val] = v := by simp only [hi]; exact List.getElem_idxOf _
    have hcount : committees_per_slot.val * slots_per_epoch.val ≤ Usize.max :=
      le_trans (Nat.le_mul_of_pos_left _ hn) hfit
    obtain ⟨c, hc, hcval⟩ := epoch_committee_count_ok committees_per_slot slots_per_epoch hcount
    have hc0 : 0 < c.val := by
      rw [hcval]
      exact Nat.mul_pos (get_committee_count_per_slot_pos p _ _ hcps) hspe0
    have hlenu : (Slice.len shuffling).val = L.length := by simp [hLdef]
    obtain ⟨k, cp, cl, hduty, hkc, hin, hcp, hcl⟩ := attestation_duty_spec i c (Slice.len shuffling)
      hc0 (by rw [hlenu]; exact hipos) (by rw [hlenu, hlen, hcval]; exact hfit)
    rw [hlenu, hcval, hspe] at hin hcp hcl
    rw [hcval] at hkc
    have hfound := findSome_assignment p L hnodup committees_per_slot.val epoch
      i.val k.val hipos (by rw [← hspe]; exact hkc) hin
    rw [hLi] at hfound
    refine ⟨some (epoch * slots_per_epoch.val + k.val / committees_per_slot.val,
      k.val % committees_per_slot.val, cp.val, cl.val, committees_per_slot.val), ?_, rfl,
      committeeSlice p L.length committees_per_slot.val L
        (epoch * p.SLOTS_PER_EPOCH + k.val / committees_per_slot.val)
        (k.val % committees_per_slot.val), ?_, ?_, ?_⟩
    · simp only [lhGetAttestationDuties, hc, hduty, bind_tc_ok]
    · rw [hsearch]
      show Except.ok (match List.findSome? (assignmentAt p L.length committees_per_slot.val L v)
        _ with | none => none | some a => a) = _
      rw [hfound, hspe]
    · rw [committeeSlice_at _ _ _ _ _ _ (by rw [← hspe]; exact hkc), hcl]
      have he : L.length * (k.val + 1) / (committees_per_slot.val * p.SLOTS_PER_EPOCH) ≤
          L.length := by
        apply Nat.div_le_of_le_mul; rw [Nat.mul_comm (_ * _)]
        exact Nat.mul_le_mul_left _ (by rw [← hspe]; omega)
      simp only [List.length_take, List.length_drop]
      omega
    · rw [committeeSlice_at _ _ _ _ _ _ (by rw [← hspe]; exact hkc), hcp]
      have hsl := hin.1
      have hpl := hin.2
      rw [List.getElem?_take, if_pos (by omega), List.getElem?_drop,
        show L.length * k.val / (committees_per_slot.val * p.SLOTS_PER_EPOCH) +
          (i.val - L.length * k.val / (committees_per_slot.val * p.SLOTS_PER_EPOCH)) = i.val by
          omega, List.getElem?_eq_getElem hipos, hLi]
  · simp only [hv, if_false, Option.map_eq_none_iff] at hposition
    subst hposition
    refine ⟨none, rfl, ?_⟩
    rw [hsearch]
    show Except.ok (match List.findSome? (assignmentAt p L.length committees_per_slot.val L v)
      _ with | none => none | some a => a) = _
    rw [findSome_assignment_none p L _ epoch v hv]


/-- `shuffled_position` inverts the shuffling, and is `None` for other validators. -/
theorem shuffled_position_inverse (shuffling : Slice Usize) (validator_count : Usize)
    (hnodup : (shuffling.val.map (·.val)).Nodup)
    (hrange : ∀ x ∈ shuffling.val, x.val < validator_count.val) :
    ∃ positions, shuffling_positions shuffling validator_count = ok (.Ok positions) ∧
      positions.val.length = validator_count.val ∧
      (∀ i (h : i < shuffling.val.length),
        lhShuffledPosition positions.val shuffling.val[i].val = some i) ∧
      (∀ v, v ∉ shuffling.val.map (·.val) → lhShuffledPosition positions.val v = none) := by
  obtain ⟨r, hr, positions, rfl, hlen, hpos⟩ :=
    spec_exists (shuffling_positions_spec shuffling validator_count hnodup hrange)
  refine ⟨positions, hr, hlen, ?_, ?_⟩
  · intro i h
    have hm : shuffling.val[i].val ∈ shuffling.val.map (·.val) :=
      List.mem_map.mpr ⟨_, List.getElem_mem h, rfl⟩
    rw [hpos, if_pos hm]
    have : (shuffling.val.map (·.val))[i]'(by simpa using h) = shuffling.val[i].val := by simp
    rw [← this, List.Nodup.idxOf_getElem hnodup]
  · intro v hv
    rw [hpos, if_neg hv]

end CacheProofs.CommitteeCache
