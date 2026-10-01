import SlotScheduleProofs.Model
import SlotScheduleProofs.SaturatingMul

/-!
# Specifications of the primitive operations

Step specifications for the scalar operations and small helpers that the schedule functions
call: `usize` saturating arithmetic, the `safe_arith` operations, `Epoch::start_slot` and
`time_after_slots_ms`.
-/

namespace SlotScheduleProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result types

theorem usize_saturating_sub_val (x y : Usize) :
    (core.num.Usize.saturating_sub x y).val = x.val - y.val := by
  simp only [core.num.Usize.saturating_sub, UScalar.saturating_sub, UScalar.val, BitVec.toNat_ofNat]
  have := x.hBounds
  apply Nat.mod_eq_of_lt
  simp only [UScalar.val] at this
  omega

theorem usize_saturating_add_val (x y : Usize) :
    (core.num.Usize.saturating_add x y).val = min Usize.max (x.val + y.val) := by
  simp only [core.num.Usize.saturating_add, UScalar.saturating_add]
  show (BitVec.ofNat _ _).toNat = _
  rw [BitVec.toNat_ofNat, UScalar.max_USize_eq]
  apply Nat.mod_eq_of_lt
  have : Usize.max < 2 ^ UScalarTy.Usize.numBits := by
    rw [← UScalar.max_USize_eq, UScalar.max]
    have := Nat.one_le_two_pow (n := UScalarTy.Usize.numBits); omega
  omega

theorem index_succ_val {schedule : Slice Entry} (index : Usize) (h : index.val < schedule.length) :
    (core.num.Usize.saturating_add index 1#usize).val = index.val + 1 := by
  rw [usize_saturating_add_val]
  have := Slice.length_ineq schedule
  have h1 : (1#usize).val = 1 := by simp
  simp only [Slice.length] at h this
  rw [h1]
  omega

@[step]
theorem start_slot_spec (ep : core.slot_epoch.Epoch) (spe : U64)
    (h : ep.val * spe.val ≤ U64.max) :
    core.slot_epoch.Epoch.start_slot ep spe ⦃ s => s.val = ep.val * spe.val ⦄ := by
  unfold core.slot_epoch.Epoch.start_slot
  step as ⟨r, hr⟩
  simp only [core.slot_epoch.Slot.Insts.CoreConvertFromU64.from, spec_ok]
  omega

theorem time_after_slots_ms_spec (t : U64) (s e : core.slot_epoch.Slot) (d : U64)
    (h : s.val ≤ e.val) :
    core.slot_duration_schedule.time_after_slots_ms t s e d ⦃ r => match r with
      | .Ok v => v.val = t.val + (e.val - s.val) * d.val
      | .Err _ => U64.max < t.val + (e.val - s.val) * d.val ⦄ := by
  unfold core.slot_duration_schedule.time_after_slots_ms
  have hsub := U64.checked_sub_bv_spec e s
  have hmul := U64.checked_mul_bv_spec (U64.checked_sub e s).get! d
  simp only [core.slot_epoch.Slot.Insts.Safe_arithSafeArithSlot.safe_sub,
    U64.Insts.CoreConvertFromSlot.from, lift, core.option.Option.map,
    P.Insts.CoreOpsFunctionFnOnceTupleU64Slot.call_once,
    core.slot_epoch.Slot.new, core.convert.IntoFrom.into, bind_tc_ok]
  rcases hs : U64.checked_sub e s with _ | z
  · simp [hs] at hsub; omega
  simp only [hs] at hsub hmul
  obtain ⟨-, hz, -⟩ := hsub
  simp only [Option.get!_some] at hmul
  simp only [bind_tc_ok, core.option.Option.ok_or, core.result.Result.Insts.CoreOpsTry.branch,
    core.slot_epoch.Slot.as_u64, U64.Insts.Safe_arithSafeArithU64.safe_mul,
    U64.Insts.Safe_arithSafeArithU64.safe_add, lift]
  rcases hm : U64.checked_mul z d with _ | w
  · simp only [hm] at hmul
    simp [hm, core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual]
    rw [← hz]; omega
  simp only [hm] at hmul
  obtain ⟨-, hw, -⟩ := hmul
  have hadd := U64.checked_add_bv_spec t w
  rcases ha : U64.checked_add t w with _ | v
  · simp only [ha] at hadd
    simp [hm, ha]
    rw [← hz, ← hw]; exact hadd
  simp only [ha] at hadd
  simp [hm, ha]
  rw [← hz, ← hw]; exact hadd.2.1

theorem safe_sub_ok (x y : U64) (h : y.val ≤ x.val) :
    U64.Insts.Safe_arithSafeArithU64.safe_sub x y ⦃ r => match r with
      | .Ok z => z.val = x.val - y.val
      | .Err _ => False ⦄ := by
  have hs := U64.checked_sub_bv_spec x y
  unfold U64.Insts.Safe_arithSafeArithU64.safe_sub
  rcases hc : U64.checked_sub x y with _ | z
  · simp only [hc] at hs; omega
  · simp only [hc] at hs
    simp [lift, core.option.Option.ok_or, hs.2.1]

theorem safe_sub_err (x y : U64) (h : x.val < y.val) :
    U64.Insts.Safe_arithSafeArithU64.safe_sub x y ⦃ r => match r with
      | .Ok _ => False
      | .Err _ => True ⦄ := by
  have hs := U64.checked_sub_bv_spec x y
  unfold U64.Insts.Safe_arithSafeArithU64.safe_sub
  rcases hc : U64.checked_sub x y with _ | z
  · simp [lift, core.option.Option.ok_or]
  · simp only [hc] at hs; omega

theorem safe_div_ok (x y : U64) (h : 0 < y.val) :
    U64.Insts.Safe_arithSafeArithU64.safe_div x y ⦃ r => match r with
      | .Ok z => z.val = x.val / y.val
      | .Err _ => False ⦄ := by
  have hs := U64.checked_div_bv_spec x y
  unfold U64.Insts.Safe_arithSafeArithU64.safe_div
  rcases hc : U64.checked_div x y with _ | z
  · simp only [hc] at hs; omega
  · simp only [hc] at hs
    simp [lift, core.option.Option.ok_or, hs.2.1]

theorem slot_safe_add_ok (x : core.slot_epoch.Slot) (y : U64) (h : x.val + y.val ≤ U64.max) :
    core.slot_epoch.Slot.Insts.Safe_arithSafeArithU64.safe_add x y ⦃ r => match r with
      | .Ok z => z.val = x.val + y.val
      | .Err _ => False ⦄ := by
  have hs := U64.checked_add_bv_spec x y
  unfold core.slot_epoch.Slot.Insts.Safe_arithSafeArithU64.safe_add
  rcases hc : U64.checked_add x y with _ | z
  · simp only [hc] at hs; omega
  · simp only [hc] at hs
    simp [lift, core.option.Option.ok_or, core.option.Option.map, core.convert.IntoFrom.into,
      P.Insts.CoreOpsFunctionFnOnceTupleU64Slot.call_once, core.slot_epoch.Slot.new, hc, hs.2.1]

theorem bind_ne_ok {α : Type} (m : Result α) (k : α → Result (core.result.Result Unit String))
    (hk : ∀ a, k a ≠ ok (.Ok ())) : (m >>= k) ≠ ok (.Ok ()) := by
  cases m with
  | ok a => simpa using hk a
  | fail e => simp
  | div => simp

end SlotScheduleProofs
