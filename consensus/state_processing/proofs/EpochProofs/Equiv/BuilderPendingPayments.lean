import EpochProofs.Generated
import EpochProofs.Sanity.BuilderPendingPayments

/-!
# Lighthouse `process_builder_pending_payments` equals the reference

The main theorem is `builder_pending_payments_equiv`. For every well-formed input, the
Lighthouse code returns `ok`, and its result maps to the reference result.

`lighthouseBuilderPendingPayments` models the glue in `single_pass.rs`. It is written by hand.
-/

namespace EpochProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result state_processing

abbrev LhWithdrawal := types.builder.builder_pending_withdrawal.BuilderPendingWithdrawal
abbrev LhPayment := types.builder.builder_pending_payment.BuilderPendingPayment

theorem usize_saturating_add_one (i : Usize) (h : i.val < Usize.max) :
    (core.num.Usize.saturating_add i 1#usize).val = i.val + 1 := by
  have hmax : Usize.max < 2 ^ UScalarTy.Usize.numBits := by
    have := Nat.one_le_two_pow (n := UScalarTy.Usize.numBits)
    rw [Usize.max_def]; simp [Usize.numBits]
  simp only [core.num.Usize.saturating_add, UScalar.saturating_add, UScalar.val,
    BitVec.toNat_ofNat, UScalar.max_USize_eq]
  rw [Nat.mod_eq_of_lt (by omega)]
  simp at h ⊢; omega

theorem slice_get (s : Slice LhPayment) (i : Usize) :
    core.slice.Slice.get (core.slice.index.SliceIndexUsizeSlice LhPayment) s i = ok s.val[i.val]? :=
  rfl

theorem withdrawal_clone (w : LhWithdrawal) :
    types.builder.builder_pending_withdrawal.BuilderPendingWithdrawal.Insts.CoreCloneClone.clone w
      = ok w := by
  rfl

theorem payment_clone (p : LhPayment) :
    types.builder.builder_pending_payment.BuilderPendingPayment.Insts.CoreCloneClone.clone p
      = ok p := by
  rfl

/-- `BuilderPendingPayment::default()`. -/
def lhDefaultPayment : LhPayment :=
  { weight := 0#u64
    withdrawal :=
      { fee_recipient := alloy_primitives.bits.fixed.FixedBytes.ZERO 20#usize
        amount := 0#u64
        builder_index := 0#u64 }
    proposer_index := 0#u64 }

theorem payment_default :
    types.builder.builder_pending_payment.BuilderPendingPayment.Insts.CoreDefaultDefault.default
      = ok lhDefaultPayment := by
  rfl

/-- The withdrawals that loop 0 collects from the first `n` payments. -/
def newWithdrawals (payments : List LhPayment) (n : Nat) (quorum : U64) : List LhWithdrawal :=
  ((payments.take n).filter (fun p => quorum ≤ p.weight)).map (·.withdrawal)

theorem newWithdrawals_length_le (payments : List LhPayment) (n : Nat) (quorum : U64) :
    (newWithdrawals payments n quorum).length ≤ n := by
  unfold newWithdrawals
  have := List.length_filter_le (fun p : LhPayment => decide (quorum ≤ p.weight)) (payments.take n)
  simp at this ⊢; omega

theorem newWithdrawals_succ (payments : List LhPayment) (n : Nat) (quorum : U64) :
    newWithdrawals payments (n + 1) quorum =
      match payments[n]? with
      | none => newWithdrawals payments n quorum
      | some p =>
        if quorum ≤ p.weight then newWithdrawals payments n quorum ++ [p.withdrawal]
        else newWithdrawals payments n quorum := by
  unfold newWithdrawals
  rw [List.take_add_one]
  cases payments[n]? with
  | none => simp
  | some p =>
    by_cases h : quorum ≤ p.weight
    · have h' : quorum.val ≤ p.weight.val := by scalar_tac
      simp [h, h']
    · have h' : p.weight.val < quorum.val := by scalar_tac
      simp [h, h']

@[step]
theorem loop0_spec (payments : Slice LhPayment) (spe : Usize) (quorum : U64)
    (acc : alloc.vec.Vec LhWithdrawal) (i : Usize) (hi : i.val ≤ spe.val)
    (hacc : acc.val = newWithdrawals payments.val i.val quorum) :
    per_epoch_processing.builder_pending_payments.process_builder_pending_payments_loop0
      payments spe quorum acc i ⦃ res => res.val = newWithdrawals payments.val spe.val quorum ⦄ := by
  unfold per_epoch_processing.builder_pending_payments.process_builder_pending_payments_loop0
  apply loop.spec_decr_nat
    (measure := fun (x : alloc.vec.Vec LhWithdrawal × Usize) => spe.val - x.2.val)
    (inv := fun (x : alloc.vec.Vec LhWithdrawal × Usize) =>
      x.2.val ≤ spe.val ∧ x.1.val = newWithdrawals payments.val x.2.val quorum)
  · rintro ⟨w, j⟩ ⟨hj, hw⟩
    dsimp only at hj hw
    unfold per_epoch_processing.builder_pending_payments.process_builder_pending_payments_loop0.body
    simp only [slice_get, withdrawal_clone, lift, bind_tc_ok]
    split
    · rename_i hlt
      have hjmax : j.val < Usize.max := by scalar_tac
      have hsat := usize_saturating_add_one j hjmax
      have hsucc := newWithdrawals_succ payments.val j.val quorum
      have hlen := newWithdrawals_length_le payments.val j.val quorum
      have hjs : j.val < spe.val := by scalar_tac
      have hwlen : w.val.length < Usize.max := by rw [hw]; omega
      cases hget : payments.val[j.val]? with
      | none =>
        simp only [hget] at hsucc ⊢
        simp only [bind_tc_ok, spec_ok, hsat]
        refine ⟨⟨by omega, ?_⟩, by omega⟩
        rw [hsucc, hw]
      | some p =>
        simp only [hget] at hsucc ⊢
        split
        · rename_i hq
          have hq' : quorum ≤ p.weight := hq
          simp only [hq', if_true] at hsucc
          step as ⟨w1, hw1⟩
          simp only [hsat]
          refine ⟨by omega, ?_, by omega⟩
          rw [hsucc, hw1, hw]
        · rename_i hq
          have hq' : ¬ quorum ≤ p.weight := hq
          simp only [hq', if_false] at hsucc
          simp only [bind_tc_ok, spec_ok, hsat]
          refine ⟨⟨by omega, ?_⟩, by omega⟩
          rw [hsucc, hw]
    · rename_i hge
      have hjeq : j.val = spe.val := by scalar_tac
      simp only [spec_ok]
      rw [hw, hjeq]
  · exact ⟨hi, hacc⟩

@[step]
theorem loop1_spec (payments : Slice LhPayment) (spe : Usize)
    (acc : alloc.vec.Vec LhPayment) (i : Usize)
    (hi : spe.val ≤ i.val) (hi' : i.val ≤ payments.val.length ∨ i.val = spe.val)
    (hacc : acc.val = (payments.val.drop spe.val).take (i.val - spe.val)) :
    per_epoch_processing.builder_pending_payments.process_builder_pending_payments_loop1
      payments acc i ⦃ res => res.val = payments.val.drop spe.val ⦄ := by
  unfold per_epoch_processing.builder_pending_payments.process_builder_pending_payments_loop1
  apply loop.spec_decr_nat
    (measure := fun (x : alloc.vec.Vec LhPayment × Usize) => payments.val.length - x.2.val)
    (inv := fun (x : alloc.vec.Vec LhPayment × Usize) =>
      spe.val ≤ x.2.val ∧ (x.2.val ≤ payments.val.length ∨ x.2.val = spe.val)
      ∧ x.1.val = (payments.val.drop spe.val).take (x.2.val - spe.val))
  · rintro ⟨u, j⟩ ⟨hj, hj', hu⟩
    dsimp only at hj hj' hu
    unfold per_epoch_processing.builder_pending_payments.process_builder_pending_payments_loop1.body
    simp only [slice_get, payment_clone, lift, bind_tc_ok]
    split
    · rename_i hlt
      have hjl : j.val < payments.val.length := by scalar_tac
      have hjmax : j.val < Usize.max := by scalar_tac
      have hsat := usize_saturating_add_one j hjmax
      have hulen : u.val.length < Usize.max := by
        rw [hu]; simp only [List.length_take, List.length_drop]; omega
      simp only [List.getElem?_eq_getElem hjl]
      step as ⟨u1, hu1⟩
      simp only [hsat]
      refine ⟨by omega, by omega, ?_, by omega⟩
      rw [hu1, hu, show j.val + 1 - spe.val = (j.val - spe.val) + 1 by omega, List.take_add_one,
        List.getElem?_drop, show spe.val + (j.val - spe.val) = j.val by omega,
        List.getElem?_eq_getElem hjl]
      simp
    · rename_i hge
      have hjl : payments.val.length ≤ j.val := by scalar_tac
      simp only [spec_ok]
      rw [hu, List.take_of_length_le]
      simp only [List.length_drop]; omega
  · exact ⟨hi, hi', hacc⟩

@[step]
theorem loop2_spec (spe : Usize) (base : List LhPayment)
    (acc : alloc.vec.Vec LhPayment) (i : Usize)
    (hbase : base.length + spe.val ≤ Usize.max) (hi : i.val ≤ spe.val)
    (hacc : acc.val = base ++ List.replicate i.val lhDefaultPayment) :
    per_epoch_processing.builder_pending_payments.process_builder_pending_payments_loop2
      spe acc i ⦃ res => res.val = base ++ List.replicate spe.val lhDefaultPayment ⦄ := by
  unfold per_epoch_processing.builder_pending_payments.process_builder_pending_payments_loop2
  apply loop.spec_decr_nat
    (measure := fun (x : alloc.vec.Vec LhPayment × Usize) => spe.val - x.2.val)
    (inv := fun (x : alloc.vec.Vec LhPayment × Usize) =>
      x.2.val ≤ spe.val ∧ x.1.val = base ++ List.replicate x.2.val lhDefaultPayment)
  · rintro ⟨u, j⟩ ⟨hj, hu⟩
    dsimp only at hj hu
    unfold per_epoch_processing.builder_pending_payments.process_builder_pending_payments_loop2.body
    simp only [payment_default, lift, bind_tc_ok]
    split
    · rename_i hlt
      have hjs : j.val < spe.val := by scalar_tac
      have hjmax : j.val < Usize.max := by omega
      have hsat := usize_saturating_add_one j hjmax
      have hulen : u.val.length < Usize.max := by
        rw [hu]; simp only [List.length_append, List.length_replicate]; omega
      step as ⟨u1, hu1⟩
      simp only [hsat]
      refine ⟨by omega, ?_, by omega⟩
      rw [hu1, hu, List.replicate_succ', List.append_assoc]
    · rename_i hge
      have hjeq : j.val = spe.val := by scalar_tac
      simp only [spec_ok]
      rw [hu, hjeq]
  · exact ⟨hi, hacc⟩

theorem process_builder_pending_payments_spec (payments : Slice LhPayment) (spe : Usize)
    (quorum : U64) :
    per_epoch_processing.builder_pending_payments.process_builder_pending_payments
      payments spe quorum ⦃ res =>
        res.1.val = newWithdrawals payments.val spe.val quorum
        ∧ res.2.val = payments.val.drop spe.val ++ List.replicate spe.val lhDefaultPayment ⦄ := by
  unfold per_epoch_processing.builder_pending_payments.process_builder_pending_payments
  have hlen : payments.val.length ≤ Usize.max := by scalar_tac
  have hspe : spe.val ≤ Usize.max := by scalar_tac
  step as ⟨w, hw⟩
  case hacc => simp [newWithdrawals]
  step with loop1_spec payments spe as ⟨u, hu⟩
  case hacc => simp
  step with loop2_spec spe (payments.val.drop spe.val) as ⟨u1, hu1⟩
  case hbase => simp only [List.length_drop]; omega
  case hacc => simp [hu]
  exact ⟨hw, hu1⟩

def absArithError : safe_arith.ArithError → Spec.SpecError
  | .Overflow => .overflow
  | .DivisionByZero => .divisionByZero

def absResult {α β : Type} (f : α → β) : core.result.Result α safe_arith.ArithError → Spec.SpecM β
  | .Ok x => .ok (f x)
  | .Err e => .error (absArithError e)

theorem quorum_equiv (total_active_balance slots_per_epoch : U64) :
    per_epoch_processing.builder_pending_payments.get_builder_payment_quorum_threshold
      total_active_balance slots_per_epoch 6#u64 10#u64 ⦃ r =>
        absResult (·.val) r =
          Spec.get_builder_payment_quorum_threshold ⟨slots_per_epoch.val⟩
            total_active_balance.val ⦄ := by
  unfold per_epoch_processing.builder_pending_payments.get_builder_payment_quorum_threshold
    U64.Insts.Safe_arithSafeArithU64.safe_div U64.Insts.Safe_arithSafeArithU64.safe_mul
  simp only [Spec.get_builder_payment_quorum_threshold, Spec.uint64Div, Spec.uint64Mul,
    Spec.BUILDER_PAYMENT_THRESHOLD_NUMERATOR, Spec.BUILDER_PAYMENT_THRESHOLD_DENOMINATOR,
    Spec.UINT64_SIZE, lift, bind_tc_ok]
  have h1 := U64.checked_div_bv_spec total_active_balance slots_per_epoch
  cases hc1 : U64.checked_div total_active_balance slots_per_epoch with
  | none =>
    simp only [hc1] at h1
    simp [h1, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      absResult, absArithError]
    rfl
  | some q1 =>
    simp only [hc1] at h1
    have h2 := U64.checked_mul_bv_spec q1 6#u64
    cases hc2 : U64.checked_mul q1 6#u64 with
    | none =>
      simp only [hc2] at h2
      have hq1 : total_active_balance.val / slots_per_epoch.val = q1.val := h1.2.1.symm
      have hov : ¬ q1.val * 6 < 18446744073709551616 := by
        simp only [U64.max_eq] at h2; simp at h2; omega
      simp [h1.1, hq1, hov, hc2, core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        absResult, absArithError]
      rfl
    | some q2 =>
      simp only [hc2] at h2
      have h3 := U64.checked_div_bv_spec q2 10#u64
      cases hc3 : U64.checked_div q2 10#u64 with
      | none => simp only [hc3] at h3; simp at h3
      | some q3 =>
        simp only [hc3] at h3
        have hq1 : total_active_balance.val / slots_per_epoch.val = q1.val := h1.2.1.symm
        have hq2 : q1.val * 6 = q2.val := by have := h2.2.1; simp at this; omega
        have hfit : q1.val * 6 < 18446744073709551616 := by
          have := h2.1; simp only [U64.max_eq] at this; simp at this; omega
        have hq3 : q3.val = q2.val / 10 := by have := h3.2.1; simp at this; omega
        have hfit2 : q2.val < 18446744073709551616 := by omega
        simp [h1.1, hq1, hc2, hc3, hq2, hq3, hfit2, core.result.Result.Insts.CoreOpsTry.branch,
          absResult]
        rfl

def absAddress (a : alloy_primitives.bits.address.Address) : Spec.ExecutionAddress :=
  ⟨(a.val.map fun b => UInt8.ofNat b.val).toArray, by simp⟩

def absWithdrawal (w : LhWithdrawal) : Spec.BuilderPendingWithdrawal :=
  { fee_recipient := absAddress w.fee_recipient
    amount := w.amount.val
    builder_index := w.builder_index.val }

def absPayment (p : LhPayment) : Spec.BuilderPendingPayment :=
  { weight := p.weight.val
    withdrawal := absWithdrawal p.withdrawal
    proposer_index := p.proposer_index.val }

theorem absPayment_default : absPayment lhDefaultPayment = Spec.BuilderPendingPayment.empty := by
  simp only [absPayment, absWithdrawal, absAddress, lhDefaultPayment,
    Spec.BuilderPendingPayment.empty, Spec.BuilderPendingWithdrawal.empty,
    alloy_primitives.bits.fixed.FixedBytes.ZERO]
  congr 2

theorem map_newWithdrawals (payments : List LhPayment) (n : Nat) (quorum : U64) :
    (newWithdrawals payments n quorum).map absWithdrawal =
      (((payments.map absPayment).take n).filter (·.weight ≥ quorum.val)).map (·.withdrawal) := by
  simp [newWithdrawals, ← List.map_take, List.filter_map, Function.comp_def, absPayment]
  rfl

/-- Models the glue in `single_pass.rs`. Not generated.

Lighthouse reads the threshold from `ChainSpec`. Both presets set it to 6/10. -/
def lighthouseBuilderPendingPayments (total_active_balance slots_per_epoch : U64) (spe : Usize)
    (payments : Slice LhPayment) (withdrawals : List LhWithdrawal) :
    Result (core.result.Result (List LhWithdrawal × List LhPayment) safe_arith.ArithError) := do
  let r ← per_epoch_processing.builder_pending_payments.get_builder_payment_quorum_threshold
    total_active_balance slots_per_epoch 6#u64 10#u64
  match r with
  | .Err e => ok (.Err e)
  | .Ok quorum =>
    let res ← per_epoch_processing.builder_pending_payments.process_builder_pending_payments
      payments spe quorum
    ok (.Ok (withdrawals ++ res.1.val, res.2.val))

def absState (x : List LhWithdrawal × List LhPayment) : Spec.BeaconState :=
  { builder_pending_payments := x.2.map absPayment
    builder_pending_withdrawals := x.1.map absWithdrawal }

/-- Lighthouse equals the reference. An error maps to the same spec error.

`hspe`: `E::slots_per_epoch()` and `E::SlotsPerEpoch::to_usize()` are the same number. -/
theorem builder_pending_payments_equiv
    (total_active_balance slots_per_epoch : U64) (spe : Usize)
    (hspe : slots_per_epoch.val = spe.val)
    (payments : Slice LhPayment) (withdrawals : List LhWithdrawal)
    (hlen : payments.val.length = 2 * spe.val) :
    lighthouseBuilderPendingPayments total_active_balance slots_per_epoch spe payments withdrawals
      ⦃ r => absResult absState r =
        Spec.process_builder_pending_payments ⟨spe.val⟩ total_active_balance.val
          (absState (withdrawals, payments.val)) ⦄ := by
  unfold lighthouseBuilderPendingPayments
  rw [Spec.process_builder_pending_payments_eq _ _ _
    (by simp [absState, Spec.BeaconState.WellFormed, hlen])]
  step with quorum_equiv as ⟨r, hr⟩
  rw [hspe] at hr
  cases r with
  | Err e =>
    simp only [spec_ok]
    rw [← hr]
    rfl
  | Ok quorum =>
    step with process_builder_pending_payments_spec as ⟨res, h1, h2⟩
    rw [← hr]
    simp only [absResult, Except.map, absState, Spec.builderPendingPaymentsResult, List.map_append,
      h1, h2, map_newWithdrawals, List.map_drop, List.map_replicate, absPayment_default]

end EpochProofs
