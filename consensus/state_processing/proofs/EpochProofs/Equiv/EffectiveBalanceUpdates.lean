import EpochProofs.Equiv.BuilderPendingPayments
import EpochProofs.Sanity.EffectiveBalanceUpdates

/-!
# Lighthouse effective balance kernel equals the reference

`hysteresis_thresholds_equiv` and `new_effective_balance_equiv` relate the two Rust functions in
`effective_balance.rs` to `hysteresisThresholds` and `newEffectiveBalance`.

Lighthouse computes `effective_balance_limit` with `Validator::get_max_effective_balance`. The
theorem takes its value as a hypothesis.
-/

namespace EpochProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result state_processing

theorem hysteresis_thresholds_equiv (p : Spec.Preset)
    (increment quotient downward_multiplier upward_multiplier : U64)
    (hincrement : increment.val = p.EFFECTIVE_BALANCE_INCREMENT)
    (hquotient : quotient.val = p.HYSTERESIS_QUOTIENT)
    (hdownward : downward_multiplier.val = p.HYSTERESIS_DOWNWARD_MULTIPLIER)
    (hupward : upward_multiplier.val = p.HYSTERESIS_UPWARD_MULTIPLIER) :
    per_epoch_processing.effective_balance.hysteresis_thresholds
      increment quotient downward_multiplier upward_multiplier ⦃ r =>
        absResult (fun x => (x.1.val, x.2.val)) r = Spec.hysteresisThresholds p ⦄ := by
  unfold per_epoch_processing.effective_balance.hysteresis_thresholds
    U64.Insts.Safe_arithSafeArithU64.safe_div U64.Insts.Safe_arithSafeArithU64.safe_mul
  simp only [Spec.hysteresisThresholds, Spec.uint64Div, Spec.uint64Mul, Spec.UINT64_SIZE,
    lift, bind_tc_ok, ← hincrement, ← hquotient, ← hdownward, ← hupward]
  have h1 := U64.checked_div_bv_spec increment quotient
  cases hc1 : U64.checked_div increment quotient with
  | none =>
    simp only [hc1] at h1
    simp [h1, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      absResult, absArithError]
    rfl
  | some q =>
    simp only [hc1] at h1
    have hq : increment.val / quotient.val = q.val := h1.2.1.symm
    have h2 := U64.checked_mul_bv_spec q downward_multiplier
    cases hc2 : U64.checked_mul q downward_multiplier with
    | none =>
      simp only [hc2] at h2
      have hov : ¬ q.val * downward_multiplier.val < 18446744073709551616 := by
        simp only [U64.max_eq] at h2; omega
      simp [h1.1, hq, hov, hc2, core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        absResult, absArithError]
      rfl
    | some d =>
      simp only [hc2] at h2
      have hfit : q.val * downward_multiplier.val < 18446744073709551616 := by
        have := h2.1; simp only [U64.max_eq] at this; omega
      have h3 := U64.checked_mul_bv_spec q upward_multiplier
      cases hc3 : U64.checked_mul q upward_multiplier with
      | none =>
        simp only [hc3] at h3
        have hov : ¬ q.val * upward_multiplier.val < 18446744073709551616 := by
          simp only [U64.max_eq] at h3; omega
        simp [h1.1, hq, hfit, hov, hc2, hc3, core.result.Result.Insts.CoreOpsTry.branch,
          core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
          absResult, absArithError]
        rfl
      | some u =>
        simp only [hc3] at h3
        have hfit' : q.val * upward_multiplier.val < 18446744073709551616 := by
          have := h3.1; simp only [U64.max_eq] at this; omega
        simp [h1.1, hq, hfit, hfit', hc2, hc3, h2.2.1, h3.2.1,
          core.result.Result.Insts.CoreOpsTry.branch, absResult]
        rfl

theorem branch_ok {T E : Type} (v : T) :
    core.result.Result.Insts.CoreOpsTry.branch (E := E) (.Ok v) =
      ok (core.ops.control_flow.ControlFlow.Continue v) :=
  rfl

theorem new_effective_balance_equiv (p : Spec.Preset) (validator : Spec.Validator)
    (balance effective_balance limit downward upward increment : U64)
    (heffective : effective_balance.val = validator.effective_balance)
    (hlimit : limit.val = Spec.get_max_effective_balance p validator)
    (hincrement : increment.val = p.EFFECTIVE_BALANCE_INCREMENT) :
    per_epoch_processing.effective_balance.new_effective_balance
      balance effective_balance limit downward upward increment ⦃ r =>
        absResult (·.val) r =
          Spec.newEffectiveBalance p downward.val upward.val validator balance.val ⦄ := by
  unfold per_epoch_processing.effective_balance.new_effective_balance
    U64.Insts.Safe_arithSafeArithU64.safe_add U64.Insts.Safe_arithSafeArithU64.safe_sub
    U64.Insts.Safe_arithSafeArithU64.safe_rem
  simp only [Spec.newEffectiveBalance, Spec.uint64Add, Spec.uint64Sub, Spec.uint64Mod,
    Spec.UINT64_SIZE, lift, bind_tc_ok, ← heffective, ← hlimit, ← hincrement]
  have h1 := U64.checked_add_bv_spec balance downward
  have hr := U64.checked_rem_bv_spec balance increment
  have h4 := U64.checked_add_bv_spec effective_balance upward
  simp only [U64.max_eq] at h1 h4
  cases hc1 : balance.checked_add downward <;> simp only [hc1] at h1
  · have hov : ¬ balance.val + downward.val < 18446744073709551616 := by omega
    simp [hov, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      absResult, absArithError]
    rfl
  rename_i below
  have hfit1 : balance.val + downward.val < 18446744073709551616 := by omega
  have hbelow : below.val = balance.val + downward.val := h1.2.1
  have hfit1' : balance.val + downward.val < 2 ^ 64 := hfit1
  have hround :
      (do
        let r1 ← core.option.Option.ok_or (balance.checked_rem increment)
          safe_arith.ArithError.DivisionByZero
        let cf1 ← core.result.Result.Insts.CoreOpsTry.branch r1
        match cf1 with
        | core.ops.control_flow.ControlFlow.Continue val1 => do
          let r2 ← core.option.Option.ok_or (balance.checked_sub val1)
            safe_arith.ArithError.Overflow
          let cf2 ← core.result.Result.Insts.CoreOpsTry.branch r2
          match cf2 with
          | core.ops.control_flow.ControlFlow.Continue val2 => do
            let i ← core.cmp.min core.cmp.OrdU64 val2 limit
            ok (core.result.Result.Ok i)
          | core.ops.control_flow.ControlFlow.Break residual =>
            core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual U64
              (core.convert.FromSame safe_arith.ArithError) residual
        | core.ops.control_flow.ControlFlow.Break residual =>
          core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual U64
            (core.convert.FromSame safe_arith.ArithError) residual)
        ⦃ r => absResult (·.val) r =
          (do
            let remainder ← (if increment.val = 0 then throw Spec.SpecError.divisionByZero
              else pure (balance.val % increment.val) : Spec.SpecM Nat)
            let rounded ← (if remainder ≤ balance.val then pure (balance.val - remainder)
              else throw Spec.SpecError.overflow : Spec.SpecM Nat)
            pure (min rounded limit.val)) ⦄ := by
    have hr := U64.checked_rem_bv_spec balance increment
    cases hcr : balance.checked_rem increment <;> simp only [hcr] at hr
    · simp [hr, core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        absResult, absArithError]
      rfl
    rename_i remainder
    have hle : remainder.val ≤ balance.val := by rw [hr.2.1]; exact Nat.mod_le _ _
    have hs := U64.checked_sub_bv_spec balance remainder
    cases hcs : balance.checked_sub remainder <;> simp only [hcs] at hs
    · omega
    rename_i rounded
    simp [hr.1, ← hr.2.1, hle, hcs, hs.2.1, core.result.Result.Insts.CoreOpsTry.branch,
      absResult]
    rfl
  by_cases hA : balance.val + downward.val < effective_balance.val
  · have hA' : below < effective_balance := by simp [hbelow, hA]
    simp only [core.option.Option.ok_or_some, bind_tc_ok, branch_ok, hA', if_true]
    simp only [hfit1', hA, Bind.bind, Except.bind, Pure.pure, Except.pure, throw, throwThe,
      MonadExceptOf.throw, if_true]
    simp only [Bind.bind, Except.bind, Pure.pure, Except.pure, throw, throwThe,
      MonadExceptOf.throw] at hround
    exact hround
  · have hA' : ¬ below < effective_balance := by simp [hbelow, hA]
    cases hc4 : effective_balance.checked_add upward <;> simp only [hc4] at h4
    · have hov : ¬ effective_balance.val + upward.val < 2 ^ 64 := by simp; omega
      simp only [core.option.Option.ok_or_some, core.option.Option.ok_or_none, bind_tc_ok,
        branch_ok, hA', if_false]
      have hov' : ¬ effective_balance.val + upward.val < 18446744073709551616 := by omega
      simp [hfit1, hA, hov', core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        absResult, absArithError]
      rfl
    rename_i above
    have habove : above.val = effective_balance.val + upward.val := h4.2.1
    have hfit4 : effective_balance.val + upward.val < 2 ^ 64 := by simp; omega
    by_cases hB : effective_balance.val + upward.val < balance.val
    · have hB' : above < balance := by simp [habove, hB]
      simp only [core.option.Option.ok_or_some, bind_tc_ok, branch_ok, hA', if_false, hB',
        if_true]
      simp only [hfit1', hA, hfit4, hB, decide_true, Bind.bind, Except.bind, Pure.pure,
        Except.pure, throw, throwThe, MonadExceptOf.throw, if_true, if_false]
      simp only [Bind.bind, Except.bind, Pure.pure, Except.pure, throw, throwThe,
        MonadExceptOf.throw] at hround
      exact hround
    · have hB' : ¬ above < balance := by simp [habove, hB]
      simp only [core.option.Option.ok_or_some, bind_tc_ok, branch_ok, hA', if_false, hB']
      have hfit4' : effective_balance.val + upward.val < 18446744073709551616 := by omega
      simp [hfit1, hA, hfit4', hB, absResult]
      rfl

end EpochProofs
