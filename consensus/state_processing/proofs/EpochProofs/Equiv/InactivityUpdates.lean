import EpochProofs.Equiv.EffectiveBalanceUpdates
import EpochProofs.Sanity.InactivityUpdates

/-!
# Lighthouse inactivity score kernel equals the reference

`new_inactivity_score_equiv` relates `new_inactivity_score` in `inactivity_updates.rs` to
`inactivityScoreStep`. This covers the early return for a participating validator with a
score of 0.

Lighthouse computes the three flags in `single_pass.rs`. The theorem takes them as inputs.
-/

namespace EpochProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result state_processing

theorem new_inactivity_score_equiv (p : Spec.Preset) (score bias recovery_rate : U64)
    (is_eligible is_participating is_in_leak : Bool)
    (hbias : bias.val = p.INACTIVITY_SCORE_BIAS)
    (hrecovery : recovery_rate.val = p.INACTIVITY_SCORE_RECOVERY_RATE) :
    per_epoch_processing.inactivity_updates.new_inactivity_score
      score is_eligible is_participating is_in_leak bias recovery_rate ⦃ r =>
        absResult (·.val) r =
          if is_eligible then Spec.inactivityScoreStep p is_participating is_in_leak score.val
          else .ok score.val ⦄ := by
  unfold per_epoch_processing.inactivity_updates.new_inactivity_score
    U64.Insts.Safe_arithSafeArithU64.safe_add U64.Insts.Safe_arithSafeArithU64.safe_sub
  by_cases he : is_eligible = true
  swap
  · simp [he, absResult]
  have hs := U64.checked_sub_bv_spec score 1#u64
  have hone : (1#u64 : U64).val = 1 := rfl
  simp only [hone] at hs
  have ha := U64.checked_add_bv_spec score bias
  simp only [U64.max_eq] at ha
  have hzero : score = 0#u64 ↔ score.val = 0 := by
    rw [UScalar.eq_equiv]; simp
  have hdeduct : ∀ v : U64,
      (do
        let deduction ← core.cmp.min core.cmp.OrdU64 recovery_rate v
        let r1 ← core.option.Option.ok_or (v.checked_sub deduction) safe_arith.ArithError.Overflow
        let cf1 ← core.result.Result.Insts.CoreOpsTry.branch r1
        match cf1 with
        | core.ops.control_flow.ControlFlow.Continue val1 => ok (core.result.Result.Ok val1)
        | core.ops.control_flow.ControlFlow.Break residual =>
          core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual U64
            (core.convert.FromSame safe_arith.ArithError) residual) ⦃ r =>
        absResult (·.val) r =
          .ok (if v.val > recovery_rate.val then v.val - recovery_rate.val else v.val - v.val) ⦄ := by
    intro v
    have hd := U64.checked_sub_bv_spec v (core.cmp.impls.OrdU64.min recovery_rate v)
    have hmin := core.cmp.impls.OrdU64.min_val recovery_rate v
    cases hc : v.checked_sub (core.cmp.impls.OrdU64.min recovery_rate v) <;> simp only [hc] at hd
    · simp [hmin] at hd
    · simp [hc, core.result.Result.Insts.CoreOpsTry.branch, absResult, hd.2.1, hmin]
      split <;> omega
  by_cases hp : is_participating = true <;> by_cases hl : is_in_leak = true
  all_goals simp only [he, hp, hl, Spec.inactivityScoreStep, Spec.uint64Add,
    Spec.saturating_sub, Spec.UINT64_SIZE, lift, bind_tc_ok, ← hbias, ← hrecovery, if_true,
    if_false, Bool.false_eq_true]
  · split
    · rename_i h0
      simp [hzero.mp h0, absResult]
      rfl
    · rename_i h0
      have h0' : score.val ≠ 0 := fun h => h0 (hzero.mpr h)
      cases hc : score.checked_sub 1#u64 <;> simp only [hc] at hs
      · simp at hs; omega
      · have hdec : (if 1 < score.val then score.val - 1 else 0) = score.val - 1 := by
          split <;> omega
        simp [core.result.Result.Insts.CoreOpsTry.branch, absResult, hs.2.1, hdec]
        rfl
  · split
    · rename_i h0
      simp [hzero.mp h0, absResult]
      rfl
    · rename_i h0
      have h0' : score.val ≠ 0 := fun h => h0 (hzero.mpr h)
      cases hc : score.checked_sub 1#u64 <;> simp only [hc] at hs
      · simp at hs; omega
      · rename_i lowered
        simp only [core.option.Option.ok_or_some, bind_tc_ok, branch_ok]
        apply WP.spec_mono (hdeduct lowered)
        intro r hr
        rw [hr]
        have hlowered : lowered.val = score.val - 1 := hs.2.1
        have hdec : (if 1 < score.val then score.val - 1 else 0) = score.val - 1 := by
          split <;> omega
        simp [hlowered, hdec, Bind.bind, Except.bind, Pure.pure, Except.pure]
  · cases hc : score.checked_add bias <;> simp only [hc] at ha
    · have hov : ¬ score.val + bias.val < 18446744073709551616 := by omega
      simp [hov, core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        absResult, absArithError]
      rfl
    · have hfit : score.val + bias.val < 18446744073709551616 := by omega
      simp [hfit, core.result.Result.Insts.CoreOpsTry.branch, absResult, ha.2.1]
      rfl
  · cases hc : score.checked_add bias <;> simp only [hc] at ha
    · have hov : ¬ score.val + bias.val < 18446744073709551616 := by omega
      simp [hov, core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        absResult, absArithError]
      rfl
    · rename_i raised
      have hfit : score.val + bias.val < 18446744073709551616 := by omega
      simp only [core.option.Option.ok_or_some, bind_tc_ok, branch_ok]
      apply WP.spec_mono (hdeduct raised)
      intro r hr
      rw [hr]
      simp [hfit, ha.2.1, Bind.bind, Except.bind, Pure.pure, Except.pure]

end EpochProofs
