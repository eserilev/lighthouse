import EpochProofs.Equiv.InactivityUpdates
import EpochProofs.Sanity.Slashings

/-!
# Lighthouse slashings penalty equals the reference

`slashings_context_equiv` relates `slashings_context` to `slashingsPreamble` and the target
epoch. `new_balance_after_slashing_equiv` relates `new_balance_after_slashing` to
`slashingBalanceStep` after Electra.

Lighthouse sums `state.slashings` in `single_pass.rs` with `safe_sum`. The theorem takes the
sum as an input.
-/

namespace EpochProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result state_processing

theorem u64_saturating_sub_val (a b : U64) :
    (core.num.U64.saturating_sub a b).val = a.val - b.val := by
  have hlt : a.val - b.val < 2 ^ UScalarTy.U64.numBits := by
    have := a.hBounds; simp at this ⊢; omega
  simp only [core.num.U64.saturating_sub, UScalar.saturating_sub, UScalar.val,
    BitVec.toNat_ofNat]
  simp only [UScalar.val] at hlt
  rw [Nat.mod_eq_of_lt (by omega)]
  omega

theorem slashings_context_equiv (p : Spec.Preset) (slashings : List Spec.Gwei)
    (sum_slashings multiplier total_active_balance current_epoch vector increment : U64)
    (hsum : sum_slashings.val = slashings.sum)
    (hmultiplier : multiplier.val = p.PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX)
    (hvector : vector.val = p.EPOCHS_PER_SLASHINGS_VECTOR)
    (hincrement : increment.val = p.EFFECTIVE_BALANCE_INCREMENT)
    (htarget : current_epoch.val + vector.val / 2 < 18446744073709551616) :
    per_epoch_processing.slashings_penalty.slashings_context
      sum_slashings multiplier total_active_balance current_epoch vector increment ⦃ r =>
        absResult (fun x => (x.1.val, x.2.2.val)) r =
          Spec.slashingsPreamble p total_active_balance.val slashings
        ∧ ∀ x, r = .Ok x → x.2.1.val = current_epoch.val + p.EPOCHS_PER_SLASHINGS_VECTOR / 2 ⦄ := by
  have hsumfit : Spec.uint64Sum slashings = .ok sum_slashings.val := by
    have := sum_slashings.hBounds
    simp only [Spec.uint64Sum, ← hsum, Spec.UINT64_SIZE]
    simp at this ⊢; simp [this, pure, Except.pure]
  unfold per_epoch_processing.slashings_penalty.slashings_context
    U64.Insts.Safe_arithSafeArithU64.safe_div U64.Insts.Safe_arithSafeArithU64.safe_mul
    U64.Insts.Safe_arithSafeArithU64.safe_add
  simp only [Spec.slashingsPreamble, hsumfit, Spec.uint64Mul, Spec.uint64Div, Spec.UINT64_SIZE,
    lift, bind_tc_ok, ← hmultiplier, ← hincrement, ← hvector]
  have h1 := U64.checked_mul_bv_spec sum_slashings multiplier
  simp only [U64.max_eq] at h1
  cases hc1 : sum_slashings.checked_mul multiplier <;> simp only [hc1] at h1
  · have hov : ¬ sum_slashings.val * multiplier.val < 18446744073709551616 := by omega
    simp [hov, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      absResult, absArithError, Bind.bind, Except.bind, throw, throwThe,
      MonadExceptOf.throw]
  rename_i scaled
  have hfit1 : sum_slashings.val * multiplier.val < 18446744073709551616 := by omega
  have hscaled : scaled.val = sum_slashings.val * multiplier.val := h1.2.1
  have hv := U64.checked_div_bv_spec vector 2#u64
  cases hcv : vector.checked_div 2#u64 <;> simp only [hcv] at hv
  · simp at hv
  rename_i half
  have hhalf : half.val = vector.val / 2 := by simpa using hv.2.1
  have ha := U64.checked_add_bv_spec current_epoch half
  simp only [U64.max_eq] at ha
  cases hca : current_epoch.checked_add half <;> simp only [hca] at ha
  · omega
  rename_i target
  have hd := U64.checked_div_bv_spec total_active_balance increment
  cases hcd : total_active_balance.checked_div increment <;> simp only [hcd] at hd
  · simp [hd, hca, hfit1, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, throw, throwThe,
      MonadExceptOf.throw]
  rename_i increments
  have hm := core.cmp.impls.OrdU64.min_val scaled total_active_balance
  have hd2 := U64.checked_div_bv_spec (core.cmp.impls.OrdU64.min scaled total_active_balance)
    increments
  cases hcd2 : (core.cmp.impls.OrdU64.min scaled total_active_balance).checked_div increments <;>
    simp only [hcd2] at hd2
  · have hlt : total_active_balance.val < increment.val := by
      have h0 : total_active_balance.val / increment.val = 0 := by rw [← hd.2.1]; exact hd2
      rcases Nat.div_eq_zero_iff.mp h0 with h | h
      · exact absurd h hd.1
      · exact h
    simp [hlt, hd.1, hca, hcd2, hfit1, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, throw, throwThe,
      MonadExceptOf.throw]
  · have hnlt : ¬ total_active_balance.val < increment.val := by
      intro h
      apply hd2.1
      rw [hd.2.1]
      exact Nat.div_eq_of_lt h
    simp [hnlt, hd.1, hd.2.1, hd2.2.1, hm, hca, hcd2, hscaled, hfit1, ha.2.1,
      hhalf, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, 
      ]

theorem saturating_sub_eq (a b : Nat) : Spec.saturating_sub a b = a - b := by
  unfold Spec.saturating_sub
  split
  · rfl
  · rename_i h
    rw [Nat.sub_self, Nat.sub_eq_zero_of_le (Nat.le_of_not_gt h)]

theorem new_balance_after_slashing_equiv (p : Spec.Preset) (validator : Spec.Validator)
    (balance withdrawable_epoch effective_balance target adjusted penalty_per_increment
      total_active_balance increment : U64) (slashed : Bool)
    (hslashed : validator.slashed = slashed)
    (hwithdrawable : withdrawable_epoch.val = validator.withdrawable_epoch)
    (heffective : effective_balance.val = validator.effective_balance)
    (hincrement : increment.val = p.EFFECTIVE_BALANCE_INCREMENT) :
    per_epoch_processing.slashings_penalty.new_balance_after_slashing balance slashed
      withdrawable_epoch effective_balance target adjusted penalty_per_increment
      total_active_balance increment true ⦃ r =>
        absResult (·.val) r =
          Spec.slashingBalanceStep p target.val penalty_per_increment.val validator balance.val ⦄ := by
  unfold per_epoch_processing.slashings_penalty.new_balance_after_slashing
    U64.Insts.Safe_arithSafeArithU64.safe_div U64.Insts.Safe_arithSafeArithU64.safe_mul
  simp only [Spec.slashingBalanceStep, Spec.uint64Div, Spec.uint64Mul, Spec.UINT64_SIZE,
    saturating_sub_eq, lift, bind_tc_ok, hslashed, ← hwithdrawable, ← heffective, ← hincrement]
  by_cases hs : slashed = true
  swap
  · simp [hs, absResult, Pure.pure, Except.pure]
  by_cases ht : target.val = withdrawable_epoch.val
  swap
  · have ht' : target ≠ withdrawable_epoch := fun h => ht (by rw [h])
    simp [hs, ht, ht', absResult, Pure.pure, Except.pure]
  have ht' : target = withdrawable_epoch := by rw [UScalar.eq_equiv]; exact ht
  have hd := U64.checked_div_bv_spec effective_balance increment
  cases hcd : effective_balance.checked_div increment <;> simp only [hcd] at hd
  · simp [hs, ht', hd, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      absResult, absArithError, Bind.bind, Except.bind, throw, throwThe,
      MonadExceptOf.throw]
  rename_i n
  have hm := U64.checked_mul_bv_spec penalty_per_increment n
  simp only [U64.max_eq] at hm
  cases hcm : penalty_per_increment.checked_mul n <;> simp only [hcm] at hm
  · have hov : ¬ penalty_per_increment.val * n.val < 18446744073709551616 := by omega
    simp [hs, ht', hd.1, ← hd.2.1, hov, hcm, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, throw, throwThe,
      MonadExceptOf.throw]
  · have hfit : penalty_per_increment.val * n.val < 18446744073709551616 := by omega
    simp [hs, ht', hd.1, ← hd.2.1, hfit, hcm, hm.2.1, u64_saturating_sub_val, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, 
      ]

end EpochProofs
