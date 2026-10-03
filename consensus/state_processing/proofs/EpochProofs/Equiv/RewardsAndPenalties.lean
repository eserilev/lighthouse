import EpochProofs.Equiv.Slashings
import EpochProofs.Sanity.RewardsAndPenalties

/-!
# Lighthouse rewards and penalties equal the reference

`flag_delta_equiv` and `inactivity_penalty_equiv` relate the Rust helpers to `flagDelta` and
`inactivityDelta`. `new_balance_after_rewards_equiv` relates `new_balance_after_rewards` to
`rewardsCombined` over `rewardDeltas`.

`new_balance_after_rewards_eq_spec` then states when Lighthouse equals the spec's four-round
application: the balance covers all penalties and no addition overflows. See
`rewards_saturation_example` for an input where the two differ.

Lighthouse computes `base_reward`, the participation flags, the leak flag and the increments
in `single_pass.rs`. The theorems take them as inputs.
-/

namespace EpochProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result state_processing

theorem except_ok_bind {ε α β : Type} (x : α) (f : α → Except ε β) :
    (Except.ok x >>= f) = f x := rfl

theorem except_error_bind {ε α β : Type} (e : ε) (f : α → Except ε β) :
    (Except.error e >>= f) = Except.error e := rfl

theorem weight_denominator_val : types.core.consts.altair.WEIGHT_DENOMINATOR.val = 64 := by
  unfold types.core.consts.altair.WEIGHT_DENOMINATOR; rfl
theorem source_weight_val : types.core.consts.altair.TIMELY_SOURCE_WEIGHT.val = 14 := by
  unfold types.core.consts.altair.TIMELY_SOURCE_WEIGHT; rfl
theorem target_weight_val : types.core.consts.altair.TIMELY_TARGET_WEIGHT.val = 26 := by
  unfold types.core.consts.altair.TIMELY_TARGET_WEIGHT; rfl
theorem head_weight_val : types.core.consts.altair.TIMELY_HEAD_WEIGHT.val = 14 := by
  unfold types.core.consts.altair.TIMELY_HEAD_WEIGHT; rfl

theorem flag_delta_equiv (base_reward weight increments active_increments : U64)
    (is_participating is_head_flag in_leak : Bool) :
    per_epoch_processing.rewards_penalties.flag_delta base_reward weight is_participating
      is_head_flag in_leak increments active_increments ⦃ r =>
        absResult (fun x => (x.1.val, x.2.val)) r =
          Spec.flagDelta base_reward.val weight.val is_participating is_head_flag in_leak
            increments.val active_increments.val ⦄ := by
  unfold per_epoch_processing.rewards_penalties.flag_delta
    U64.Insts.Safe_arithSafeArithU64.safe_mul U64.Insts.Safe_arithSafeArithU64.safe_div
  simp only [Spec.flagDelta, Spec.uint64Mul, Spec.uint64Div, Spec.UINT64_SIZE,
    Spec.WEIGHT_DENOMINATOR, lift, bind_tc_ok]
  have h1 := U64.checked_mul_bv_spec base_reward weight
  simp only [U64.max_eq] at h1
  by_cases hp : is_participating = true
  · by_cases hl : in_leak = true
    · simp [hp, hl, absResult, Pure.pure, Except.pure]
    · cases hc1 : base_reward.checked_mul weight <;> simp only [hc1] at h1
      · have hov : ¬ base_reward.val * weight.val < 18446744073709551616 := by omega
        simp [hp, hl, hov, core.result.Result.Insts.CoreOpsTry.branch,
          core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
          absResult, absArithError, Bind.bind, Except.bind, throw,
          throwThe, MonadExceptOf.throw]
      rename_i v1
      have hv1 : v1.val = base_reward.val * weight.val := h1.2.1
      have hfit1 : base_reward.val * weight.val < 18446744073709551616 := by omega
      have h2 := U64.checked_mul_bv_spec v1 increments
      simp only [U64.max_eq, hv1] at h2
      cases hc2 : v1.checked_mul increments <;> simp only [hc2] at h2
      · have hov : ¬ base_reward.val * weight.val * increments.val < 18446744073709551616 := by
          omega
        simp [hp, hl, hov, hfit1, hc2, core.result.Result.Insts.CoreOpsTry.branch,
          core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
          absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, throw,
          throwThe, MonadExceptOf.throw]
      rename_i v2
      have hfit2 : base_reward.val * weight.val * increments.val < 18446744073709551616 := by
        omega
      have h3 := U64.checked_mul_bv_spec active_increments
        types.core.consts.altair.WEIGHT_DENOMINATOR
      simp only [U64.max_eq, weight_denominator_val] at h3
      cases hc3 : active_increments.checked_mul types.core.consts.altair.WEIGHT_DENOMINATOR <;>
        simp only [hc3] at h3
      · have hov : ¬ active_increments.val * 64 < 18446744073709551616 := by omega
        simp [hp, hl, hov, hfit1, hfit2, hc2, 
          core.result.Result.Insts.CoreOpsTry.branch,
          core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
          absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, throw,
          throwThe, MonadExceptOf.throw]
      rename_i v3
      have hfit3 : active_increments.val * 64 < 18446744073709551616 := by omega
      have hv2 : v2.val < 18446744073709551616 := by rw [h2.2.1]; exact hfit2
      have hv3 : v3.val < 18446744073709551616 := by rw [h3.2.1]; exact hfit3
      have h4 := U64.checked_div_bv_spec v2 v3
      cases hc4 : v2.checked_div v3 <;> simp only [hc4] at h4
      · simp [hp, hl, hfit1, hc2, hc4, ← h2.2.1, ← h3.2.1, hv2, h4,
          core.result.Result.Insts.CoreOpsTry.branch,
          core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
          absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, throw,
          throwThe, MonadExceptOf.throw]
      · simp [hp, hl, hfit1, hc2, hc4, ← h2.2.1, ← h3.2.1, hv2, hv3, h4.1,
          h4.2.1, core.result.Result.Insts.CoreOpsTry.branch, absResult, Bind.bind,
          Except.bind, Pure.pure, Except.pure]
  · by_cases hh : is_head_flag = true
    · simp [hp, hh, absResult, Pure.pure, Except.pure]
    · cases hc1 : base_reward.checked_mul weight <;> simp only [hc1] at h1
      · have hov : ¬ base_reward.val * weight.val < 18446744073709551616 := by omega
        simp [hp, hh, hov, core.result.Result.Insts.CoreOpsTry.branch,
          core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
          absResult, absArithError, Bind.bind, Except.bind, throw,
          throwThe, MonadExceptOf.throw]
      rename_i v1
      have hfit1 : base_reward.val * weight.val < 18446744073709551616 := by omega
      have h2 := U64.checked_div_bv_spec v1 types.core.consts.altair.WEIGHT_DENOMINATOR
      simp only [weight_denominator_val] at h2
      cases hc2 : v1.checked_div types.core.consts.altair.WEIGHT_DENOMINATOR <;>
        simp only [hc2] at h2
      · simp at h2
      · have hv1 : v1.val < 18446744073709551616 := by rw [h1.2.1]; exact hfit1
        simp [hp, hh, hv1, hc2, ← h1.2.1, h2.2.1, 
          core.result.Result.Insts.CoreOpsTry.branch, absResult, Bind.bind, Except.bind,
          Pure.pure, Except.pure]

theorem inactivity_penalty_equiv (p : Spec.Preset)
    (effective_balance inactivity_score bias quotient : U64) (is_participating_target : Bool)
    (hbias : bias.val = p.INACTIVITY_SCORE_BIAS)
    (hquotient : quotient.val = p.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX) :
    per_epoch_processing.rewards_penalties.inactivity_penalty effective_balance inactivity_score
      is_participating_target bias quotient ⦃ r =>
        absResult (·.val) r =
          Spec.inactivityDelta p effective_balance.val inactivity_score.val
            is_participating_target ⦄ := by
  unfold per_epoch_processing.rewards_penalties.inactivity_penalty
    U64.Insts.Safe_arithSafeArithU64.safe_mul U64.Insts.Safe_arithSafeArithU64.safe_div
  simp only [Spec.inactivityDelta, Spec.uint64Mul, Spec.uint64Div, Spec.UINT64_SIZE, lift,
    bind_tc_ok, ← hbias, ← hquotient]
  by_cases hp : is_participating_target = true
  · simp [hp, absResult, Pure.pure, Except.pure]
  have h1 := U64.checked_mul_bv_spec effective_balance inactivity_score
  simp only [U64.max_eq] at h1
  cases hc1 : effective_balance.checked_mul inactivity_score <;> simp only [hc1] at h1
  · have hov : ¬ effective_balance.val * inactivity_score.val < 18446744073709551616 := by omega
    simp [hp, hov, core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        absResult, absArithError, Bind.bind, Except.bind, throw,
        throwThe, MonadExceptOf.throw]
  rename_i num
  have hfit1 : effective_balance.val * inactivity_score.val < 18446744073709551616 := by omega
  have hnum : num.val < 18446744073709551616 := by rw [h1.2.1]; exact hfit1
  have h2 := U64.checked_mul_bv_spec bias quotient
  simp only [U64.max_eq] at h2
  cases hc2 : bias.checked_mul quotient <;> simp only [hc2] at h2
  · have hov : ¬ bias.val * quotient.val < 18446744073709551616 := by omega
    simp [hp, hov, hfit1, core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, throw,
        throwThe, MonadExceptOf.throw]
  rename_i den
  have hfit2 : bias.val * quotient.val < 18446744073709551616 := by omega
  have hden : den.val < 18446744073709551616 := by rw [h2.2.1]; exact hfit2
  have h3 := U64.checked_div_bv_spec num den
  cases hc3 : num.checked_div den <;> simp only [hc3] at h3
  · simp [hp, ← h1.2.1, ← h2.2.1, hnum, h3, hc3, core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, throw,
        throwThe, MonadExceptOf.throw]
  · simp [hp, ← h1.2.1, ← h2.2.1, hnum, hden, h3.1, h3.2.1, hc3, core.result.Result.Insts.CoreOpsTry.branch,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        absResult, absArithError, Bind.bind, Except.bind, Pure.pure, Except.pure, 
        ]

theorem result_bind {S T : Type} (m : Result (core.result.Result S safe_arith.ArithError))
    (K : S → Result (core.result.Result T safe_arith.ArithError))
    (Q : core.result.Result S safe_arith.ArithError → Prop)
    (P : core.result.Result T safe_arith.ArithError → Prop)
    (hm : m ⦃ Q ⦄)
    (hok : ∀ s, Q (.Ok s) → K s ⦃ P ⦄)
    (herr : ∀ e, Q (.Err e) → P (.Err e)) :
    (do
      let r ← m
      let cf ← core.result.Result.Insts.CoreOpsTry.branch r
      match cf with
      | core.ops.control_flow.ControlFlow.Continue v => K v
      | core.ops.control_flow.ControlFlow.Break residual =>
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual T
          (core.convert.FromSame safe_arith.ArithError) residual) ⦃ P ⦄ := by
  obtain ⟨r, hr, hq⟩ := spec_imp_exists hm
  rw [hr]
  cases r with
  | Ok s => simpa [core.result.Result.Insts.CoreOpsTry.branch] using hok s hq
  | Err e =>
    simp [core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      core.convert.FromSame.from]
    exact herr e hq

theorem safe_add_spec (a b : U64) :
    U64.Insts.Safe_arithSafeArithU64.safe_add a b ⦃ r =>
      (∀ v, r = .Ok v → v.val = a.val + b.val ∧ a.val + b.val < 18446744073709551616)
      ∧ (∀ e, r = .Err e → e = .Overflow ∧ ¬ a.val + b.val < 18446744073709551616) ⦄ := by
  unfold U64.Insts.Safe_arithSafeArithU64.safe_add
  have h := U64.checked_add_bv_spec a b
  simp only [U64.max_eq] at h
  cases hc : a.checked_add b <;> simp only [hc] at h
  · simp [lift]; omega
  · simp [lift, h.2.1]; omega


theorem new_balance_after_rewards_equiv (p : Spec.Preset)
    (balance base_reward effective_balance inactivity_score source_increments target_increments
      head_increments active_increments bias quotient : U64)
    (is_eligible source target head in_leak : Bool)
    (hbias : bias.val = p.INACTIVITY_SCORE_BIAS)
    (hquotient : quotient.val = p.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX) :
    per_epoch_processing.rewards_penalties.new_balance_after_rewards balance is_eligible
      base_reward effective_balance inactivity_score source target head in_leak
      source_increments target_increments head_increments active_increments bias quotient
      ⦃ r =>
        absResult (·.val) r =
          if is_eligible then
            (do
              let deltas ← Spec.rewardDeltas p base_reward.val effective_balance.val
                inactivity_score.val source target head in_leak source_increments.val
                target_increments.val head_increments.val active_increments.val
              Spec.rewardsCombined balance.val deltas)
          else .ok balance.val ⦄ := by
  unfold per_epoch_processing.rewards_penalties.new_balance_after_rewards
  by_cases he : is_eligible = true
  swap
  · simp [he, absResult]
  apply exists_imp_spec
  obtain ⟨rS, hrS, hS⟩ := spec_imp_exists (flag_delta_equiv base_reward
    types.core.consts.altair.TIMELY_SOURCE_WEIGHT source_increments active_increments source false
    in_leak)
  obtain ⟨rT, hrT, hT⟩ := spec_imp_exists (flag_delta_equiv base_reward
    types.core.consts.altair.TIMELY_TARGET_WEIGHT target_increments active_increments target false
    in_leak)
  obtain ⟨rH, hrH, hH⟩ := spec_imp_exists (flag_delta_equiv base_reward
    types.core.consts.altair.TIMELY_HEAD_WEIGHT head_increments active_increments head true
    in_leak)
  obtain ⟨rI, hrI, hI⟩ := spec_imp_exists (inactivity_penalty_equiv p effective_balance
    inactivity_score bias quotient target hbias hquotient)
  simp only [source_weight_val, target_weight_val, head_weight_val, absResult] at hS hT hH hI
  simp only [he, if_true, hrS, hrT, hrH, hrI, bind_tc_ok, Spec.rewardDeltas, Spec.TIMELY_SOURCE_WEIGHT, Spec.TIMELY_TARGET_WEIGHT, Spec.TIMELY_HEAD_WEIGHT]
  cases rS with
  | Err e =>
    simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      core.convert.FromSame.from]
    exact ⟨_, rfl, by rw [← hS]; rfl⟩
  | Ok x =>
  obtain ⟨source_reward, source_penalty⟩ := x
  cases rT with
  | Err e =>
    simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      core.convert.FromSame.from]
    exact ⟨_, rfl, by rw [← hS, ← hT]; rfl⟩
  | Ok x =>
  obtain ⟨target_reward, target_penalty⟩ := x
  cases rH with
  | Err e =>
    simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      core.convert.FromSame.from]
    exact ⟨_, rfl, by rw [← hS, ← hT, ← hH]; rfl⟩
  | Ok x =>
  obtain ⟨head_reward, head_penalty⟩ := x
  cases rI with
  | Err e =>
    simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      core.convert.FromSame.from]
    exact ⟨_, rfl, by rw [← hS, ← hT, ← hH, ← hI]; rfl⟩
  | Ok inactivity =>
  simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok]
  rw [← hS, ← hT, ← hH, ← hI]
  simp only [except_ok_bind, Spec.rewardsCombined, uncurry_apply_pair]
  have hb_source_reward : source_reward.val < 18446744073709551616 := by have := source_reward.hBounds; simp at this; omega
  have hb_target_reward : target_reward.val < 18446744073709551616 := by have := target_reward.hBounds; simp at this; omega
  have hb_head_reward : head_reward.val < 18446744073709551616 := by have := head_reward.hBounds; simp at this; omega
  have hb_source_penalty : source_penalty.val < 18446744073709551616 := by have := source_penalty.hBounds; simp at this; omega
  have hb_target_penalty : target_penalty.val < 18446744073709551616 := by have := target_penalty.hBounds; simp at this; omega
  have hb_head_penalty : head_penalty.val < 18446744073709551616 := by have := head_penalty.hBounds; simp at this; omega
  have hb_inactivity : inactivity.val < 18446744073709551616 := by have := inactivity.hBounds; simp at this; omega
  have hb_balance : balance.val < 18446744073709551616 := by have := balance.hBounds; simp at this; omega
  obtain ⟨r4, hr4, h4⟩ := spec_imp_exists (safe_add_spec source_reward target_reward)
  rw [hr4]
  cases r4 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h4.2 e rfl
    simp only [bind_tc_ok,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        core.convert.FromSame.from]
    refine ⟨_, rfl, ?_⟩
    simp [absResult, absArithError, List.foldlM_cons, List.foldlM_nil, Spec.uint64Add,
        Spec.UINT64_SIZE, except_ok_bind, except_error_bind, Pure.pure, Except.pure, throw,
        throwThe, MonadExceptOf.throw, hb_source_reward, hb_source_penalty, hov]
  | Ok v4 =>
  obtain ⟨hv4, hfit4⟩ := h4.1 v4 rfl
  simp only [bind_tc_ok]
  obtain ⟨r5, hr5, h5⟩ := spec_imp_exists (safe_add_spec v4 head_reward)
  rw [hr5]
  cases r5 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h5.2 e rfl
    simp only [hv4] at hov
    simp only [bind_tc_ok,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        core.convert.FromSame.from]
    refine ⟨_, rfl, ?_⟩
    simp [absResult, absArithError, List.foldlM_cons, List.foldlM_nil, Spec.uint64Add,
        Spec.UINT64_SIZE, except_ok_bind, except_error_bind, Pure.pure, Except.pure, throw,
        throwThe, MonadExceptOf.throw, hb_source_reward, hb_source_penalty, hfit4, hov]
  | Ok v5 =>
  obtain ⟨hv5, hfit5⟩ := h5.1 v5 rfl
  simp only [hv4] at hv5 hfit5
  simp only [bind_tc_ok]
  obtain ⟨r6, hr6, h6⟩ := spec_imp_exists (safe_add_spec source_penalty target_penalty)
  rw [hr6]
  cases r6 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h6.2 e rfl
    simp only [bind_tc_ok,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        core.convert.FromSame.from]
    refine ⟨_, rfl, ?_⟩
    simp [absResult, absArithError, List.foldlM_cons, List.foldlM_nil, Spec.uint64Add,
        Spec.UINT64_SIZE, except_ok_bind, except_error_bind, Pure.pure, Except.pure, throw,
        throwThe, MonadExceptOf.throw, hb_source_reward, hb_source_penalty, hfit4, hfit5, hov]
  | Ok v6 =>
  obtain ⟨hv6, hfit6⟩ := h6.1 v6 rfl
  simp only [bind_tc_ok]
  obtain ⟨r7, hr7, h7⟩ := spec_imp_exists (safe_add_spec v6 head_penalty)
  rw [hr7]
  cases r7 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h7.2 e rfl
    simp only [hv6] at hov
    simp only [bind_tc_ok,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        core.convert.FromSame.from]
    refine ⟨_, rfl, ?_⟩
    simp [absResult, absArithError, List.foldlM_cons, List.foldlM_nil, Spec.uint64Add,
        Spec.UINT64_SIZE, except_ok_bind, except_error_bind, Pure.pure, Except.pure, throw,
        throwThe, MonadExceptOf.throw, hb_source_reward, hb_source_penalty, hfit4, hfit5, hfit6, hov]
  | Ok v7 =>
  obtain ⟨hv7, hfit7⟩ := h7.1 v7 rfl
  simp only [hv6] at hv7 hfit7
  simp only [bind_tc_ok]
  obtain ⟨r8, hr8, h8⟩ := spec_imp_exists (safe_add_spec v7 inactivity)
  rw [hr8]
  cases r8 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h8.2 e rfl
    simp only [hv7] at hov
    simp only [bind_tc_ok,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        core.convert.FromSame.from]
    refine ⟨_, rfl, ?_⟩
    simp [absResult, absArithError, List.foldlM_cons, List.foldlM_nil, Spec.uint64Add,
        Spec.UINT64_SIZE, except_ok_bind, except_error_bind, Pure.pure, Except.pure, throw,
        throwThe, MonadExceptOf.throw, hb_source_reward, hb_source_penalty, hfit4, hfit5, hfit6, hfit7, hov]
  | Ok v8 =>
  obtain ⟨hv8, hfit8⟩ := h8.1 v8 rfl
  simp only [hv7] at hv8 hfit8
  simp only [bind_tc_ok]
  obtain ⟨r9, hr9, h9⟩ := spec_imp_exists (safe_add_spec balance v5)
  rw [hr9]
  cases r9 with
  | Err e =>
    obtain ⟨rfl, hov⟩ := h9.2 e rfl
    simp only [hv5] at hov
    simp only [bind_tc_ok,
        core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
        core.convert.FromSame.from]
    refine ⟨_, rfl, ?_⟩
    simp [absResult, absArithError, List.foldlM_cons, List.foldlM_nil, Spec.uint64Add,
        Spec.UINT64_SIZE, except_ok_bind, except_error_bind, Pure.pure, Except.pure, throw,
        throwThe, MonadExceptOf.throw, hb_source_reward, hb_source_penalty, hfit4, hfit5, hfit6, hfit7, hfit8, hov]
  | Ok v9 =>
  obtain ⟨hv9, hfit9⟩ := h9.1 v9 rfl
  simp only [hv5] at hv9 hfit9
  simp only [bind_tc_ok]
  refine ⟨_, rfl, ?_⟩
  simp [absResult, List.foldlM_cons, List.foldlM_nil, Spec.uint64Add,
        Spec.UINT64_SIZE, except_ok_bind, Pure.pure, Except.pure, throw,
        throwThe, MonadExceptOf.throw, hb_source_reward, hb_source_penalty, hfit4, hfit5, hfit6, hfit7, hv8, hfit8, hv9, hfit9,
    u64_saturating_sub_val, saturating_sub_eq]


/-- For an eligible validator whose balance covers all penalties, Lighthouse equals the spec's
four-round application of the deltas. -/
theorem new_balance_after_rewards_eq_spec (p : Spec.Preset)
    (balance base_reward effective_balance inactivity_score source_increments target_increments
      head_increments active_increments bias quotient : U64)
    (source target head in_leak : Bool) (deltas : List (Nat × Nat))
    (hbias : bias.val = p.INACTIVITY_SCORE_BIAS)
    (hquotient : quotient.val = p.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX)
    (hdeltas : Spec.rewardDeltas p base_reward.val effective_balance.val inactivity_score.val
      source target head in_leak source_increments.val target_increments.val
      head_increments.val active_increments.val = .ok deltas)
    (hpenalties : (deltas.map (·.2)).sum ≤ balance.val)
    (hfit : balance.val + (deltas.map (·.1)).sum < 2 ^ 64) :
    per_epoch_processing.rewards_penalties.new_balance_after_rewards balance true
      base_reward effective_balance inactivity_score source target head in_leak
      source_increments target_increments head_increments active_increments bias quotient
      ⦃ r => absResult (·.val) r = Spec.rewardsSequential balance.val deltas ⦄ := by
  apply WP.spec_mono (new_balance_after_rewards_equiv p balance base_reward effective_balance
    inactivity_score source_increments target_increments head_increments active_increments bias
    quotient true source target head in_leak hbias hquotient)
  intro r hr
  rw [hr]
  simp only [if_true, hdeltas, except_ok_bind]
  exact Spec.rewardsCombined_eq_sequential balance.val deltas hpenalties hfit

end EpochProofs
