import EpochProofs.Equiv.PendingConsolidations
import EpochProofs.Sanity.LighthouseRow

/-!
# Lighthouse single-pass step equals the row step

`single_pass_step_equiv` relates `single_pass_step` in `single_pass_step.rs` to
`Spec.lhRowStepFull`. Each of the six steps has its own lemma. The main theorem chains them.
-/

namespace EpochProofs

open Aeneas Aeneas.Std Aeneas.Std.WP Result state_processing

open state_processing.per_epoch_processing.single_pass_step (ValidatorRow ExitChurn StepContext RowInputs)

/-- Each field of the Lighthouse row equals the matching field of the spec row. -/
structure RowMatches (lr : ValidatorRow) (r : Spec.Row) : Prop where
  balance : lr.balance.val = r.balance
  inactivity_score : lr.inactivity_score.val = r.inactivity_score
  effective_balance : lr.effective_balance.val = r.validator.effective_balance
  slashed : lr.slashed = r.validator.slashed
  activation_eligibility_epoch :
    lr.activation_eligibility_epoch.val = r.validator.activation_eligibility_epoch
  activation_epoch : lr.activation_epoch.val = r.validator.activation_epoch
  exit_epoch : lr.exit_epoch.val = r.validator.exit_epoch
  withdrawable_epoch : lr.withdrawable_epoch.val = r.validator.withdrawable_epoch
  previous_participation : lr.previous_epoch_participation.val = r.previous_participation.toNat

/-- The Lighthouse context holds the spec context values and the preset constants. All step
switches are on, as in production. -/
structure ContextMatches (p : Spec.Preset) (c : StepContext) (ctx : Spec.LhStepContext) :
    Prop where
  current_epoch : c.current_epoch.val = ctx.current_epoch
  previous_epoch : c.previous_epoch.val = ctx.previous_epoch
  finalized_epoch : c.finalized_epoch.val = ctx.finalized_epoch
  in_leak : c.is_in_inactivity_leak = ctx.in_leak
  bias : c.inactivity_score_bias.val = p.INACTIVITY_SCORE_BIAS
  recovery_rate : c.inactivity_score_recovery_rate.val = p.INACTIVITY_SCORE_RECOVERY_RATE
  penalty_quotient : c.inactivity_penalty_quotient.val = p.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX
  source_increments : c.source_increments.val = ctx.source_increments
  target_increments : c.target_increments.val = ctx.target_increments
  head_increments : c.head_increments.val = ctx.head_increments
  active_increments : c.active_increments.val = ctx.active_increments
  total_active_balance : c.total_active_balance.val = ctx.total_active_balance
  slashings_target : c.target_withdrawable_epoch.val = ctx.slashings_target
  penalty_per_increment : c.penalty_per_effective_balance_increment.val = ctx.penalty_per_increment
  effective_balance_increment : c.effective_balance_increment.val = p.EFFECTIVE_BALANCE_INCREMENT
  after_genesis : c.after_genesis = true
  inactivity_updates : c.inactivity_updates = true
  rewards_and_penalties : c.rewards_and_penalties = true
  registry_updates : c.registry_updates = true
  slashings : c.slashings = true
  pending_deposits : c.pending_deposits = true
  effective_balance_updates : c.effective_balance_updates = true

/-- The row inputs hold the spec inputs. `effective_balance_limit` is
`get_max_effective_balance` of the validator, which `single_pass.rs` computes. -/
structure InputsMatches (p : Spec.Preset) (i : RowInputs) (inputs : Spec.LhRowInputs)
    (r : Spec.Row) : Prop where
  base_reward : i.base_reward.val = inputs.base_reward
  deposit : i.deposit.val = inputs.deposit
  in_consolidation : i.in_consolidation = inputs.in_consolidation
  effective_balance_limit : i.effective_balance_limit.val = Spec.get_max_effective_balance p r.validator

/-- The spec view of a Lighthouse step result. The step writes the balance, the inactivity
score, the effective balance, the four validator epochs and the churn. The rest of the row
comes from `r`. -/
def absStep (r : Spec.Row) :
    core.result.Result (ValidatorRow × ExitChurn) safe_arith.ArithError →
      Spec.SpecM ((Spec.Epoch × Spec.Gwei) × Spec.Row)
  | .Ok (lr', ch') =>
    .ok ((ch'.earliest_exit_epoch.val, ch'.exit_balance_to_consume.val),
      { r with
        balance := lr'.balance.val
        inactivity_score := lr'.inactivity_score.val
        validator := { r.validator with
          effective_balance := lr'.effective_balance.val
          activation_eligibility_epoch := lr'.activation_eligibility_epoch.val
          activation_epoch := lr'.activation_epoch.val
          exit_epoch := lr'.exit_epoch.val
          withdrawable_epoch := lr'.withdrawable_epoch.val } })
  | .Err e => .error (absArithError e)

/-- `is_active_previous_epoch` is `is_active_validator`. -/
theorem is_active_previous_epoch_eq (lr : ValidatorRow) (r : Spec.Row) (hm : RowMatches lr r)
    (previous_epoch : U64) :
    per_epoch_processing.single_pass_step.is_active_previous_epoch lr previous_epoch =
      ok (Spec.is_active_validator r.validator previous_epoch.val) := by
  unfold per_epoch_processing.single_pass_step.is_active_previous_epoch
  simp only [Spec.is_active_validator, ← hm.activation_epoch, ← hm.exit_epoch]
  split <;> rename_i h <;> simp at h <;> simp [h]

/-- `is_eligible` is `validatorEligible`. -/
theorem is_eligible_equiv (lr : ValidatorRow) (r : Spec.Row) (hm : RowMatches lr r)
    (previous_epoch : U64) :
    per_epoch_processing.single_pass_step.is_eligible lr previous_epoch ⦃ res =>
      absResult id res = Spec.validatorEligible previous_epoch.val r.validator ⦄ := by
  unfold per_epoch_processing.single_pass_step.is_eligible
  simp only [is_active_previous_epoch_eq lr r hm, bind_tc_ok, Spec.validatorEligible]
  by_cases ha : Spec.is_active_validator r.validator previous_epoch.val = true
  · simp [ha, absResult, Pure.pure, Except.pure]
  simp only [ha, Bool.false_eq_true, if_false, ← hm.slashed]
  by_cases hs : lr.slashed = true
  swap
  · simp [hs, absResult, Pure.pure, Except.pure]
  simp only [hs, if_true]
  apply exists_imp_spec
  obtain ⟨a, ha', hA⟩ := spec_imp_exists (safe_add_spec previous_epoch 1#u64)
  rw [ha']
  have hone : (1#u64 : U64).val = 1 := rfl
  simp only [hone] at hA
  cases a with
  | Err e =>
    obtain ⟨he, hov⟩ := hA.2 e rfl
    subst he
    simp [hov, Spec.uint64Add, Spec.UINT64_SIZE, core.result.Result.Insts.CoreOpsTry.branch,
      core.result.Result.Insts.CoreOpsTryTraitFromResidualResultInfallible.from_residual,
      core.convert.FromSame.from, absResult, absArithError]
    rfl
  | Ok v =>
    obtain ⟨hv, hfit⟩ := hA.1 v rfl
    simp [hv, hfit, Spec.uint64Add, Spec.UINT64_SIZE, core.result.Result.Insts.CoreOpsTry.branch,
      absResult, ← hm.withdrawable_epoch]
    rfl

/-- The `U8` mask test equals `rewardsHasFlag` on the same bits. -/
theorem mask_test_eq (x mask : U8) (y : UInt8) (flag : Nat) (hxy : x.val = y.toNat)
    (hmask : mask.val = 2 ^ flag) (hflag : flag < 8) :
    decide ((x &&& mask) = mask) = Spec.rewardsHasFlag y flag := by
  have hpow : 2 ^ flag < 256 := by
    have := Nat.pow_lt_pow_right (by decide : 1 < 2) hflag
    simpa using this
  have hlhs : (x &&& mask) = mask ↔ x.val &&& 2 ^ flag = 2 ^ flag := by
    rw [UScalar.eq_equiv, UScalar.val_and, hmask]
  have hrhs : (y &&& UInt8.ofNat (2 ^ flag)) = UInt8.ofNat (2 ^ flag) ↔
      y.toNat &&& 2 ^ flag = 2 ^ flag := by
    rw [← UInt8.toNat_inj, UInt8.toNat_and, UInt8.toNat_ofNat', Nat.mod_eq_of_lt hpow]
  unfold Spec.rewardsHasFlag
  rw [Bool.eq_iff_iff, decide_eq_true_iff, beq_iff_eq, hlhs, hrhs, hxy]

/-- `is_unslashed_participating` with mask `2 ^ flag` is `rewardsParticipating` for `flag`. -/
theorem is_unslashed_participating_eq (lr : ValidatorRow) (r : Spec.Row) (hm : RowMatches lr r)
    (previous_epoch : U64) (mask : U8) (flag : Nat) (hmask : mask.val = 2 ^ flag)
    (hflag : flag < 8) :
    per_epoch_processing.single_pass_step.is_unslashed_participating lr previous_epoch mask =
      ok (Spec.rewardsParticipating previous_epoch.val r flag) := by
  unfold per_epoch_processing.single_pass_step.is_unslashed_participating
  rw [is_active_previous_epoch_eq lr r hm]
  simp only [bind_tc_ok, Spec.rewardsParticipating, ← hm.slashed, lift,
    ← mask_test_eq lr.previous_epoch_participation mask r.previous_participation flag
      hm.previous_participation hmask hflag]
  cases Spec.is_active_validator r.validator previous_epoch.val <;> cases lr.slashed <;> simp

/-- The target test of the inactivity update is `rewardsParticipating` for the target flag. -/
theorem rowHitsTarget_eq (previous_epoch : Spec.Epoch) (r : Spec.Row) :
    Spec.rowHitsTarget previous_epoch r =
      Spec.rewardsParticipating previous_epoch r Spec.TIMELY_TARGET_FLAG_INDEX := rfl

/-- `inactivity_step` gives the inactivity score of `lhRowStep` and changes no other field. -/
theorem inactivity_step_equiv (p : Spec.Preset) (lr : ValidatorRow) (r : Spec.Row)
    (hm : RowMatches lr r) (eligible : Bool) (c : StepContext) (ctx : Spec.LhStepContext)
    (hc : ContextMatches p c ctx) :
    per_epoch_processing.single_pass_step.inactivity_step lr eligible c ⦃ res =>
      absResult (·.inactivity_score.val) res =
        (if eligible then
          Spec.inactivityScoreStep p (Spec.rowHitsTarget ctx.previous_epoch r) ctx.in_leak
            r.inactivity_score
        else pure r.inactivity_score)
      ∧ ∀ lr', res = .Ok lr' → lr' = { lr with inactivity_score := lr'.inactivity_score } ⦄ := by
  unfold per_epoch_processing.single_pass_step.inactivity_step
  simp only [hc.after_genesis, hc.inactivity_updates, if_true,
    is_unslashed_participating_eq lr r hm c.previous_epoch 2#u8 1 rfl (by decide), bind_tc_ok]
  apply exists_imp_spec
  obtain ⟨res, hres, h⟩ := spec_imp_exists (new_inactivity_score_equiv p lr.inactivity_score
    c.inactivity_score_bias c.inactivity_score_recovery_rate eligible
    (Spec.rewardsParticipating c.previous_epoch.val r 1) c.is_in_inactivity_leak hc.bias
    hc.recovery_rate)
  rw [hres]
  rw [hc.previous_epoch, hc.in_leak, hm.inactivity_score] at h
  cases res with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    simp only [rowHitsTarget_eq, Spec.TIMELY_TARGET_FLAG_INDEX, Pure.pure, Except.pure, ← h]
    simp [core.convert.FromSame.from, absResult]
  | Ok v =>
    refine ⟨_, rfl, ?_⟩
    simp only [rowHitsTarget_eq, Spec.TIMELY_TARGET_FLAG_INDEX, Pure.pure, Except.pure, ← h]
    simp [absResult]

/-- `rewards_step` gives the balance of `lhRowStep` and changes no other field. -/
theorem rewards_step_equiv (p : Spec.Preset) (lr : ValidatorRow) (r : Spec.Row)
    (hm : RowMatches lr r) (eligible : Bool) (base_reward : U64) (c : StepContext)
    (ctx : Spec.LhStepContext) (hc : ContextMatches p c ctx) :
    per_epoch_processing.single_pass_step.rewards_step lr eligible base_reward c ⦃ res =>
      absResult (·.balance.val) res =
        (if eligible then do
          let deltas ← Spec.rewardDeltas p base_reward.val r.validator.effective_balance
            r.inactivity_score
            (Spec.rewardsParticipating ctx.previous_epoch r Spec.TIMELY_SOURCE_FLAG_INDEX)
            (Spec.rewardsParticipating ctx.previous_epoch r Spec.TIMELY_TARGET_FLAG_INDEX)
            (Spec.rewardsParticipating ctx.previous_epoch r Spec.TIMELY_HEAD_FLAG_INDEX)
            ctx.in_leak ctx.source_increments ctx.target_increments ctx.head_increments
            ctx.active_increments
          Spec.rewardsCombined r.balance deltas
        else pure r.balance)
      ∧ ∀ lr', res = .Ok lr' → lr' = { lr with balance := lr'.balance } ⦄ := by
  unfold per_epoch_processing.single_pass_step.rewards_step
  simp only [hc.after_genesis, hc.rewards_and_penalties, if_true,
    is_unslashed_participating_eq lr r hm c.previous_epoch 1#u8 0 rfl (by decide),
    is_unslashed_participating_eq lr r hm c.previous_epoch 2#u8 1 rfl (by decide),
    is_unslashed_participating_eq lr r hm c.previous_epoch 4#u8 2 rfl (by decide), bind_tc_ok]
  apply exists_imp_spec
  obtain ⟨res, hres, h⟩ := spec_imp_exists (new_balance_after_rewards_equiv p lr.balance
    base_reward lr.effective_balance lr.inactivity_score c.source_increments c.target_increments
    c.head_increments c.active_increments c.inactivity_score_bias c.inactivity_penalty_quotient
    eligible (Spec.rewardsParticipating c.previous_epoch.val r 0)
    (Spec.rewardsParticipating c.previous_epoch.val r 1)
    (Spec.rewardsParticipating c.previous_epoch.val r 2) c.is_in_inactivity_leak hc.bias
    hc.penalty_quotient)
  rw [hres]
  rw [hc.previous_epoch, hc.in_leak, hc.source_increments, hc.target_increments,
    hc.head_increments, hc.active_increments, hm.balance, hm.effective_balance,
    hm.inactivity_score] at h
  simp only [Spec.TIMELY_SOURCE_FLAG_INDEX, Spec.TIMELY_TARGET_FLAG_INDEX,
    Spec.TIMELY_HEAD_FLAG_INDEX, Pure.pure, Except.pure, ← h]
  cases res with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    simp [core.convert.FromSame.from, absResult]
  | Ok v =>
    refine ⟨_, rfl, ?_⟩
    simp [absResult]

/-- The registry fields that `registry_step` builds from a row and the churn. -/
def lhRegistryFieldsOf (lr : ValidatorRow) (churn : ExitChurn) : LhRegistryFields :=
  { activation_eligibility_epoch := lr.activation_eligibility_epoch
    activation_epoch := lr.activation_epoch
    exit_epoch := lr.exit_epoch
    withdrawable_epoch := lr.withdrawable_epoch
    earliest_exit_epoch := churn.earliest_exit_epoch
    exit_balance_to_consume := churn.exit_balance_to_consume }

/-- The built registry fields are `registryFieldsOf` of the spec validator. -/
theorem absRegistry_lhRegistryFieldsOf (lr : ValidatorRow) (r : Spec.Row)
    (hm : RowMatches lr r) (churn : ExitChurn) :
    absRegistry (lhRegistryFieldsOf lr churn) = Spec.registryFieldsOf r.validator
      (churn.earliest_exit_epoch.val, churn.exit_balance_to_consume.val) := by
  simp [absRegistry, lhRegistryFieldsOf, Spec.registryFieldsOf, hm.activation_eligibility_epoch,
    hm.activation_epoch, hm.exit_epoch, hm.withdrawable_epoch]

/-- `registry_step` gives the registry fields of `lhRowStep` and changes only the four
validator epochs of the row. -/
theorem registry_step_equiv (p : Spec.Preset) (lr : ValidatorRow) (r : Spec.Row)
    (hm : RowMatches lr r) (churn : ExitChurn) (c : StepContext) (ctx : Spec.LhStepContext)
    (hc : ContextMatches p c ctx) (consts : LhRegistryConstants)
    (hconsts : ConstantsMatch p consts) :
    per_epoch_processing.single_pass_step.registry_step lr churn c consts ⦃ res =>
      absResult (fun x => absRegistry (lhRegistryFieldsOf x.1 x.2)) res =
        Spec.registryStepIndependent p ctx.total_active_balance ctx.current_epoch
          ctx.finalized_epoch r.validator.effective_balance
          (Spec.registryFieldsOf r.validator
            (churn.earliest_exit_epoch.val, churn.exit_balance_to_consume.val))
      ∧ ∀ lr' ch', res = .Ok (lr', ch') → lr' =
        { lr with
          activation_eligibility_epoch := lr'.activation_eligibility_epoch
          activation_epoch := lr'.activation_epoch
          exit_epoch := lr'.exit_epoch
          withdrawable_epoch := lr'.withdrawable_epoch } ⦄ := by
  unfold per_epoch_processing.single_pass_step.registry_step
  simp only [hc.registry_updates, if_true]
  apply exists_imp_spec
  obtain ⟨res, hres, h⟩ := spec_imp_exists (registry_update_equiv p (lhRegistryFieldsOf lr churn)
    lr.effective_balance c.current_epoch c.finalized_epoch c.total_active_balance consts hconsts)
  simp only [lhRegistryFieldsOf] at hres
  rw [hres]
  rw [absRegistry_lhRegistryFieldsOf lr r hm, hm.effective_balance, hc.current_epoch,
    hc.finalized_epoch, hc.total_active_balance] at h
  rw [← h]
  cases res with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    simp [core.convert.FromSame.from, absResult]
  | Ok v =>
    refine ⟨_, rfl, ?_⟩
    simp [absResult, absRegistry, lhRegistryFieldsOf]

/-- `slashings_step` gives the balance of `slashingBalanceStep` and changes no other field. -/
theorem slashings_step_equiv (p : Spec.Preset) (lr : ValidatorRow) (r : Spec.Row)
    (hm : RowMatches lr r) (c : StepContext) (ctx : Spec.LhStepContext)
    (hc : ContextMatches p c ctx) :
    per_epoch_processing.single_pass_step.slashings_step lr c ⦃ res =>
      absResult (·.balance.val) res =
        Spec.slashingBalanceStep p ctx.slashings_target ctx.penalty_per_increment r.validator
          r.balance
      ∧ ∀ lr', res = .Ok lr' → lr' = { lr with balance := lr'.balance } ⦄ := by
  unfold per_epoch_processing.single_pass_step.slashings_step
  simp only [hc.slashings, if_true]
  apply exists_imp_spec
  obtain ⟨res, hres, h⟩ := spec_imp_exists (new_balance_after_slashing_equiv p r.validator
    lr.balance lr.withdrawable_epoch lr.effective_balance c.target_withdrawable_epoch
    c.adjusted_total_slashing_balance c.penalty_per_effective_balance_increment
    c.total_active_balance c.effective_balance_increment lr.slashed hm.slashed.symm
    hm.withdrawable_epoch hm.effective_balance hc.effective_balance_increment)
  rw [hres]
  rw [hc.slashings_target, hc.penalty_per_increment, hm.balance] at h
  rw [← h]
  cases res with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    simp [core.convert.FromSame.from, absResult]
  | Ok v =>
    refine ⟨_, rfl, ?_⟩
    simp [absResult]

/-- `lhRowStep` as a plain chain of binds, once the eligibility is known. -/
theorem lhRowStep_eq (p : Spec.Preset) (ctx : Spec.LhStepContext) (base_reward : Spec.Gwei)
    (churn : Spec.Epoch × Spec.Gwei) (r : Spec.Row) (eligible : Bool)
    (h : Spec.validatorEligible ctx.previous_epoch r.validator = .ok eligible) :
    Spec.lhRowStep p ctx base_reward churn r =
      ((if eligible then
          Spec.inactivityScoreStep p (Spec.rowHitsTarget ctx.previous_epoch r) ctx.in_leak
            r.inactivity_score
        else pure r.inactivity_score) >>= fun s =>
      (if eligible then
          Spec.rewardDeltas p base_reward r.validator.effective_balance s
            (Spec.rewardsParticipating ctx.previous_epoch { r with inactivity_score := s }
              Spec.TIMELY_SOURCE_FLAG_INDEX)
            (Spec.rewardsParticipating ctx.previous_epoch { r with inactivity_score := s }
              Spec.TIMELY_TARGET_FLAG_INDEX)
            (Spec.rewardsParticipating ctx.previous_epoch { r with inactivity_score := s }
              Spec.TIMELY_HEAD_FLAG_INDEX)
            ctx.in_leak ctx.source_increments ctx.target_increments ctx.head_increments
            ctx.active_increments >>= fun deltas => Spec.rewardsCombined r.balance deltas
        else pure r.balance) >>= fun balance =>
      Spec.registryStepIndependent p ctx.total_active_balance ctx.current_epoch
        ctx.finalized_epoch r.validator.effective_balance (Spec.registryFieldsOf r.validator churn)
        >>= fun f =>
      Spec.slashingBalanceStep p ctx.slashings_target ctx.penalty_per_increment
        (r.validator.withRegistryFields f) balance >>= fun balance' =>
      pure ((f.earliest_exit_epoch, f.exit_balance_to_consume),
        { r with
          inactivity_score := s
          balance := balance'
          validator := r.validator.withRegistryFields f })) := by
  cases eligible <;> simp [Spec.lhRowStep, h, except_ok_bind]

/-- `deposit_step` adds the deposit to the balance and changes no other field. -/
theorem deposit_step_equiv (p : Spec.Preset) (lr : ValidatorRow) (deposit : U64)
    (c : StepContext) (ctx : Spec.LhStepContext) (hc : ContextMatches p c ctx) :
    per_epoch_processing.single_pass_step.deposit_step lr deposit c ⦃ res =>
      absResult (·.balance.val) res = Spec.uint64Add lr.balance.val deposit.val
      ∧ ∀ lr', res = .Ok lr' → lr' = { lr with balance := lr'.balance } ⦄ := by
  unfold per_epoch_processing.single_pass_step.deposit_step
  simp only [hc.pending_deposits, if_true]
  apply exists_imp_spec
  obtain ⟨a, ha, hA⟩ := spec_imp_exists (safe_add_spec lr.balance deposit)
  rw [ha]
  cases a with
  | Err e =>
    obtain ⟨he, hov⟩ := hA.2 e rfl
    subst he
    refine ⟨_, rfl, ?_⟩
    simp [hov, Spec.uint64Add, Spec.UINT64_SIZE, core.convert.FromSame.from, absResult,
      absArithError]
    rfl
  | Ok v =>
    obtain ⟨hv, hfit⟩ := hA.1 v rfl
    refine ⟨_, rfl, ?_⟩
    simp [hv, hfit, Spec.uint64Add, Spec.UINT64_SIZE, absResult]
    rfl

/-- `get_max_effective_balance` does not read the registry fields. -/
theorem get_max_effective_balance_withRegistryFields (p : Spec.Preset) (v : Spec.Validator)
    (f : Spec.RegistryFields) :
    Spec.get_max_effective_balance p (v.withRegistryFields f) =
      Spec.get_max_effective_balance p v := rfl

/-- `effective_balance_step` gives the effective balance of `lhRowStepFull` and changes no
other field. -/
theorem effective_balance_step_equiv (p : Spec.Preset) (lr : ValidatorRow) (r : Spec.Row)
    (hm : RowMatches lr r) (i : RowInputs) (inputs : Spec.LhRowInputs)
    (hi : InputsMatches p i inputs r) (c : StepContext) (ctx : Spec.LhStepContext)
    (hc : ContextMatches p c ctx) (downward upward : Spec.Uint64)
    (hdownward : c.downward_threshold.val = downward) (hupward : c.upward_threshold.val = upward) :
    per_epoch_processing.single_pass_step.effective_balance_step lr i c ⦃ res =>
      absResult (·.effective_balance.val) res =
        (if inputs.in_consolidation then pure r.validator.effective_balance
        else Spec.newEffectiveBalance p downward upward r.validator r.balance)
      ∧ ∀ lr', res = .Ok lr' → lr' = { lr with effective_balance := lr'.effective_balance } ⦄ := by
  unfold per_epoch_processing.single_pass_step.effective_balance_step
  simp only [hc.effective_balance_updates, if_true, hi.in_consolidation]
  by_cases hin : inputs.in_consolidation = true
  · simp [hin, absResult, hm.effective_balance, Pure.pure, Except.pure]
  simp only [hin, Bool.false_eq_true, if_false]
  apply exists_imp_spec
  obtain ⟨res, hres, h⟩ := spec_imp_exists (new_effective_balance_equiv p r.validator lr.balance
    lr.effective_balance i.effective_balance_limit c.downward_threshold c.upward_threshold
    c.effective_balance_increment hm.effective_balance hi.effective_balance_limit
    hc.effective_balance_increment)
  rw [hres]
  rw [hdownward, hupward, hm.balance] at h
  rw [← h]
  cases res with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    simp [core.convert.FromSame.from, absResult]
  | Ok v =>
    refine ⟨_, rfl, ?_⟩
    simp [absResult]

/-- `lhRowStepFull` as a plain chain of binds, once the eligibility is known. -/
theorem lhRowStepFull_eq (p : Spec.Preset) (ctx : Spec.LhStepContext)
    (downward upward : Spec.Uint64) (inputs : Spec.LhRowInputs)
    (churn : Spec.Epoch × Spec.Gwei) (r : Spec.Row) (eligible : Bool)
    (h : Spec.validatorEligible ctx.previous_epoch r.validator = .ok eligible) :
    Spec.lhRowStepFull p ctx downward upward inputs churn r =
      ((if eligible then
          Spec.inactivityScoreStep p (Spec.rowHitsTarget ctx.previous_epoch r) ctx.in_leak
            r.inactivity_score
        else pure r.inactivity_score) >>= fun s =>
      (if eligible then
          Spec.rewardDeltas p inputs.base_reward r.validator.effective_balance s
            (Spec.rewardsParticipating ctx.previous_epoch { r with inactivity_score := s }
              Spec.TIMELY_SOURCE_FLAG_INDEX)
            (Spec.rewardsParticipating ctx.previous_epoch { r with inactivity_score := s }
              Spec.TIMELY_TARGET_FLAG_INDEX)
            (Spec.rewardsParticipating ctx.previous_epoch { r with inactivity_score := s }
              Spec.TIMELY_HEAD_FLAG_INDEX)
            ctx.in_leak ctx.source_increments ctx.target_increments ctx.head_increments
            ctx.active_increments >>= fun deltas => Spec.rewardsCombined r.balance deltas
        else pure r.balance) >>= fun balance =>
      Spec.registryStepIndependent p ctx.total_active_balance ctx.current_epoch
        ctx.finalized_epoch r.validator.effective_balance (Spec.registryFieldsOf r.validator churn)
        >>= fun f =>
      Spec.slashingBalanceStep p ctx.slashings_target ctx.penalty_per_increment
        (r.validator.withRegistryFields f) balance >>= fun balance' =>
      Spec.uint64Add balance' inputs.deposit >>= fun balance'' =>
      (if inputs.in_consolidation then pure (r.validator.withRegistryFields f).effective_balance
        else Spec.newEffectiveBalance p downward upward (r.validator.withRegistryFields f)
          balance'') >>= fun effective_balance =>
      pure ((f.earliest_exit_epoch, f.exit_balance_to_consume),
        { r with
          inactivity_score := s
          balance := balance''
          validator := { r.validator.withRegistryFields f with effective_balance } })) := by
  rw [Spec.lhRowStepFull, lhRowStep_eq p ctx _ _ r eligible h]
  cases eligible <;> cases inputs.in_consolidation <;> simp [Spec.Validator.withRegistryFields]

/-- Lighthouse's `single_pass_step` equals `lhRowStepFull` for one validator. -/
theorem single_pass_step_equiv (p : Spec.Preset) (lr : ValidatorRow) (r : Spec.Row)
    (hm : RowMatches lr r) (i : RowInputs) (inputs : Spec.LhRowInputs)
    (hi : InputsMatches p i inputs r) (churn : ExitChurn) (c : StepContext)
    (ctx : Spec.LhStepContext) (hc : ContextMatches p c ctx) (downward upward : Spec.Uint64)
    (hdownward : c.downward_threshold.val = downward) (hupward : c.upward_threshold.val = upward)
    (consts : LhRegistryConstants) (hconsts : ConstantsMatch p consts) :
    per_epoch_processing.single_pass_step.single_pass_step lr i churn c consts ⦃ res =>
      absStep r res = Spec.lhRowStepFull p ctx downward upward inputs
        (churn.earliest_exit_epoch.val, churn.exit_balance_to_consume.val) r ⦄ := by
  unfold per_epoch_processing.single_pass_step.single_pass_step
  apply exists_imp_spec
  obtain ⟨e0, he0, h0⟩ := spec_imp_exists (is_eligible_equiv lr r hm c.previous_epoch)
  rw [he0]
  rw [hc.previous_epoch] at h0
  cases e0 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    simp only [Spec.lhRowStepFull, Spec.lhRowStep, ← h0]
    simp [core.convert.FromSame.from, absResult, absStep, except_error_bind]
  | Ok b =>
  simp only [absResult, id] at h0
  rw [lhRowStepFull_eq p ctx _ _ _ _ r b h0.symm]
  simp only [core.result.Result.Insts.CoreOpsTry.branch, bind_tc_ok]
  obtain ⟨e1, he1, h1, hf1⟩ := spec_imp_exists (inactivity_step_equiv p lr r hm b c ctx hc)
  rw [he1]
  simp only [absResult] at h1
  cases e1 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    rw [← h1]
    simp [core.convert.FromSame.from, absStep, except_error_bind]
  | Ok lr1 =>
  obtain ⟨s1, rfl⟩ : ∃ s, lr1 = { lr with inactivity_score := s } := ⟨_, hf1 lr1 rfl⟩
  rw [← h1]
  simp only [bind_tc_ok, except_ok_bind]
  have hm1 : RowMatches { lr with inactivity_score := s1 } { r with inactivity_score := s1.val } :=
    ⟨hm.1, rfl, hm.3, hm.4, hm.5, hm.6, hm.7, hm.8, hm.9⟩
  obtain ⟨e2, he2, h2, hf2⟩ := spec_imp_exists
    (rewards_step_equiv p _ _ hm1 b i.base_reward c ctx hc)
  rw [he2]
  simp only [absResult, hi.base_reward] at h2
  cases e2 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    rw [← h2]
    simp [core.convert.FromSame.from, absStep, except_error_bind]
  | Ok lr2 =>
  obtain ⟨s2, rfl⟩ : ∃ s, lr2 = { lr with inactivity_score := s1, balance := s } :=
    ⟨_, hf2 lr2 rfl⟩
  rw [← h2]
  simp only [bind_tc_ok, except_ok_bind]
  have hm2 : RowMatches { lr with inactivity_score := s1, balance := s2 }
      { r with inactivity_score := s1.val, balance := s2.val } :=
    ⟨rfl, rfl, hm.3, hm.4, hm.5, hm.6, hm.7, hm.8, hm.9⟩
  obtain ⟨e3, he3, h3, hf3⟩ := spec_imp_exists
    (registry_step_equiv p _ _ hm2 churn c ctx hc consts hconsts)
  rw [he3]
  simp only [absResult] at h3
  cases e3 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    rw [← h3]
    simp [core.convert.FromSame.from, absStep, except_error_bind]
  | Ok x3 =>
  obtain ⟨lr3, ch3⟩ := x3
  obtain ⟨a3, b3, c3, d3, rfl⟩ : ∃ a b c d, lr3 = { lr with
      inactivity_score := s1, balance := s2, activation_eligibility_epoch := a,
      activation_epoch := b, exit_epoch := c, withdrawable_epoch := d } :=
    ⟨_, _, _, _, hf3 lr3 ch3 rfl⟩
  rw [← h3]
  simp only [bind_tc_ok, except_ok_bind]
  generalize hf : absRegistry (lhRegistryFieldsOf _ ch3) = f
  have hm3 : RowMatches
      { lr with
        inactivity_score := s1, balance := s2, activation_eligibility_epoch := a3,
        activation_epoch := b3, exit_epoch := c3, withdrawable_epoch := d3 }
      { r with
        inactivity_score := s1.val, balance := s2.val,
        validator := r.validator.withRegistryFields f } := by
    subst hf
    exact ⟨rfl, rfl, hm.3, hm.4, rfl, rfl, rfl, rfl, hm.9⟩
  obtain ⟨e4, he4, h4, hf4⟩ := spec_imp_exists (slashings_step_equiv p _ _ hm3 c ctx hc)
  simp only [uncurry_apply_pair]
  rw [he4]
  simp only [absResult] at h4
  cases e4 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    rw [← h4]
    simp [core.convert.FromSame.from, absStep, except_error_bind]
  | Ok lr4 =>
  obtain ⟨s4, rfl⟩ : ∃ s, lr4 = { lr with
      inactivity_score := s1, balance := s, activation_eligibility_epoch := a3,
      activation_epoch := b3, exit_epoch := c3, withdrawable_epoch := d3 } :=
    ⟨_, hf4 lr4 rfl⟩
  rw [← h4]
  simp only [bind_tc_ok, except_ok_bind]
  obtain ⟨e5, he5, h5, hf5⟩ := spec_imp_exists (deposit_step_equiv p _ i.deposit c ctx hc)
  rw [he5]
  simp only [absResult, hi.deposit] at h5
  cases e5 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    rw [← h5]
    simp [core.convert.FromSame.from, absStep, except_error_bind]
  | Ok lr5 =>
  obtain ⟨s5, rfl⟩ : ∃ s, lr5 = { lr with
      inactivity_score := s1, balance := s, activation_eligibility_epoch := a3,
      activation_epoch := b3, exit_epoch := c3, withdrawable_epoch := d3 } :=
    ⟨_, hf5 lr5 rfl⟩
  rw [← h5]
  simp only [bind_tc_ok, except_ok_bind]
  have hm5 : RowMatches
      { lr with
        inactivity_score := s1, balance := s5, activation_eligibility_epoch := a3,
        activation_epoch := b3, exit_epoch := c3, withdrawable_epoch := d3 }
      { r with
        inactivity_score := s1.val, balance := s5.val,
        validator := r.validator.withRegistryFields f } := ⟨rfl, hm3.2, hm3.3, hm3.4, hm3.5,
          hm3.6, hm3.7, hm3.8, hm3.9⟩
  have hi5 : InputsMatches p i inputs
      { r with
        inactivity_score := s1.val, balance := s5.val,
        validator := r.validator.withRegistryFields f } :=
    ⟨hi.1, hi.2, hi.3, hi.4⟩
  obtain ⟨e6, he6, h6, hf6⟩ := spec_imp_exists
    (effective_balance_step_equiv p _ _ hm5 i inputs hi5 c ctx hc downward upward hdownward hupward)
  rw [he6]
  simp only [absResult] at h6
  cases e6 with
  | Err e =>
    refine ⟨_, rfl, ?_⟩
    rw [← h6]
    simp [core.convert.FromSame.from, absStep]
  | Ok lr6 =>
  obtain ⟨s6, rfl⟩ : ∃ s, lr6 = { lr with
      inactivity_score := s1, balance := s5, effective_balance := s,
      activation_eligibility_epoch := a3, activation_epoch := b3, exit_epoch := c3,
      withdrawable_epoch := d3 } :=
    ⟨_, hf6 lr6 rfl⟩
  rw [← h6]
  subst hf
  simp [absStep, absRegistry, lhRegistryFieldsOf, Spec.Validator.withRegistryFields]

end EpochProofs
