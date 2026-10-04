import EpochProofs.Sanity.LighthouseRow

/-!
# The Lighthouse row step equals the spec row step

`lhRowStep_eq_singlePassStep` shows that `lhRowStep` and `singlePassStep` give the same result
for one row. The theorem needs the base reward, no saturation in the rewards of this row, and
the conditions of `registryStepIndependent_eq_exclusive`.

`lhRowStep_eq_singlePassStep_of_combined` takes the rewards step as a hypothesis.
`RewardsBound.lean` uses it with a weaker rewards condition.
-/

namespace EpochProofs.Spec

/-- The inactivity step and the rewards step use the same eligibility test. -/
theorem validatorEligible_eq_rewardsEligible (previous_epoch : Epoch) (v : Validator) :
    validatorEligible previous_epoch v = rewardsEligible previous_epoch v := rfl

/-- The inactivity step and the rewards step use the same target test. -/
theorem rowHitsTarget_eq_rewardsParticipating (previous_epoch : Epoch) (r : Row) :
    rowHitsTarget previous_epoch r =
      rewardsParticipating previous_epoch r TIMELY_TARGET_FLAG_INDEX := rfl

/-- Participation does not read the inactivity score. -/
theorem rewardsParticipating_withScore (previous_epoch : Epoch) (r : Row) (score : Uint64)
    (flag_index : Nat) :
    rewardsParticipating previous_epoch { r with inactivity_score := score } flag_index =
      rewardsParticipating previous_epoch r flag_index := rfl

/-- Four zero deltas keep a balance that fits in a `u64`. -/
theorem rewardsSequential_zeros (balance : Gwei) (h : balance < 2 ^ 64) :
    rewardsSequential balance [(0, 0), (0, 0), (0, 0), (0, 0)] = .ok balance := by
  rw [rewardsSequential_eq_combined balance _ (by simp) (by simpa using h)]
  simp

/-- The Lighthouse row step equals the spec row step, with the churn as the accumulator. This
form takes the rewards step as a hypothesis: `rewardsCombined` equals `rewardsSequential` on the
deltas of this row.

Hypotheses:
- `hprev`, `hleak`, `hsource`, `htarget`, `hhead`, `hactive`: the rewards context has the same
  epoch-wide values as `ctx`. `rctx.total_active_balance` is free: only `hbase` reads it.
- `hbase`: for an eligible row, `base_reward` is the spec base reward.
- `hbalance`: for a row that is not eligible, the balance fits in a `u64`. The spec checks this
  when it adds four zero deltas.
- `hcombined`: for the row after the inactivity step and the deltas that the spec computes,
  `rewardsCombined` equals `rewardsSequential`.
- `hbalances`, `hfinalized`, `hcurrent`, `hactivation`, `hexit`: the conditions of
  `registryStepIndependent_eq_exclusive`. `hexit` is on the validator of this row. -/
theorem lhRowStep_eq_singlePassStep_of_combined (p : Preset) (ctx : LhStepContext)
    (rctx : RewardsContext) (r : Row) (churn : Epoch × Gwei) (base_reward : Gwei)
    (activation_epoch : Epoch)
    (hprev : rctx.previous_epoch = ctx.previous_epoch)
    (hleak : rctx.in_leak = ctx.in_leak)
    (hsource : rctx.source_increments = ctx.source_increments)
    (htarget : rctx.target_increments = ctx.target_increments)
    (hhead : rctx.head_increments = ctx.head_increments)
    (hactive : rctx.active_increments = ctx.active_increments)
    (hbase : rewardsEligible ctx.previous_epoch r.validator = .ok true →
      rewardsBaseReward p rctx.total_active_balance r.validator = .ok base_reward)
    (hbalance : rewardsEligible ctx.previous_epoch r.validator = .ok false →
      r.balance < 2 ^ 64)
    (hcombined : ∀ r' deltas,
      inactivityRowStep p ctx.previous_epoch ctx.in_leak () r = .ok ((), r') →
      rewardsRowDeltas p rctx r' = .ok deltas →
      rewardsCombined r.balance deltas = rewardsSequential r.balance deltas)
    (hbalances : p.EJECTION_BALANCE < p.MIN_ACTIVATION_BALANCE)
    (hfinalized : ctx.finalized_epoch ≤ ctx.current_epoch)
    (hcurrent : ctx.current_epoch + 1 < UINT64_SIZE)
    (hactivation : compute_activation_exit_epoch p ctx.current_epoch = .ok activation_epoch)
    (hexit : r.validator.exit_epoch ≤ FAR_FUTURE_EPOCH) :
    lhRowStep p ctx base_reward churn r =
      (fun x => (x.1.1.2, x.2)) <$> singlePassStep p ctx.total_active_balance ctx.previous_epoch
        ctx.in_leak rctx ctx.current_epoch ctx.finalized_epoch activation_epoch
        ctx.slashings_target ctx.penalty_per_increment ((((), ()), churn), ()) r := by
  obtain ⟨rprev, rleak, rtotal, rsource, rtarget, rhead, ractive⟩ := rctx
  dsimp only at hprev hleak hsource htarget hhead hactive hbase hcombined
  subst hprev hleak hsource htarget hhead hactive
  have hreg : ∀ (v : Validator) (c : Epoch × Gwei), v.exit_epoch ≤ FAR_FUTURE_EPOCH →
      registryStepIndependent p ctx.total_active_balance ctx.current_epoch ctx.finalized_epoch
        v.effective_balance (registryFieldsOf v c) =
      registryStepExclusive p ctx.total_active_balance ctx.current_epoch ctx.finalized_epoch
        activation_epoch v.effective_balance (registryFieldsOf v c) := fun v c hv =>
    registryStepIndependent_eq_exclusive p _ _ _ _ _ _ hbalances hfinalized hv hcurrent
      hactivation
  unfold lhRowStep singlePassStep
  simp only [bothSteps, inactivityRowStep, rewardsRowStep, registryRowStep, slashingsRowStep,
    rewardsRowDeltas, ← validatorEligible_eq_rewardsEligible]
  cases helig : validatorEligible ctx.previous_epoch r.validator with
  | error e => rfl
  | ok eligible =>
  cases eligible with
  | false =>
    simp only [helig, bind, Except.bind, pure, Except.pure, if_false, Bool.false_eq_true,
      rewardsSequential_zeros r.balance (hbalance helig), hreg r.validator churn hexit]
    cases registryStepExclusive p ctx.total_active_balance ctx.current_epoch ctx.finalized_epoch
        activation_epoch r.validator.effective_balance (registryFieldsOf r.validator churn) with
    | error e => rfl
    | ok f =>
      dsimp only
      cases slashingBalanceStep p ctx.slashings_target ctx.penalty_per_increment
        (r.validator.withRegistryFields f) r.balance <;> rfl
  | true =>
    have hb := hbase helig
    simp only [bind, Except.bind, pure, Except.pure, if_true]
    cases hs : inactivityScoreStep p (rowHitsTarget ctx.previous_epoch r) ctx.in_leak
        r.inactivity_score with
    | error e => rfl
    | ok s =>
    simp only [rewardsParticipating_withScore, helig, hb, if_true]
    cases hd : rewardDeltas p base_reward r.validator.effective_balance s
        (rewardsParticipating ctx.previous_epoch r TIMELY_SOURCE_FLAG_INDEX)
        (rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX)
        (rewardsParticipating ctx.previous_epoch r TIMELY_HEAD_FLAG_INDEX)
        ctx.in_leak ctx.source_increments ctx.target_increments ctx.head_increments
        ctx.active_increments with
    | error e => rfl
    | ok d =>
    have hc := hcombined { r with inactivity_score := s } d
      (by simp [inactivityRowStep, helig, hs, bind, Except.bind, pure, Except.pure])
      (by simp [rewardsRowDeltas, ← validatorEligible_eq_rewardsEligible, helig, hb,
        rewardsParticipating_withScore, hd, bind, Except.bind])
    dsimp only
    rw [hc, hreg r.validator churn hexit]
    cases rewardsSequential r.balance d with
    | error e => rfl
    | ok b =>
    dsimp only
    cases registryStepExclusive p ctx.total_active_balance ctx.current_epoch ctx.finalized_epoch
        activation_epoch r.validator.effective_balance (registryFieldsOf r.validator churn) with
    | error e => rfl
    | ok f =>
      dsimp only
      cases slashingBalanceStep p ctx.slashings_target ctx.penalty_per_increment
        (r.validator.withRegistryFields f) b <;> rfl

/-- The Lighthouse row step equals the spec row step, with the churn as the accumulator.

Hypotheses:
- `hprev`, `hleak`, `hsource`, `htarget`, `hhead`, `hactive`: the rewards context has the same
  epoch-wide values as `ctx`. `rctx.total_active_balance` is free: only `hbase` reads it.
- `hbase`: for an eligible row, `base_reward` is the spec base reward.
- `hrewards`: no saturation in the rewards of this row. For the row after the inactivity step,
  the deltas that the spec computes have penalties at most the balance, and the balance plus
  the rewards fits in a `u64`. For a row that is not eligible, this says the balance fits in a
  `u64`.
- `hbalances`, `hfinalized`, `hcurrent`, `hactivation`, `hexit`: the conditions of
  `registryStepIndependent_eq_exclusive`. `hexit` is on the validator of this row. -/
theorem lhRowStep_eq_singlePassStep (p : Preset) (ctx : LhStepContext) (rctx : RewardsContext)
    (r : Row) (churn : Epoch × Gwei) (base_reward : Gwei) (activation_epoch : Epoch)
    (hprev : rctx.previous_epoch = ctx.previous_epoch)
    (hleak : rctx.in_leak = ctx.in_leak)
    (hsource : rctx.source_increments = ctx.source_increments)
    (htarget : rctx.target_increments = ctx.target_increments)
    (hhead : rctx.head_increments = ctx.head_increments)
    (hactive : rctx.active_increments = ctx.active_increments)
    (hbase : rewardsEligible ctx.previous_epoch r.validator = .ok true →
      rewardsBaseReward p rctx.total_active_balance r.validator = .ok base_reward)
    (hrewards : ∀ r' deltas,
      inactivityRowStep p ctx.previous_epoch ctx.in_leak () r = .ok ((), r') →
      rewardsRowDeltas p rctx r' = .ok deltas →
      (deltas.map (·.2)).sum ≤ r.balance ∧ r.balance + (deltas.map (·.1)).sum < 2 ^ 64)
    (hbalances : p.EJECTION_BALANCE < p.MIN_ACTIVATION_BALANCE)
    (hfinalized : ctx.finalized_epoch ≤ ctx.current_epoch)
    (hcurrent : ctx.current_epoch + 1 < UINT64_SIZE)
    (hactivation : compute_activation_exit_epoch p ctx.current_epoch = .ok activation_epoch)
    (hexit : r.validator.exit_epoch ≤ FAR_FUTURE_EPOCH) :
    lhRowStep p ctx base_reward churn r =
      (fun x => (x.1.1.2, x.2)) <$> singlePassStep p ctx.total_active_balance ctx.previous_epoch
        ctx.in_leak rctx ctx.current_epoch ctx.finalized_epoch activation_epoch
        ctx.slashings_target ctx.penalty_per_increment ((((), ()), churn), ()) r := by
  refine lhRowStep_eq_singlePassStep_of_combined p ctx rctx r churn base_reward activation_epoch
    hprev hleak hsource htarget hhead hactive hbase ?_ ?_ hbalances hfinalized hcurrent
    hactivation hexit
  · intro helig
    obtain ⟨rprev, rleak, rtotal, rsource, rtarget, rhead, ractive⟩ := rctx
    dsimp only at hprev hrewards
    subst hprev
    have hz := hrewards r [(0, 0), (0, 0), (0, 0), (0, 0)]
      (by simp [inactivityRowStep, validatorEligible_eq_rewardsEligible, helig, bind,
        Except.bind, pure, Except.pure])
      (by simp [rewardsRowDeltas, helig, bind, Except.bind, pure, Except.pure])
    simpa using hz.2
  · intro r' deltas hi hd
    have hc := hrewards r' deltas hi hd
    exact rewardsCombined_eq_sequential _ _ hc.1 hc.2

end EpochProofs.Spec
