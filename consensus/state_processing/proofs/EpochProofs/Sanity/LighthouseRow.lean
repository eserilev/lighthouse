import EpochProofs.Sanity.RowsSinglePass

/-!
# The Lighthouse row step

`lhRowStep` runs the four steps for one validator in the Lighthouse order, with the functions
that the Lighthouse code is proved equal to. Rewards use `rewardsCombined`, and the registry
update uses `registryStepIndependent`. `single_pass_step` in `single_pass_step.rs` equals this
step.
-/

namespace EpochProofs.Spec

/-- The values that are the same for all validators in this epoch. -/
structure LhStepContext where
  current_epoch : Epoch
  previous_epoch : Epoch
  finalized_epoch : Epoch
  in_leak : Bool
  source_increments : Uint64
  target_increments : Uint64
  head_increments : Uint64
  active_increments : Uint64
  total_active_balance : Gwei
  slashings_target : Uint64
  penalty_per_increment : Uint64

/-- Inactivity update, rewards and penalties, registry update and slashings penalty for one
row, in the Lighthouse order. `base_reward` is only read if the row is eligible. -/
def lhRowStep (p : Preset) (ctx : LhStepContext) (base_reward : Gwei) (churn : Epoch × Gwei)
    (r : Row) : SpecM ((Epoch × Gwei) × Row) := do
  let eligible ← validatorEligible ctx.previous_epoch r.validator
  let inactivity_score ←
    if eligible then
      inactivityScoreStep p (rowHitsTarget ctx.previous_epoch r) ctx.in_leak r.inactivity_score
    else pure r.inactivity_score
  let r := { r with inactivity_score }
  let balance ←
    if eligible then do
      let deltas ← rewardDeltas p base_reward r.validator.effective_balance r.inactivity_score
        (rewardsParticipating ctx.previous_epoch r TIMELY_SOURCE_FLAG_INDEX)
        (rewardsParticipating ctx.previous_epoch r TIMELY_TARGET_FLAG_INDEX)
        (rewardsParticipating ctx.previous_epoch r TIMELY_HEAD_FLAG_INDEX)
        ctx.in_leak ctx.source_increments ctx.target_increments ctx.head_increments
        ctx.active_increments
      rewardsCombined r.balance deltas
    else pure r.balance
  let r := { r with balance }
  let f ← registryStepIndependent p ctx.total_active_balance ctx.current_epoch
    ctx.finalized_epoch r.validator.effective_balance (registryFieldsOf r.validator churn)
  let r := { r with validator := r.validator.withRegistryFields f }
  let balance ← slashingBalanceStep p ctx.slashings_target ctx.penalty_per_increment r.validator
    r.balance
  pure ((f.earliest_exit_epoch, f.exit_balance_to_consume), { r with balance })

end EpochProofs.Spec
