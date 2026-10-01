//! EIP-8198 changes to state processing.
//!
//! Rewards, penalties and churn scale with the slot duration, so that they stay the same per unit
//! of wall-clock time. Payload timestamps follow the slot duration schedule.

use integer_sqrt::IntegerSquareRoot;
use safe_arith::{ArithError, SafeArith};
use types::features::eip8198::{PreviousEpochBaseRewards, scale_by_slot_duration};
use types::features::{Active, Eip8198};
use types::{BeaconState, ChainSpec, Epoch, EpochCacheError, EthSpec, ForkName, Slot};

use crate::common::altair::{self, BaseRewardPerIncrement};
use crate::per_epoch_processing::errors::EpochProcessingError as Error;
use crate::per_epoch_processing::single_pass::StateContext;

#[cfg(test)]
mod tests;

/// Spec: `get_base_reward_per_increment(state, epoch)`.
pub fn get_base_reward_per_increment(
    total_active_balance: u64,
    epoch: Epoch,
    spec: &ChainSpec,
    _active: Active<Eip8198>,
) -> Result<u64, ArithError> {
    spec.effective_balance_increment
        .safe_mul(spec.base_reward_factor)?
        .safe_mul(spec.get_slot_duration_ms(epoch))?
        .safe_div(spec.get_slot_duration_ms(Epoch::new(0)))?
        .safe_div(total_active_balance.integer_sqrt())
}

/// The base rewards of the epoch before `epoch`, for the epoch cache of `epoch`.
pub fn previous_epoch_base_rewards(
    total_active_balance: u64,
    epoch: Epoch,
    max_effective_balance_eth: u64,
    spec: &ChainSpec,
    _active: Active<Eip8198>,
) -> Result<PreviousEpochBaseRewards, EpochCacheError> {
    let previous_epoch = epoch.saturating_sub(1u64);
    let base_reward_per_increment =
        BaseRewardPerIncrement::new(total_active_balance, previous_epoch, spec)?;
    let base_rewards = (0..=max_effective_balance_eth)
        .map(|effective_balance_eth| {
            let effective_balance =
                effective_balance_eth.safe_mul(spec.effective_balance_increment)?;
            Ok(altair::get_base_reward(
                effective_balance,
                base_reward_per_increment,
                spec,
            )?)
        })
        .collect::<Result<Vec<_>, EpochCacheError>>()?;
    Ok(PreviousEpochBaseRewards {
        epoch: previous_epoch,
        base_rewards,
        effective_balance_increment: spec.effective_balance_increment,
    })
}

/// The denominator of the inactivity penalty for the rewards of `previous_epoch`.
///
/// The penalty scales with the square of the slot duration, so that the penalty over a leak of a
/// fixed wall-clock length does not change.
pub fn inactivity_penalty_denominator(
    fork_name: ForkName,
    previous_epoch: Epoch,
    spec: &ChainSpec,
    _active: Active<Eip8198>,
) -> Result<u64, ArithError> {
    let genesis_slot_duration_ms = spec.get_slot_duration_ms(Epoch::new(0));
    let previous_slot_duration_ms = spec.get_slot_duration_ms(previous_epoch);
    spec.inactivity_score_bias
        .safe_mul(spec.inactivity_penalty_quotient_for_fork(fork_name))?
        .safe_mul(genesis_slot_duration_ms.safe_mul(genesis_slot_duration_ms)?)?
        .safe_div(previous_slot_duration_ms.safe_mul(previous_slot_duration_ms)?)
}

/// Spec: `get_activation_churn_limit`, for epoch processing.
pub(crate) fn get_activation_exit_churn_limit(
    state_ctxt: &StateContext,
    spec: &ChainSpec,
    _active: Active<Eip8198>,
) -> Result<u64, Error> {
    let churn = std::cmp::max(
        spec.min_per_epoch_churn_limit_electra,
        state_ctxt
            .total_active_balance
            .safe_div(spec.churn_limit_quotient_gloas)?,
    );
    let churn = scale_by_slot_duration(
        std::cmp::min(spec.max_per_epoch_activation_churn_limit_gloas, churn),
        state_ctxt.current_epoch,
        spec,
    )?;
    Ok(churn.safe_sub(churn.safe_rem(spec.effective_balance_increment)?)?)
}

/// Spec: `get_exit_churn_limit`, for epoch processing.
pub(crate) fn get_balance_churn_limit(
    state_ctxt: &StateContext,
    spec: &ChainSpec,
    _active: Active<Eip8198>,
) -> Result<u64, Error> {
    let churn = std::cmp::max(
        spec.min_per_epoch_churn_limit_electra,
        state_ctxt
            .total_active_balance
            .safe_div(spec.churn_limit_quotient_gloas)?,
    );
    let churn = scale_by_slot_duration(churn, state_ctxt.current_epoch, spec)?;
    Ok(churn.safe_sub(churn.safe_rem(spec.effective_balance_increment)?)?)
}

/// Spec: `compute_timestamp_at_slot`, on the slot duration schedule.
pub fn compute_timestamp_at_slot<E: EthSpec>(
    state: &BeaconState<E>,
    block_slot: Slot,
    spec: &ChainSpec,
    _active: Active<Eip8198>,
) -> Result<u64, ArithError> {
    let slots_since_genesis = block_slot.as_u64().safe_sub(spec.genesis_slot.as_u64())?;
    spec.slot_duration_schedule()
        .compute_time_at_slot_ms(
            E::slots_per_epoch(),
            state.genesis_time().safe_mul(1000)?,
            Slot::new(slots_since_genesis),
        )?
        .safe_div(1000)
}
