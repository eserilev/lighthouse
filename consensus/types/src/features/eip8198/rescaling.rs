//! Churn limits and base rewards that scale with the slot duration.

use safe_arith::{ArithError, SafeArith};

use crate::core::{ChainSpec, Epoch, EthSpec};
use crate::features::{Active, Eip8198};
use crate::state::{BeaconState, BeaconStateError, EpochCache, EpochCacheError};

/// Scale `value` by the slot duration at `epoch` over the slot duration at genesis.
pub fn scale_by_slot_duration(
    value: u64,
    epoch: Epoch,
    spec: &ChainSpec,
) -> Result<u64, ArithError> {
    value
        .safe_mul(spec.get_slot_duration_ms(epoch))?
        .safe_div(spec.get_slot_duration_ms(Epoch::new(0)))
}

/// Base rewards priced at the slot duration of the epoch before the epoch of an epoch cache.
///
/// Keyed by `effective_balance / effective_balance_increment`.
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PreviousEpochBaseRewards {
    pub epoch: Epoch,
    pub base_rewards: Vec<u64>,
    pub effective_balance_increment: u64,
}

/// Spec: `get_base_reward(state, index, epoch)`, for `epoch` equal to the epoch of the cache or
/// the epoch before it.
pub fn get_base_reward_for_epoch(
    epoch_cache: &EpochCache,
    validator_index: usize,
    epoch: Epoch,
    _active: Active<Eip8198>,
) -> Result<u64, EpochCacheError> {
    let previous = epoch_cache
        .previous_epoch_base_rewards()?
        .ok_or(EpochCacheError::CacheNotInitialized)?;
    if previous.epoch.safe_add(1)? == epoch {
        return epoch_cache.get_base_reward(validator_index);
    }
    if previous.epoch != epoch {
        return Err(EpochCacheError::IncorrectEpoch {
            cache: previous.epoch.safe_add(1)?,
            state: epoch,
        });
    }
    let effective_balance_eth = epoch_cache
        .get_effective_balance(validator_index)?
        .safe_div(previous.effective_balance_increment)? as usize;
    previous
        .base_rewards
        .get(effective_balance_eth)
        .copied()
        .ok_or(EpochCacheError::EffectiveBalanceOutOfBounds {
            effective_balance_eth,
        })
}

/// Spec: `get_exit_churn_limit`, scaled by the slot duration.
pub fn get_balance_churn_limit<E: EthSpec>(
    state: &BeaconState<E>,
    spec: &ChainSpec,
    _active: Active<Eip8198>,
) -> Result<u64, BeaconStateError> {
    let total_active_balance = state.get_total_active_balance()?;
    let churn = std::cmp::max(
        spec.min_per_epoch_churn_limit_electra,
        total_active_balance.safe_div(spec.churn_limit_quotient_gloas)?,
    );
    let churn = scale_by_slot_duration(churn, state.current_epoch(), spec)?;

    Ok(churn.safe_sub(churn.safe_rem(spec.effective_balance_increment)?)?)
}

/// Spec: `get_activation_churn_limit`. The cap applies before the slot duration scaling.
pub fn get_activation_exit_churn_limit<E: EthSpec>(
    state: &BeaconState<E>,
    spec: &ChainSpec,
    _active: Active<Eip8198>,
) -> Result<u64, BeaconStateError> {
    let churn = std::cmp::max(
        spec.min_per_epoch_churn_limit_electra,
        state
            .get_total_active_balance()?
            .safe_div(spec.churn_limit_quotient_gloas)?,
    );
    let churn = scale_by_slot_duration(
        std::cmp::min(spec.max_per_epoch_activation_churn_limit_gloas, churn),
        state.current_epoch(),
        spec,
    )?;
    Ok(churn.safe_sub(churn.safe_rem(spec.effective_balance_increment)?)?)
}

/// Spec: `get_consolidation_churn_limit`, scaled by the slot duration.
pub fn get_consolidation_churn_limit<E: EthSpec>(
    state: &BeaconState<E>,
    spec: &ChainSpec,
    _active: Active<Eip8198>,
) -> Result<u64, BeaconStateError> {
    let churn = scale_by_slot_duration(
        state
            .get_total_active_balance()?
            .safe_div(spec.consolidation_churn_limit_quotient)?,
        state.current_epoch(),
        spec,
    )?;
    Ok(churn.safe_sub(churn.safe_rem(spec.effective_balance_increment)?)?)
}
