use crate::metrics;
use fixed_bytes::FixedBytesExtended;
use tracing::instrument;
use types::state::base_rewards::{self, BaseRewardsError};
use types::state::total_active_balance;
use types::state::{EpochCache, EpochCacheError, EpochCacheKey};
use types::{ActivationQueue, BeaconState, ChainSpec, EthSpec, ForkName, Hash256};

/// Precursor to an `EpochCache`.
pub struct PreEpochCache {
    epoch_key: EpochCacheKey,
    effective_balances: Vec<u64>,
    total_active_balance: u64,
}

impl PreEpochCache {
    pub fn new_for_next_epoch<E: EthSpec>(
        state: &mut BeaconState<E>,
    ) -> Result<Self, EpochCacheError> {
        // The decision block root for the next epoch is the latest block root from this epoch.
        let latest_block_header = state.latest_block_header();

        let decision_block_root = if !latest_block_header.state_root.is_zero() {
            latest_block_header.canonical_root()
        } else {
            // State root should already have been filled in by `process_slot`, except in the case
            // of a `partial_state_advance`. Once we have tree-states this can be an error, and
            // `self` can be immutable.
            let state_root = state.update_tree_hash_cache()?;
            state.get_latest_block_root(state_root)
        };

        let epoch_key = EpochCacheKey {
            epoch: state.next_epoch()?,
            decision_block_root,
        };

        Ok(Self {
            epoch_key,
            effective_balances: Vec::with_capacity(state.validators().len()),
            total_active_balance: 0,
        })
    }

    pub fn update_effective_balance(
        &mut self,
        validator_index: usize,
        effective_balance: u64,
        is_active_next_epoch: bool,
    ) -> Result<(), EpochCacheError> {
        if total_active_balance::update_effective_balance(
            &mut self.effective_balances,
            &mut self.total_active_balance,
            validator_index,
            effective_balance,
            is_active_next_epoch,
        )? {
            Ok(())
        } else {
            Err(EpochCacheError::ValidatorIndexOutOfBounds { validator_index })
        }
    }

    /// Note: the spec-mandated floor (max with EFFECTIVE_BALANCE_INCREMENT) is applied in
    /// `into_epoch_cache` and `set_total_active_balance`. This returns the raw sum.
    pub fn get_total_active_balance(&self) -> u64 {
        self.total_active_balance
    }

    pub fn into_epoch_cache(
        self,
        activation_queue: ActivationQueue,
        spec: &ChainSpec,
    ) -> Result<EpochCache, EpochCacheError> {
        let fork_name = spec.fork_name_at_epoch(self.epoch_key.epoch);
        let base_rewards = base_rewards::base_rewards(
            self.total_active_balance,
            spec.effective_balance_increment,
            spec.max_effective_balance_for_fork(fork_name),
            spec.base_reward_factor,
            spec.base_rewards_per_epoch,
            fork_name == ForkName::Base,
        )
        .map_err(|e| match e {
            BaseRewardsError::Arith(e) => EpochCacheError::Arith(e),
            BaseRewardsError::AltairBaseReward(e) => EpochCacheError::BeaconState(e.into()),
        })?;

        Ok(EpochCache::new(
            self.epoch_key,
            self.effective_balances,
            base_rewards,
            activation_queue,
            spec,
        ))
    }
}

pub fn is_epoch_cache_initialized<E: EthSpec>(
    state: &BeaconState<E>,
) -> Result<bool, EpochCacheError> {
    let current_epoch = state.current_epoch();
    let epoch_cache: &EpochCache = state.epoch_cache();
    let decision_block_root = state
        .epoch_cache_decision_root(Hash256::zero())
        .map_err(EpochCacheError::BeaconState)?;

    Ok(epoch_cache
        .check_validity(current_epoch, decision_block_root)
        .is_ok())
}

#[instrument(skip_all, level = "debug")]
pub fn initialize_epoch_cache<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
) -> Result<(), EpochCacheError> {
    if is_epoch_cache_initialized(state)? {
        // `EpochCache` has already been initialized and is valid, no need to initialize.
        return Ok(());
    }

    let _timer = metrics::start_timer(&metrics::BUILD_EPOCH_CACHE_TIME);

    let current_epoch = state.current_epoch();
    let next_epoch = state.next_epoch().map_err(EpochCacheError::BeaconState)?;
    let decision_block_root = state
        .epoch_cache_decision_root(Hash256::zero())
        .map_err(EpochCacheError::BeaconState)?;

    state.build_total_active_balance_cache(spec)?;
    let total_active_balance = state.get_total_active_balance_at_epoch(current_epoch)?;

    // Collect effective balances and compute activation queue.
    let mut effective_balances = Vec::with_capacity(state.validators().len());
    let mut activation_queue = ActivationQueue::default();

    for (index, validator) in state.validators().iter().enumerate() {
        effective_balances.push(validator.effective_balance);

        // Add to speculative activation queue.
        activation_queue
            .add_if_could_be_eligible_for_activation(index, validator, next_epoch, spec);
    }

    // Compute base rewards.
    let pre_epoch_cache = PreEpochCache {
        epoch_key: EpochCacheKey {
            epoch: current_epoch,
            decision_block_root,
        },
        effective_balances,
        total_active_balance,
    };
    *state.epoch_cache_mut() = pre_epoch_cache.into_epoch_cache(activation_queue, spec)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::{Epoch, MinimalEthSpec};

    /// Regression test for division-by-zero when all validators have zero effective balance.
    ///
    /// When `process_effective_balance_updates` drops all effective balances to 0, the
    /// `PreEpochCache` accumulates `total_active_balance = 0`. Without the spec-mandated floor
    /// of `max(EFFECTIVE_BALANCE_INCREMENT, sum)`, `BaseRewardPerIncrement::new()` would divide
    /// by `integer_sqrt(0) = 0`.
    #[test]
    fn into_epoch_cache_zero_total_active_balance() {
        let spec = MinimalEthSpec::default_spec();

        let cache = PreEpochCache {
            epoch_key: EpochCacheKey {
                epoch: Epoch::new(1),
                decision_block_root: Hash256::zero(),
            },
            effective_balances: vec![0, 0, 0, 0],
            total_active_balance: 0,
        };

        // Verify the raw total is zero.
        assert_eq!(cache.get_total_active_balance(), 0);

        // This should succeed, not panic with division by zero.
        let epoch_cache = cache
            .into_epoch_cache(ActivationQueue::default(), &spec)
            .expect("into_epoch_cache should not fail with zero total_active_balance");

        // Base reward for validator index 0 should be 0.
        assert_eq!(epoch_cache.get_base_reward(0).unwrap(), 0);
    }
}
