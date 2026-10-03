use std::sync::Arc;

use safe_arith::ArithError;

use crate::{
    core::{ChainSpec, Epoch, Hash256, Slot},
    state::{ActivationQueue, BeaconStateError, base_rewards},
};

/// Cache of values which are uniquely determined at the start of an epoch.
///
/// The values are fixed with respect to the last block of the _prior_ epoch, which we refer
/// to as the "decision block".
///
/// Prior to Fulu this cache was similar to the `BeaconProposerCache` in that beacon proposers were
/// determined at exactly the same time as the values in this cache, so the keys for the two caches
/// were identical.
///
/// Post-Fulu, we use a different key (the proposers have more lookahead).
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct EpochCache {
    inner: Option<Arc<Inner>>,
}

#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Clone)]
struct Inner {
    /// Unique identifier for this cache, which can be used to check its validity before use
    /// with any `BeaconState`.
    key: EpochCacheKey,
    /// Effective balance for every validator in this epoch.
    effective_balances: Vec<u64>,
    /// Base rewards for every effective balance increment (currently 0..32 ETH).
    ///
    /// Keyed by `effective_balance / effective_balance_increment`.
    base_rewards: Vec<u64>,
    /// Validator activation queue.
    activation_queue: ActivationQueue,
    /// Effective balance increment.
    effective_balance_increment: u64,
}

#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct EpochCacheKey {
    pub epoch: Epoch,
    pub decision_block_root: Hash256,
}

#[derive(Debug, PartialEq, Clone)]
pub enum EpochCacheError {
    IncorrectEpoch { cache: Epoch, state: Epoch },
    IncorrectDecisionBlock { cache: Hash256, state: Hash256 },
    ValidatorIndexOutOfBounds { validator_index: usize },
    EffectiveBalanceOutOfBounds { effective_balance_eth: usize },
    InvalidSlot { slot: Slot },
    Arith(ArithError),
    BeaconState(BeaconStateError),
    CacheNotInitialized,
}

impl From<BeaconStateError> for EpochCacheError {
    fn from(e: BeaconStateError) -> Self {
        Self::BeaconState(e)
    }
}

impl From<ArithError> for EpochCacheError {
    fn from(e: ArithError) -> Self {
        Self::Arith(e)
    }
}

impl EpochCache {
    pub fn new(
        key: EpochCacheKey,
        effective_balances: Vec<u64>,
        base_rewards: Vec<u64>,
        activation_queue: ActivationQueue,
        spec: &ChainSpec,
    ) -> EpochCache {
        Self {
            inner: Some(Arc::new(Inner {
                key,
                effective_balances,
                base_rewards,
                activation_queue,
                effective_balance_increment: spec.effective_balance_increment,
            })),
        }
    }

    pub fn check_validity(
        &self,
        current_epoch: Epoch,
        state_decision_root: Hash256,
    ) -> Result<(), EpochCacheError> {
        let cache = self
            .inner
            .as_ref()
            .ok_or(EpochCacheError::CacheNotInitialized)?;
        if cache.key.epoch != current_epoch {
            return Err(EpochCacheError::IncorrectEpoch {
                cache: cache.key.epoch,
                state: current_epoch,
            });
        }
        if cache.key.decision_block_root != state_decision_root {
            return Err(EpochCacheError::IncorrectDecisionBlock {
                cache: cache.key.decision_block_root,
                state: state_decision_root,
            });
        }
        Ok(())
    }

    #[inline]
    pub fn get_effective_balance(&self, validator_index: usize) -> Result<u64, EpochCacheError> {
        let inner = self
            .inner
            .as_ref()
            .ok_or(EpochCacheError::CacheNotInitialized)?;
        base_rewards::get_effective_balance(&inner.effective_balances, validator_index)
            .map_err(|e| read_error(e, validator_index))
    }

    #[inline]
    pub fn get_base_reward(&self, validator_index: usize) -> Result<u64, EpochCacheError> {
        let inner = self
            .inner
            .as_ref()
            .ok_or(EpochCacheError::CacheNotInitialized)?;
        base_rewards::get_base_reward(
            &inner.effective_balances,
            &inner.base_rewards,
            inner.effective_balance_increment,
            validator_index,
        )
        .map_err(|e| read_error(e, validator_index))
    }

    pub fn activation_queue(&self) -> Result<&ActivationQueue, EpochCacheError> {
        let inner = self
            .inner
            .as_ref()
            .ok_or(EpochCacheError::CacheNotInitialized)?;
        Ok(&inner.activation_queue)
    }
}

fn read_error(e: base_rewards::ReadError, validator_index: usize) -> EpochCacheError {
    match e {
        base_rewards::ReadError::ValidatorIndexOutOfBounds => {
            EpochCacheError::ValidatorIndexOutOfBounds { validator_index }
        }
        base_rewards::ReadError::EffectiveBalanceOutOfBounds(effective_balance_eth) => {
            EpochCacheError::EffectiveBalanceOutOfBounds {
                effective_balance_eth,
            }
        }
        base_rewards::ReadError::Arith(e) => EpochCacheError::Arith(e),
    }
}
