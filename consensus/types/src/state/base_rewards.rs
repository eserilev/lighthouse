//! Pure functions for the base reward table and the reads of `EpochCache`.
//!
//! Aeneas translates this module for the proofs in `consensus/types/proofs`, so it uses plain
//! integers, indexed `while` loops and no iterator adapters.

use safe_arith::{ArithError, SafeArith};

use super::total_active_balance::floor_total_active_balance;

/// Error of a read from the `EpochCache` vectors.
pub enum ReadError {
    ValidatorIndexOutOfBounds,
    EffectiveBalanceOutOfBounds(usize),
    Arith(ArithError),
}

/// Error of the base reward table build.
pub enum BaseRewardsError {
    Arith(ArithError),
    AltairBaseReward(ArithError),
}

impl From<ArithError> for BaseRewardsError {
    fn from(e: ArithError) -> Self {
        BaseRewardsError::Arith(e)
    }
}

/// Returns the largest `x` with `x * x <= n`, with the Newton loop of the spec `integer_squareroot`.
pub fn integer_sqrt(n: u64) -> u64 {
    if n == u64::MAX {
        return 4294967295;
    }
    let mut x = n;
    let mut y = x.saturating_add(1) / 2;
    while y < x {
        x = y;
        y = x.saturating_add(n.checked_div(x).unwrap_or(0)) / 2;
    }
    x
}

/// Returns the altair `get_base_reward_per_increment`.
pub fn base_reward_per_increment(
    total_active_balance: u64,
    effective_balance_increment: u64,
    base_reward_factor: u64,
) -> Result<u64, ArithError> {
    effective_balance_increment
        .safe_mul(base_reward_factor)?
        .safe_div(integer_sqrt(total_active_balance))
}

/// Returns the altair base reward for an effective balance.
pub fn altair_base_reward(
    effective_balance: u64,
    effective_balance_increment: u64,
    base_reward_per_increment: u64,
) -> Result<u64, ArithError> {
    effective_balance
        .safe_div(effective_balance_increment)?
        .safe_mul(base_reward_per_increment)
}

/// Returns the phase0 base reward for an effective balance.
pub fn phase0_base_reward(
    effective_balance: u64,
    sqrt_total_active_balance: u64,
    base_reward_factor: u64,
    base_rewards_per_epoch: u64,
) -> Result<u64, ArithError> {
    effective_balance
        .safe_mul(base_reward_factor)?
        .safe_div(sqrt_total_active_balance)?
        .safe_div(base_rewards_per_epoch)
}

/// Returns the base reward for `effective_balance_eth` increments.
#[allow(clippy::too_many_arguments)]
pub fn base_reward_at(
    effective_balance_eth: u64,
    effective_balance_increment: u64,
    is_phase0: bool,
    sqrt_total_active_balance: u64,
    base_reward_per_increment: u64,
    base_reward_factor: u64,
    base_rewards_per_epoch: u64,
) -> Result<u64, BaseRewardsError> {
    let effective_balance = effective_balance_eth.safe_mul(effective_balance_increment)?;
    if is_phase0 {
        Ok(phase0_base_reward(
            effective_balance,
            sqrt_total_active_balance,
            base_reward_factor,
            base_rewards_per_epoch,
        )?)
    } else {
        match altair_base_reward(
            effective_balance,
            effective_balance_increment,
            base_reward_per_increment,
        ) {
            Ok(base_reward) => Ok(base_reward),
            Err(e) => Err(BaseRewardsError::AltairBaseReward(e)),
        }
    }
}

/// Returns the base reward for each effective balance increment from 0 to `max_effective_balance`.
pub fn base_rewards(
    total_active_balance: u64,
    effective_balance_increment: u64,
    max_effective_balance: u64,
    base_reward_factor: u64,
    base_rewards_per_epoch: u64,
    is_phase0: bool,
) -> Result<Vec<u64>, BaseRewardsError> {
    let total_active_balance =
        floor_total_active_balance(total_active_balance, effective_balance_increment);
    let sqrt_total_active_balance = integer_sqrt(total_active_balance);
    let base_reward_per_increment = base_reward_per_increment(
        total_active_balance,
        effective_balance_increment,
        base_reward_factor,
    )?;
    let max_effective_balance_eth = max_effective_balance.safe_div(effective_balance_increment)?;
    let capacity = max_effective_balance_eth.safe_add(1)?;

    let mut base_rewards = Vec::with_capacity(capacity as usize);
    let mut error = None;
    let mut effective_balance_eth = 0;
    while effective_balance_eth <= max_effective_balance_eth && error.is_none() {
        match base_reward_at(
            effective_balance_eth,
            effective_balance_increment,
            is_phase0,
            sqrt_total_active_balance,
            base_reward_per_increment,
            base_reward_factor,
            base_rewards_per_epoch,
        ) {
            Ok(base_reward) => base_rewards.push(base_reward),
            Err(e) => error = Some(e),
        }
        effective_balance_eth = effective_balance_eth.saturating_add(1);
    }

    match error {
        Some(e) => Err(e),
        None => Ok(base_rewards),
    }
}

/// Returns the cached effective balance of a validator.
pub fn get_effective_balance(
    effective_balances: &[u64],
    validator_index: usize,
) -> Result<u64, ReadError> {
    match effective_balances.get(validator_index) {
        Some(effective_balance) => Ok(*effective_balance),
        None => Err(ReadError::ValidatorIndexOutOfBounds),
    }
}

/// Returns the cached base reward of a validator.
pub fn get_base_reward(
    effective_balances: &[u64],
    base_rewards: &[u64],
    effective_balance_increment: u64,
    validator_index: usize,
) -> Result<u64, ReadError> {
    let effective_balance = get_effective_balance(effective_balances, validator_index)?;
    let effective_balance_eth = match effective_balance.safe_div(effective_balance_increment) {
        Ok(effective_balance_eth) => effective_balance_eth as usize,
        Err(e) => return Err(ReadError::Arith(e)),
    };
    match base_rewards.get(effective_balance_eth) {
        Some(base_reward) => Ok(*base_reward),
        None => Err(ReadError::EffectiveBalanceOutOfBounds(
            effective_balance_eth,
        )),
    }
}
