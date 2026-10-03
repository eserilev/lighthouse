//! Pure core of the effective balance update.
//!
//! Aeneas translates this module for the proofs in `../../proofs`. So it takes plain `u64`
//! values, not `Validator` or `ChainSpec`.

use safe_arith::{ArithError, SafeArith};
use std::cmp::min;

/// Returns the downward and upward hysteresis thresholds.
pub fn hysteresis_thresholds(
    effective_balance_increment: u64,
    hysteresis_quotient: u64,
    hysteresis_downward_multiplier: u64,
    hysteresis_upward_multiplier: u64,
) -> Result<(u64, u64), ArithError> {
    let hysteresis_increment = effective_balance_increment.safe_div(hysteresis_quotient)?;
    let downward_threshold = hysteresis_increment.safe_mul(hysteresis_downward_multiplier)?;
    let upward_threshold = hysteresis_increment.safe_mul(hysteresis_upward_multiplier)?;
    Ok((downward_threshold, upward_threshold))
}

/// Returns the new effective balance of one validator.
pub fn new_effective_balance(
    balance: u64,
    effective_balance: u64,
    effective_balance_limit: u64,
    downward_threshold: u64,
    upward_threshold: u64,
    effective_balance_increment: u64,
) -> Result<u64, ArithError> {
    if balance.safe_add(downward_threshold)? < effective_balance
        || effective_balance.safe_add(upward_threshold)? < balance
    {
        Ok(min(
            balance.safe_sub(balance.safe_rem(effective_balance_increment)?)?,
            effective_balance_limit,
        ))
    } else {
        Ok(effective_balance)
    }
}
