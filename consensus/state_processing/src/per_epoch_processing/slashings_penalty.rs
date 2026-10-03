//! Pure core of the slashings penalty.
//!
//! Aeneas translates this module for the proofs in `../../proofs`. So it takes plain values.

use safe_arith::{ArithError, SafeArith};
use std::cmp::min;

/// Returns the adjusted total slashing balance, the target withdrawable epoch and the penalty
/// per effective balance increment.
pub fn slashings_context(
    sum_slashings: u64,
    proportional_slashing_multiplier: u64,
    total_active_balance: u64,
    current_epoch: u64,
    epochs_per_slashings_vector: u64,
    effective_balance_increment: u64,
) -> Result<(u64, u64, u64), ArithError> {
    let adjusted_total_slashing_balance = min(
        sum_slashings.safe_mul(proportional_slashing_multiplier)?,
        total_active_balance,
    );
    let target_withdrawable_epoch =
        current_epoch.safe_add(epochs_per_slashings_vector.safe_div(2)?)?;
    let penalty_per_effective_balance_increment = adjusted_total_slashing_balance
        .safe_div(total_active_balance.safe_div(effective_balance_increment)?)?;
    Ok((
        adjusted_total_slashing_balance,
        target_withdrawable_epoch,
        penalty_per_effective_balance_increment,
    ))
}

/// Returns the balance of one validator after the slashings penalty.
#[allow(clippy::too_many_arguments)]
pub fn new_balance_after_slashing(
    balance: u64,
    slashed: bool,
    withdrawable_epoch: u64,
    effective_balance: u64,
    target_withdrawable_epoch: u64,
    adjusted_total_slashing_balance: u64,
    penalty_per_effective_balance_increment: u64,
    total_active_balance: u64,
    effective_balance_increment: u64,
    electra_enabled: bool,
) -> Result<u64, ArithError> {
    if !slashed || target_withdrawable_epoch != withdrawable_epoch {
        return Ok(balance);
    }
    let penalty = if electra_enabled {
        let effective_balance_increments =
            effective_balance.safe_div(effective_balance_increment)?;
        penalty_per_effective_balance_increment.safe_mul(effective_balance_increments)?
    } else {
        let penalty_numerator = effective_balance
            .safe_div(effective_balance_increment)?
            .safe_mul(adjusted_total_slashing_balance)?;
        penalty_numerator
            .safe_div(total_active_balance)?
            .safe_mul(effective_balance_increment)?
    };
    Ok(balance.saturating_sub(penalty))
}
