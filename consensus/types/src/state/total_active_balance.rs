//! Pure functions for the total active balance of `PreEpochCache` and `BeaconState`.
//!
//! Aeneas translates this module for the proofs in `consensus/types/proofs`, so it uses plain
//! integers, indexed `while` loops and no iterator adapters.

use safe_arith::{ArithError, SafeArith};

/// Returns `max(total_active_balance, effective_balance_increment)`, the floor of `get_total_balance`.
pub fn floor_total_active_balance(
    total_active_balance: u64,
    effective_balance_increment: u64,
) -> u64 {
    if total_active_balance > effective_balance_increment {
        total_active_balance
    } else {
        effective_balance_increment
    }
}

/// Records the effective balance of a validator. Returns `Ok(false)` if the index is out of bounds.
pub fn update_effective_balance(
    effective_balances: &mut Vec<u64>,
    total_active_balance: &mut u64,
    validator_index: usize,
    effective_balance: u64,
    is_active_next_epoch: bool,
) -> Result<bool, ArithError> {
    if validator_index == effective_balances.len() {
        effective_balances.push(effective_balance);
        if is_active_next_epoch {
            *total_active_balance = total_active_balance.safe_add(effective_balance)?;
        }
        Ok(true)
    } else if let Some(existing_balance) = effective_balances.get_mut(validator_index) {
        if is_active_next_epoch {
            *total_active_balance = total_active_balance.safe_add(effective_balance)?;
            *total_active_balance = total_active_balance.safe_sub(*existing_balance)?;
        }
        *existing_balance = effective_balance;
        Ok(true)
    } else {
        Ok(false)
    }
}
