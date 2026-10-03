//! Pure functions for the exit queue epoch and churn in `ExitCache`.
//!
//! Aeneas translates this module for the proofs in `consensus/types/proofs`, so it uses plain
//! integers, indexed `while` loops and no iterator adapters.

use safe_arith::{ArithError, SafeArith};

/// Returns the new `(max_exit_epoch, max_exit_epoch_churn)` after one exit at `exit_epoch`.
pub fn record_exit(
    max_exit_epoch: u64,
    max_exit_epoch_churn: u64,
    exit_epoch: u64,
) -> Result<(u64, u64), ArithError> {
    if exit_epoch == max_exit_epoch {
        Ok((max_exit_epoch, max_exit_epoch_churn.safe_add(1)?))
    } else if exit_epoch > max_exit_epoch {
        Ok((exit_epoch, 1))
    } else {
        Ok((max_exit_epoch, max_exit_epoch_churn))
    }
}

/// Returns the number of exits at `exit_epoch`, or `None` if `exit_epoch` is below the maximum.
pub fn churn_at(max_exit_epoch: u64, max_exit_epoch_churn: u64, exit_epoch: u64) -> Option<u64> {
    if exit_epoch == max_exit_epoch {
        Some(max_exit_epoch_churn)
    } else if exit_epoch > max_exit_epoch {
        Some(0)
    } else {
        None
    }
}
