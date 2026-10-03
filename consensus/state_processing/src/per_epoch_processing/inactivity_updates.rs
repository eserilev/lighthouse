//! Pure core of the inactivity score update.
//!
//! Aeneas translates this module for the proofs in `../../proofs`. So it takes plain values.

use safe_arith::{ArithError, SafeArith};
use std::cmp::min;

/// Returns the new inactivity score of one validator.
pub fn new_inactivity_score(
    inactivity_score: u64,
    is_eligible: bool,
    is_unslashed_participating_target: bool,
    is_in_inactivity_leak: bool,
    inactivity_score_bias: u64,
    inactivity_score_recovery_rate: u64,
) -> Result<u64, ArithError> {
    if !is_eligible {
        return Ok(inactivity_score);
    }

    let mut score = inactivity_score;
    if is_unslashed_participating_target {
        if score == 0 {
            return Ok(0);
        }
        score = score.safe_sub(1)?;
    } else {
        score = score.safe_add(inactivity_score_bias)?;
    }

    if !is_in_inactivity_leak {
        let deduction = min(inactivity_score_recovery_rate, score);
        score = score.safe_sub(deduction)?;
    }

    Ok(score)
}
