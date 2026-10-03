//! Pure core of the pending consolidations balance moves.
//!
//! Aeneas translates this module for the proofs in `../../proofs`. So it works on a small table
//! of the validators that consolidations reference, uses `while` loops and has no `?` inside a
//! loop.

use std::cmp::min;

/// One pending consolidation, as positions in the local tables.
pub struct ConsolidationView {
    pub source: usize,
    pub target: usize,
}

/// What the moves read from a validator. `exists` is `false` for an index past the registry.
pub struct LocalValidator {
    pub exists: bool,
    pub slashed: bool,
    pub withdrawable_epoch: u64,
    pub effective_balance: u64,
}

pub enum ConsolidationError {
    /// A position in the local tables whose validator does not exist.
    UnknownValidator(usize),
    Overflow,
}

// Aeneas has no model of `Option::copied` or `Option::map`.
#[allow(clippy::manual_map)]
pub fn get_balance(balances: &[u64], i: usize) -> Option<u64> {
    match balances.get(i) {
        Some(balance) => Some(*balance),
        None => None,
    }
}

pub fn set_balance(balances: Vec<u64>, i: usize, value: u64) -> Vec<u64> {
    let mut balances = balances;
    if let Some(balance) = balances.get_mut(i) {
        *balance = value;
    }
    balances
}

fn validator_exists(validators: &[LocalValidator], i: usize) -> bool {
    match validators.get(i) {
        Some(validator) => validator.exists,
        None => false,
    }
}

/// Moves the balances of the consolidations that are ready. Returns the number of handled
/// consolidations and the new local balances.
pub fn process_pending_consolidations(
    consolidations: &[ConsolidationView],
    validators: &[LocalValidator],
    balances: Vec<u64>,
    next_epoch: u64,
) -> Result<(usize, Vec<u64>), ConsolidationError> {
    let mut balances = balances;
    let mut next_pending_consolidation: usize = 0;
    let mut error: Option<ConsolidationError> = None;
    let mut i = 0;
    while i < consolidations.len() {
        if let Some(consolidation) = consolidations.get(i) {
            let source = consolidation.source;
            let target = consolidation.target;
            if !validator_exists(validators, source) {
                error = Some(ConsolidationError::UnknownValidator(source));
                break;
            }
            if let Some(source_validator) = validators.get(source) {
                if source_validator.slashed {
                    next_pending_consolidation = next_pending_consolidation.saturating_add(1);
                } else if source_validator.withdrawable_epoch > next_epoch {
                    break;
                } else if let Some(source_balance) = get_balance(&balances, source) {
                    let source_effective_balance =
                        min(source_balance, source_validator.effective_balance);
                    balances = set_balance(
                        balances,
                        source,
                        source_balance.saturating_sub(source_effective_balance),
                    );
                    if !validator_exists(validators, target) {
                        error = Some(ConsolidationError::UnknownValidator(target));
                        break;
                    }
                    if let Some(target_balance) = get_balance(&balances, target) {
                        match target_balance.checked_add(source_effective_balance) {
                            None => {
                                error = Some(ConsolidationError::Overflow);
                                break;
                            }
                            Some(new_target_balance) => {
                                balances = set_balance(balances, target, new_target_balance);
                            }
                        }
                    }
                    next_pending_consolidation = next_pending_consolidation.saturating_add(1);
                }
            }
        }
        i = i.saturating_add(1);
    }
    match error {
        Some(error) => Err(error),
        None => Ok((next_pending_consolidation, balances)),
    }
}
