//! Pure core of the pending deposits decisions.
//!
//! Aeneas translates this module for the proofs in `../../proofs`. So it takes plain values, uses
//! `while` loops and has no `?` inside a loop.

use safe_arith::{ArithError, SafeArith};

/// The values of one pending deposit and its validator that the decisions read.
pub struct DepositView {
    pub slot: u64,
    pub amount: u64,
    pub is_known_validator: bool,
    pub exit_epoch: u64,
    pub withdrawable_epoch: u64,
    pub activation_epoch: u64,
    pub effective_balance: u64,
    /// Before Fulu, a deposit request waits until the Eth1 bridge deposits are applied.
    pub eth1_bridge_blocked: bool,
}

/// The chain constants that the decisions read.
pub struct DepositConstants {
    pub far_future_epoch: u64,
    pub ejection_balance: u64,
    pub max_pending_deposits_per_epoch: u64,
    /// The spec runs `process_registry_updates` before `process_pending_deposits`. If the
    /// registry update also runs, a validator that it ejects counts as exited.
    pub registry_updates: bool,
}

/// The result of the decisions.
pub struct DepositsOutcome {
    pub next_deposit_index: u64,
    pub deposit_balance_to_consume: u64,
    /// For each deposit before `next_deposit_index`: `true` to postpone it, `false` to apply it.
    pub postponed: Vec<bool>,
}

/// Returns whether the validator of a deposit is exited and whether it is withdrawn, as the
/// spec sees it after `process_registry_updates`.
pub fn deposit_status(
    view: &DepositView,
    current_epoch: u64,
    next_epoch: u64,
    constants: &DepositConstants,
) -> (bool, bool) {
    if !view.is_known_validator {
        return (false, false);
    }
    let already_exited = view.exit_epoch < constants.far_future_epoch;
    let is_active = view.activation_epoch <= current_epoch && current_epoch < view.exit_epoch;
    let will_be_exited = constants.registry_updates
        && is_active
        && view.effective_balance <= constants.ejection_balance;
    (
        already_exited || will_be_exited,
        view.withdrawable_epoch < next_epoch,
    )
}

/// Decides which pending deposits to apply and which to postpone, and how much churn is left.
pub fn process_pending_deposits(
    views: &[DepositView],
    finalized_slot: u64,
    current_epoch: u64,
    next_epoch: u64,
    deposit_balance_to_consume: u64,
    activation_churn_limit: u64,
    constants: &DepositConstants,
) -> Result<DepositsOutcome, ArithError> {
    let available_for_processing = deposit_balance_to_consume.safe_add(activation_churn_limit)?;
    let mut processed_amount: u64 = 0;
    let mut next_deposit_index: u64 = 0;
    let mut is_churn_limit_reached = false;
    let mut overflow = false;
    let mut postponed = Vec::new();
    let mut i = 0;
    while i < views.len() {
        if let Some(view) = views.get(i) {
            if view.eth1_bridge_blocked
                || view.slot > finalized_slot
                || next_deposit_index >= constants.max_pending_deposits_per_epoch
            {
                break;
            }
            let (is_exited, is_withdrawn) =
                deposit_status(view, current_epoch, next_epoch, constants);
            if is_withdrawn {
                postponed.push(false);
            } else if is_exited {
                postponed.push(true);
            } else {
                match processed_amount.checked_add(view.amount) {
                    None => {
                        overflow = true;
                        break;
                    }
                    Some(total) => {
                        is_churn_limit_reached = total > available_for_processing;
                        if is_churn_limit_reached {
                            break;
                        }
                        processed_amount = total;
                        postponed.push(false);
                    }
                }
            }
            next_deposit_index = next_deposit_index.saturating_add(1);
        }
        i = i.saturating_add(1);
    }
    if overflow {
        return Err(ArithError::Overflow);
    }

    let deposit_balance_to_consume = if is_churn_limit_reached {
        available_for_processing.safe_sub(processed_amount)?
    } else {
        0
    };
    Ok(DepositsOutcome {
        next_deposit_index,
        deposit_balance_to_consume,
        postponed,
    })
}
