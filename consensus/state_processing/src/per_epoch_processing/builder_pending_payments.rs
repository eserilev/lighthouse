//! Pure core of `process_builder_pending_payments`.
//!
//! Aeneas translates this module for the proofs in `../../proofs`. So it uses `while` loops,
//! no iterator adapters and no `?` inside a loop.

use safe_arith::{ArithError, SafeArith};
use types::{BuilderPendingPayment, BuilderPendingWithdrawal};

pub fn get_builder_payment_quorum_threshold(
    total_active_balance: u64,
    slots_per_epoch: u64,
    numerator: u64,
    denominator: u64,
) -> Result<u64, ArithError> {
    let per_slot_balance = total_active_balance.safe_div(slots_per_epoch)?;
    let quorum = per_slot_balance.safe_mul(numerator)?;
    quorum.safe_div(denominator)
}

/// Returns the withdrawals to append and the new payments vector.
pub fn process_builder_pending_payments(
    payments: &[BuilderPendingPayment],
    slots_per_epoch: usize,
    quorum: u64,
) -> (Vec<BuilderPendingWithdrawal>, Vec<BuilderPendingPayment>) {
    let mut new_withdrawals = Vec::new();
    let mut i = 0;
    while i < slots_per_epoch {
        if let Some(payment) = payments.get(i)
            && payment.weight >= quorum
        {
            new_withdrawals.push(payment.withdrawal.clone());
        }
        i = i.saturating_add(1);
    }

    let mut updated_payments = Vec::new();
    let mut i = slots_per_epoch;
    while i < payments.len() {
        if let Some(payment) = payments.get(i) {
            updated_payments.push(payment.clone());
        }
        i = i.saturating_add(1);
    }
    let mut i = 0;
    while i < slots_per_epoch {
        updated_payments.push(BuilderPendingPayment::default());
        i = i.saturating_add(1);
    }

    (new_withdrawals, updated_payments)
}
