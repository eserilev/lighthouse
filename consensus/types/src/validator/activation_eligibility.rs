//! Pure activation eligibility predicates for `Validator` and `ActivationQueue`.
//!
//! Aeneas translates this module for the proofs in `consensus/types/proofs`, so it uses plain
//! integers, indexed `while` loops and no iterator adapters.

/// Returns `true` if a validator could be eligible for activation at the end of `epoch`.
pub fn could_be_eligible_for_activation_at(
    activation_eligibility_epoch: u64,
    activation_epoch: u64,
    epoch: u64,
    far_future_epoch: u64,
) -> bool {
    activation_epoch == far_future_epoch && activation_eligibility_epoch < epoch
}

/// Returns `true` if a validator is eligible for activation with this finalized epoch.
pub fn is_eligible_for_activation(
    activation_eligibility_epoch: u64,
    activation_epoch: u64,
    finalized_epoch: u64,
    far_future_epoch: u64,
) -> bool {
    activation_eligibility_epoch <= finalized_epoch && activation_epoch == far_future_epoch
}
