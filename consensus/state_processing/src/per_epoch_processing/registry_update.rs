//! Pure core of the per-validator registry update after Electra.
//!
//! Aeneas translates this module for the proofs in `../../proofs`. So it takes plain values.

use safe_arith::{ArithError, SafeArith};
use std::cmp::{max, min};

/// The values that a registry update reads and can change.
pub struct RegistryFields {
    pub activation_eligibility_epoch: u64,
    pub activation_epoch: u64,
    pub exit_epoch: u64,
    pub withdrawable_epoch: u64,
    pub earliest_exit_epoch: u64,
    pub exit_balance_to_consume: u64,
}

/// The chain constants that a registry update reads.
pub struct RegistryConstants {
    pub far_future_epoch: u64,
    pub min_activation_balance: u64,
    pub ejection_balance: u64,
    pub max_seed_lookahead: u64,
    pub min_validator_withdrawability_delay: u64,
    pub min_per_epoch_churn_limit: u64,
    pub churn_limit_quotient: u64,
    pub effective_balance_increment: u64,
    /// The upper limit of the exit churn. Gloas has no limit, so it passes `u64::MAX`.
    pub exit_churn_cap: u64,
}

pub fn compute_activation_exit_epoch(
    epoch: u64,
    max_seed_lookahead: u64,
) -> Result<u64, ArithError> {
    epoch.safe_add(1)?.safe_add(max_seed_lookahead)
}

/// Returns the per-epoch churn limit for exits.
pub fn exit_churn_limit(
    total_active_balance: u64,
    constants: &RegistryConstants,
) -> Result<u64, ArithError> {
    let churn = max(
        constants.min_per_epoch_churn_limit,
        total_active_balance.safe_div(constants.churn_limit_quotient)?,
    );
    let rounded = churn.safe_sub(churn.safe_rem(constants.effective_balance_increment)?)?;
    Ok(min(constants.exit_churn_cap, rounded))
}

/// Returns the exit epoch, the new earliest exit epoch and the new exit balance to consume.
pub fn compute_exit_epoch_and_update_churn(
    exit_balance: u64,
    current_epoch: u64,
    earliest_exit_epoch_state: u64,
    exit_balance_to_consume_state: u64,
    total_active_balance: u64,
    constants: &RegistryConstants,
) -> Result<(u64, u64, u64), ArithError> {
    let mut earliest_exit_epoch = max(
        earliest_exit_epoch_state,
        compute_activation_exit_epoch(current_epoch, constants.max_seed_lookahead)?,
    );
    let per_epoch_churn = exit_churn_limit(total_active_balance, constants)?;
    let mut exit_balance_to_consume = if earliest_exit_epoch_state < earliest_exit_epoch {
        per_epoch_churn
    } else {
        exit_balance_to_consume_state
    };

    if exit_balance > exit_balance_to_consume {
        let balance_to_process = exit_balance.safe_sub(exit_balance_to_consume)?;
        let additional_epochs = balance_to_process
            .safe_sub(1)?
            .safe_div(per_epoch_churn)?
            .safe_add(1)?;
        earliest_exit_epoch = earliest_exit_epoch.safe_add(additional_epochs)?;
        exit_balance_to_consume =
            exit_balance_to_consume.safe_add(additional_epochs.safe_mul(per_epoch_churn)?)?;
    }

    let new_exit_balance_to_consume = exit_balance_to_consume.safe_sub(exit_balance)?;
    Ok((
        earliest_exit_epoch,
        earliest_exit_epoch,
        new_exit_balance_to_consume,
    ))
}

/// Puts a validator into the activation queue.
pub fn eligibility_step(
    fields: RegistryFields,
    effective_balance: u64,
    current_epoch: u64,
    constants: &RegistryConstants,
) -> Result<RegistryFields, ArithError> {
    let mut fields = fields;
    if fields.activation_eligibility_epoch == constants.far_future_epoch
        && effective_balance >= constants.min_activation_balance
    {
        fields.activation_eligibility_epoch = current_epoch.safe_add(1)?;
    }
    Ok(fields)
}

/// Ejects an active validator with a low effective balance.
pub fn ejection_step(
    fields: RegistryFields,
    effective_balance: u64,
    current_epoch: u64,
    total_active_balance: u64,
    constants: &RegistryConstants,
) -> Result<RegistryFields, ArithError> {
    let mut fields = fields;
    let is_active = fields.activation_epoch <= current_epoch && current_epoch < fields.exit_epoch;
    if is_active
        && effective_balance <= constants.ejection_balance
        && fields.exit_epoch == constants.far_future_epoch
    {
        let (exit_epoch, earliest_exit_epoch, exit_balance_to_consume) =
            compute_exit_epoch_and_update_churn(
                effective_balance,
                current_epoch,
                fields.earliest_exit_epoch,
                fields.exit_balance_to_consume,
                total_active_balance,
                constants,
            )?;
        fields.exit_epoch = exit_epoch;
        fields.withdrawable_epoch =
            exit_epoch.safe_add(constants.min_validator_withdrawability_delay)?;
        fields.earliest_exit_epoch = earliest_exit_epoch;
        fields.exit_balance_to_consume = exit_balance_to_consume;
    }
    Ok(fields)
}

/// Activates a validator whose queue placement is finalized.
pub fn activation_step(
    fields: RegistryFields,
    current_epoch: u64,
    finalized_epoch: u64,
    constants: &RegistryConstants,
) -> Result<RegistryFields, ArithError> {
    let mut fields = fields;
    if fields.activation_eligibility_epoch <= finalized_epoch
        && fields.activation_epoch == constants.far_future_epoch
    {
        fields.activation_epoch =
            compute_activation_exit_epoch(current_epoch, constants.max_seed_lookahead)?;
    }
    Ok(fields)
}

/// Returns the registry fields of one validator after the registry update.
///
/// The three steps run one after the other. The spec uses `if`/`elif`/`elif` instead. The two
/// agree on valid states.
pub fn registry_update(
    fields: RegistryFields,
    effective_balance: u64,
    current_epoch: u64,
    finalized_epoch: u64,
    total_active_balance: u64,
    constants: &RegistryConstants,
) -> Result<RegistryFields, ArithError> {
    let fields = eligibility_step(fields, effective_balance, current_epoch, constants)?;
    let fields = ejection_step(
        fields,
        effective_balance,
        current_epoch,
        total_active_balance,
        constants,
    )?;
    activation_step(fields, current_epoch, finalized_epoch, constants)
}
