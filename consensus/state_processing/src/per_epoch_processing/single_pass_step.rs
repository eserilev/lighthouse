//! The single-pass steps for one validator, after Electra.
//!
//! `single_pass_step` runs the inactivity update, rewards and penalties, the registry update, the
//! slashings penalty, the pending deposit top-up and the effective balance update for one
//! validator, in that order. Aeneas translates this module for the
//! proofs in `../../proofs`. So it takes plain values.
//!
//! Each step has its own function. In one function, Aeneas copies the rest of the body into each
//! branch of an `if`.

use super::{
    effective_balance, inactivity_updates, registry_update, rewards_penalties, slashings_penalty,
};
use registry_update::{RegistryConstants, RegistryFields};
use safe_arith::{ArithError, SafeArith};

/// The fields of one validator that the steps read or write.
#[derive(Clone, Copy)]
pub struct ValidatorRow {
    pub balance: u64,
    pub inactivity_score: u64,
    pub effective_balance: u64,
    pub slashed: bool,
    pub activation_eligibility_epoch: u64,
    pub activation_epoch: u64,
    pub exit_epoch: u64,
    pub withdrawable_epoch: u64,
    pub previous_epoch_participation: u8,
}

/// The exit churn that the registry update carries from one validator to the next.
#[derive(Clone, Copy)]
pub struct ExitChurn {
    pub earliest_exit_epoch: u64,
    pub exit_balance_to_consume: u64,
}

/// The values that are the same for all validators in this epoch.
#[derive(Clone, Copy)]
pub struct StepContext {
    pub current_epoch: u64,
    pub previous_epoch: u64,
    pub finalized_epoch: u64,
    pub is_in_inactivity_leak: bool,
    pub inactivity_score_bias: u64,
    pub inactivity_score_recovery_rate: u64,
    pub inactivity_penalty_quotient: u64,
    pub source_increments: u64,
    pub target_increments: u64,
    pub head_increments: u64,
    pub active_increments: u64,
    pub total_active_balance: u64,
    pub target_withdrawable_epoch: u64,
    pub adjusted_total_slashing_balance: u64,
    pub penalty_per_effective_balance_increment: u64,
    pub effective_balance_increment: u64,
    pub downward_threshold: u64,
    pub upward_threshold: u64,
    /// False in the genesis epoch, when the spec skips inactivity updates and rewards.
    pub after_genesis: bool,
    pub inactivity_updates: bool,
    pub rewards_and_penalties: bool,
    pub registry_updates: bool,
    pub slashings: bool,
    pub pending_deposits: bool,
    pub effective_balance_updates: bool,
}

/// Per-validator inputs that the steps read but do not write.
#[derive(Clone, Copy)]
pub struct RowInputs {
    /// The base reward. It is only read if the validator is eligible.
    pub base_reward: u64,
    /// The sum of this epoch's pending deposits for the validator.
    pub deposit: u64,
    /// A pending consolidation names the validator. Its effective balance update then runs after
    /// consolidations.
    pub in_consolidation: bool,
    /// `get_max_effective_balance` of the validator.
    pub effective_balance_limit: u64,
}

/// The validator was active in the previous epoch.
pub fn is_active_previous_epoch(row: &ValidatorRow, previous_epoch: u64) -> bool {
    row.activation_epoch <= previous_epoch && previous_epoch < row.exit_epoch
}

/// The validator gets rewards and penalties for the previous epoch.
pub fn is_eligible(row: &ValidatorRow, previous_epoch: u64) -> Result<bool, ArithError> {
    if is_active_previous_epoch(row, previous_epoch) {
        return Ok(true);
    }
    if !row.slashed {
        return Ok(false);
    }
    Ok(previous_epoch.safe_add(1)? < row.withdrawable_epoch)
}

/// The validator is active in the previous epoch, not slashed, and has the participation flag.
pub fn is_unslashed_participating(row: &ValidatorRow, previous_epoch: u64, mask: u8) -> bool {
    is_active_previous_epoch(row, previous_epoch)
        && !row.slashed
        && row.previous_epoch_participation & mask == mask
}

fn inactivity_step(
    row: ValidatorRow,
    is_eligible: bool,
    ctx: &StepContext,
) -> Result<ValidatorRow, ArithError> {
    if !ctx.after_genesis || !ctx.inactivity_updates {
        return Ok(row);
    }
    let inactivity_score = inactivity_updates::new_inactivity_score(
        row.inactivity_score,
        is_eligible,
        is_unslashed_participating(&row, ctx.previous_epoch, 2),
        ctx.is_in_inactivity_leak,
        ctx.inactivity_score_bias,
        ctx.inactivity_score_recovery_rate,
    )?;
    Ok(ValidatorRow {
        inactivity_score,
        ..row
    })
}

fn rewards_step(
    row: ValidatorRow,
    is_eligible: bool,
    base_reward: u64,
    ctx: &StepContext,
) -> Result<ValidatorRow, ArithError> {
    if !ctx.after_genesis || !ctx.rewards_and_penalties {
        return Ok(row);
    }
    let balance = rewards_penalties::new_balance_after_rewards(
        row.balance,
        is_eligible,
        base_reward,
        row.effective_balance,
        row.inactivity_score,
        is_unslashed_participating(&row, ctx.previous_epoch, 1),
        is_unslashed_participating(&row, ctx.previous_epoch, 2),
        is_unslashed_participating(&row, ctx.previous_epoch, 4),
        ctx.is_in_inactivity_leak,
        ctx.source_increments,
        ctx.target_increments,
        ctx.head_increments,
        ctx.active_increments,
        ctx.inactivity_score_bias,
        ctx.inactivity_penalty_quotient,
    )?;
    Ok(ValidatorRow { balance, ..row })
}

fn registry_step(
    row: ValidatorRow,
    churn: ExitChurn,
    ctx: &StepContext,
    constants: &RegistryConstants,
) -> Result<(ValidatorRow, ExitChurn), ArithError> {
    if !ctx.registry_updates {
        return Ok((row, churn));
    }
    let fields = RegistryFields {
        activation_eligibility_epoch: row.activation_eligibility_epoch,
        activation_epoch: row.activation_epoch,
        exit_epoch: row.exit_epoch,
        withdrawable_epoch: row.withdrawable_epoch,
        earliest_exit_epoch: churn.earliest_exit_epoch,
        exit_balance_to_consume: churn.exit_balance_to_consume,
    };
    let new = registry_update::registry_update(
        fields,
        row.effective_balance,
        ctx.current_epoch,
        ctx.finalized_epoch,
        ctx.total_active_balance,
        constants,
    )?;
    let row = ValidatorRow {
        activation_eligibility_epoch: new.activation_eligibility_epoch,
        activation_epoch: new.activation_epoch,
        exit_epoch: new.exit_epoch,
        withdrawable_epoch: new.withdrawable_epoch,
        ..row
    };
    let churn = ExitChurn {
        earliest_exit_epoch: new.earliest_exit_epoch,
        exit_balance_to_consume: new.exit_balance_to_consume,
    };
    Ok((row, churn))
}

fn slashings_step(row: ValidatorRow, ctx: &StepContext) -> Result<ValidatorRow, ArithError> {
    if !ctx.slashings {
        return Ok(row);
    }
    let balance = slashings_penalty::new_balance_after_slashing(
        row.balance,
        row.slashed,
        row.withdrawable_epoch,
        row.effective_balance,
        ctx.target_withdrawable_epoch,
        ctx.adjusted_total_slashing_balance,
        ctx.penalty_per_effective_balance_increment,
        ctx.total_active_balance,
        ctx.effective_balance_increment,
        true,
    )?;
    Ok(ValidatorRow { balance, ..row })
}

fn deposit_step(
    row: ValidatorRow,
    deposit: u64,
    ctx: &StepContext,
) -> Result<ValidatorRow, ArithError> {
    if !ctx.pending_deposits {
        return Ok(row);
    }
    let balance = row.balance.safe_add(deposit)?;
    Ok(ValidatorRow { balance, ..row })
}

fn effective_balance_step(
    row: ValidatorRow,
    inputs: &RowInputs,
    ctx: &StepContext,
) -> Result<ValidatorRow, ArithError> {
    if !ctx.effective_balance_updates || inputs.in_consolidation {
        return Ok(row);
    }
    let effective_balance = effective_balance::new_effective_balance(
        row.balance,
        row.effective_balance,
        inputs.effective_balance_limit,
        ctx.downward_threshold,
        ctx.upward_threshold,
        ctx.effective_balance_increment,
    )?;
    Ok(ValidatorRow {
        effective_balance,
        ..row
    })
}

/// Runs the inactivity update, rewards and penalties, the registry update, the slashings penalty,
/// the pending deposit top-up and the effective balance update for one validator.
pub fn single_pass_step(
    row: ValidatorRow,
    inputs: &RowInputs,
    churn: ExitChurn,
    ctx: &StepContext,
    constants: &RegistryConstants,
) -> Result<(ValidatorRow, ExitChurn), ArithError> {
    let is_eligible = is_eligible(&row, ctx.previous_epoch)?;
    let row = inactivity_step(row, is_eligible, ctx)?;
    let row = rewards_step(row, is_eligible, inputs.base_reward, ctx)?;
    let (row, churn) = registry_step(row, churn, ctx, constants)?;
    let row = slashings_step(row, ctx)?;
    let row = deposit_step(row, inputs.deposit, ctx)?;
    let row = effective_balance_step(row, inputs, ctx)?;
    Ok((row, churn))
}
