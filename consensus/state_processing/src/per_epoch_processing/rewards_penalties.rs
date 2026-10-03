//! Pure core of the per-validator rewards and penalties.
//!
//! Aeneas translates this module for the proofs in `../../proofs`. So it takes plain values.

use safe_arith::{ArithError, SafeArith};
use types::consts::altair::{
    TIMELY_HEAD_WEIGHT, TIMELY_SOURCE_WEIGHT, TIMELY_TARGET_WEIGHT, WEIGHT_DENOMINATOR,
};

/// Returns the reward and the penalty for one participation flag.
pub fn flag_delta(
    base_reward: u64,
    weight: u64,
    is_participating: bool,
    is_head_flag: bool,
    is_in_inactivity_leak: bool,
    unslashed_participating_increments: u64,
    active_increments: u64,
) -> Result<(u64, u64), ArithError> {
    if is_participating {
        if is_in_inactivity_leak {
            return Ok((0, 0));
        }
        let reward_numerator = base_reward
            .safe_mul(weight)?
            .safe_mul(unslashed_participating_increments)?;
        let reward = reward_numerator.safe_div(active_increments.safe_mul(WEIGHT_DENOMINATOR)?)?;
        Ok((reward, 0))
    } else if is_head_flag {
        Ok((0, 0))
    } else {
        Ok((
            0,
            base_reward.safe_mul(weight)?.safe_div(WEIGHT_DENOMINATOR)?,
        ))
    }
}

/// Returns the inactivity penalty.
pub fn inactivity_penalty(
    effective_balance: u64,
    inactivity_score: u64,
    is_participating_target: bool,
    inactivity_score_bias: u64,
    inactivity_penalty_quotient: u64,
) -> Result<u64, ArithError> {
    if is_participating_target {
        return Ok(0);
    }
    let penalty_numerator = effective_balance.safe_mul(inactivity_score)?;
    let penalty_denominator = inactivity_score_bias.safe_mul(inactivity_penalty_quotient)?;
    penalty_numerator.safe_div(penalty_denominator)
}

/// Returns the balance of one validator after rewards and penalties.
///
/// The rewards are added first, then the penalties are subtracted once.
#[allow(clippy::too_many_arguments)]
pub fn new_balance_after_rewards(
    balance: u64,
    is_eligible: bool,
    base_reward: u64,
    effective_balance: u64,
    inactivity_score: u64,
    is_participating_source: bool,
    is_participating_target: bool,
    is_participating_head: bool,
    is_in_inactivity_leak: bool,
    source_increments: u64,
    target_increments: u64,
    head_increments: u64,
    active_increments: u64,
    inactivity_score_bias: u64,
    inactivity_penalty_quotient: u64,
) -> Result<u64, ArithError> {
    if !is_eligible {
        return Ok(balance);
    }
    let (source_reward, source_penalty) = flag_delta(
        base_reward,
        TIMELY_SOURCE_WEIGHT,
        is_participating_source,
        false,
        is_in_inactivity_leak,
        source_increments,
        active_increments,
    )?;
    let (target_reward, target_penalty) = flag_delta(
        base_reward,
        TIMELY_TARGET_WEIGHT,
        is_participating_target,
        false,
        is_in_inactivity_leak,
        target_increments,
        active_increments,
    )?;
    let (head_reward, head_penalty) = flag_delta(
        base_reward,
        TIMELY_HEAD_WEIGHT,
        is_participating_head,
        true,
        is_in_inactivity_leak,
        head_increments,
        active_increments,
    )?;
    let inactivity = inactivity_penalty(
        effective_balance,
        inactivity_score,
        is_participating_target,
        inactivity_score_bias,
        inactivity_penalty_quotient,
    )?;
    let rewards = source_reward
        .safe_add(target_reward)?
        .safe_add(head_reward)?;
    let penalties = source_penalty
        .safe_add(target_penalty)?
        .safe_add(head_penalty)?
        .safe_add(inactivity)?;
    Ok(balance.safe_add(rewards)?.saturating_sub(penalties))
}
