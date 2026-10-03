//! Pure functions for committee ranges, shuffled positions and attestation duties in
//! `CommitteeCache`.
//!
//! Aeneas translates this module for the proofs in `consensus/types/proofs`, so it uses plain
//! integers, indexed `while` loops and no iterator adapters.

use safe_arith::{ArithError, SafeArith};

/// Returns `max(1, min(max_committees_per_slot, active_validator_count / slots_per_epoch / target_committee_size))`.
pub fn committee_count_per_slot(
    active_validator_count: usize,
    slots_per_epoch: usize,
    max_committees_per_slot: usize,
    target_committee_size: usize,
) -> Result<usize, ArithError> {
    Ok(std::cmp::max(
        1,
        std::cmp::min(
            max_committees_per_slot,
            active_validator_count
                .safe_div(slots_per_epoch)?
                .safe_div(target_committee_size)?,
        ),
    ))
}

/// Returns the position of committee `committee_index` at `slot` among all committees of its epoch.
pub fn committee_index_in_epoch(
    slot: usize,
    slots_per_epoch: usize,
    committees_per_slot: usize,
    committee_index: usize,
) -> Result<usize, ArithError> {
    slot.safe_rem(slots_per_epoch)?
        .safe_mul(committees_per_slot)?
        .safe_add(committee_index)
}

/// Returns the number of committees in one epoch.
pub fn epoch_committee_count(
    committees_per_slot: usize,
    slots_per_epoch: usize,
) -> Result<usize, ArithError> {
    committees_per_slot.safe_mul(slots_per_epoch)
}

/// Returns the `(start, end)` bounds of committee `index_in_epoch` in the shuffling, or `None` if it is out of range.
pub fn committee_range_in_epoch(
    epoch_committee_count: usize,
    index_in_epoch: usize,
    shuffling_len: usize,
) -> Result<Option<(usize, usize)>, ArithError> {
    if epoch_committee_count == 0 || index_in_epoch >= epoch_committee_count {
        return Ok(None);
    }

    let start = (shuffling_len.safe_mul(index_in_epoch))?.safe_div(epoch_committee_count)?;
    let end =
        (shuffling_len.safe_mul(index_in_epoch.safe_add(1)?))?.safe_div(epoch_committee_count)?;

    Ok(Some((start, end)))
}

/// Returns `positions` with `positions[shuffling[i]] = i + 1` and `0` elsewhere, or the first out-of-range validator index.
pub fn shuffling_positions(
    shuffling: &[usize],
    validator_count: usize,
) -> Result<Vec<usize>, usize> {
    let mut positions = vec![0; validator_count];
    let mut out_of_range = None;
    let mut i = 0;
    while i < shuffling.len() {
        if let Some(&v) = shuffling.get(i) {
            if let Some(p) = positions.get_mut(v) {
                *p = i.saturating_add(1);
            } else {
                out_of_range = Some(v);
                break;
            }
        }
        i = i.saturating_add(1);
    }
    match out_of_range {
        Some(v) => Err(v),
        None => Ok(positions),
    }
}

/// Returns `(committee index in epoch, committee_position, committee_len)` for the validator at shuffled `position`, or `None` if no committee holds it.
pub fn attestation_duty(
    position: usize,
    epoch_committee_count: usize,
    shuffling_len: usize,
) -> Result<Option<(usize, usize, usize)>, ArithError> {
    let mut found = Ok(None);
    let mut nth = 0;
    while nth < epoch_committee_count {
        match committee_range_in_epoch(epoch_committee_count, nth, shuffling_len) {
            Ok(Some((start, end))) => {
                if start <= position && end > position {
                    found = Ok(Some((nth, start, end)));
                    break;
                }
            }
            Ok(None) => {}
            Err(e) => {
                found = Err(e);
                break;
            }
        }
        nth = nth.saturating_add(1);
    }
    let Some((nth, start, end)) = found? else {
        return Ok(None);
    };
    Ok(Some((nth, position.safe_sub(start)?, end.safe_sub(start)?)))
}

/// Returns the committee shuffling of `active_validator_indices` with `H` as the hash function.
pub fn shuffling<H: swap_or_not_shuffle::ShuffleHash>(
    active_validator_indices: Vec<usize>,
    shuffle_round_count: u8,
    seed: &[u8],
) -> Option<Vec<usize>> {
    swap_or_not_shuffle::shuffle_list_with::<H>(
        active_validator_indices,
        shuffle_round_count,
        seed,
        false,
    )
}
