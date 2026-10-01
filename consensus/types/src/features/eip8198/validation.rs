//! The checks of `SLOT_DURATION_SCHEDULE`, on the entries alone.
//!
//! Aeneas proofs in `consensus/types/proofs` cover this module. Keep it free of `ChainSpec`.

use crate::core::{Epoch, SlotDurationScheduleEntry};

pub(super) fn validate_schedule(
    schedule: &[SlotDurationScheduleEntry],
    genesis_slot_duration_ms: u64,
    slots_per_epoch: u64,
) -> Result<(), String> {
    if schedule.is_empty() {
        return Ok(());
    }
    let mut duplicate_epoch = None;
    let mut previous_epoch = None;
    let mut index = 0;
    while let Some(entry) = schedule.get(index) {
        if let Some(epoch) = previous_epoch
            && epoch == entry.epoch
        {
            duplicate_epoch = Some(epoch);
            break;
        }
        previous_epoch = Some(entry.epoch);
        index = index.saturating_add(1);
    }
    if let Some(epoch) = duplicate_epoch {
        return Err(format!("multiple entries for epoch {}", epoch));
    }
    let mut invalid_duration = None;
    let mut index = 0;
    while let Some(entry) = schedule.get(index) {
        if entry.slot_duration_ms == 0 || entry.slot_duration_ms % 1000 != 0 {
            invalid_duration = Some((entry.slot_duration_ms, entry.epoch));
            break;
        }
        index = index.saturating_add(1);
    }
    if let Some((slot_duration_ms, epoch)) = invalid_duration {
        return Err(format!(
            "slot duration {} at epoch {} is not a positive multiple of 1000",
            slot_duration_ms, epoch
        ));
    }
    let mut invalid_start_slot = None;
    let mut index = 0;
    while let Some(entry) = schedule.get(index) {
        if entry.epoch.as_u64().checked_mul(slots_per_epoch).is_none() {
            invalid_start_slot = Some(entry.epoch);
            break;
        }
        index = index.saturating_add(1);
    }
    if let Some(epoch) = invalid_start_slot {
        return Err(format!(
            "the start slot of epoch {} does not fit in a u64",
            epoch
        ));
    }
    match schedule.get(schedule.len().saturating_sub(1)) {
        Some(entry) if entry.epoch == Epoch::new(0) => {
            if entry.slot_duration_ms != genesis_slot_duration_ms {
                return Err(format!(
                    "genesis slot duration {} does not match SLOT_DURATION_MS {}",
                    entry.slot_duration_ms, genesis_slot_duration_ms
                ));
            }
        }
        _ => return Err("the first entry must be at the genesis epoch".to_string()),
    }
    Ok(())
}

pub(super) fn validate_slot_duration_changes(
    schedule: &[SlotDurationScheduleEntry],
    eip8198_fork_epoch: Option<Epoch>,
) -> Result<(), String> {
    let mut unscheduled_change = None;
    let mut index = 0;
    while let Some(entry) = schedule.get(index) {
        if entry.epoch != Epoch::new(0)
            && !eip8198_fork_epoch.is_some_and(|fork_epoch| fork_epoch == entry.epoch)
        {
            unscheduled_change = Some(entry.epoch);
            break;
        }
        index = index.saturating_add(1);
    }
    if let Some(epoch) = unscheduled_change {
        return Err(format!(
            "the slot duration change at epoch {} is not at the EIP-8198 fork epoch",
            epoch
        ));
    }
    Ok(())
}
