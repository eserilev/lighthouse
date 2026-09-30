//! EIP-8198: a slot duration schedule, so that the slot duration can change at the EIP-8198 fork.
//!
//! The schedule type and its time functions are generic (`crate::core::SlotDurationSchedule`).
//! This feature owns the `SLOT_DURATION_SCHEDULE` config key, the rule that the slot duration
//! changes only at the EIP-8198 fork epoch, and the spec functions that scale with the slot
//! duration.

use crate::core::{ChainSpec, SlotDurationSchedule};
use crate::features::FeatureId;

mod deadlines;
mod rescaling;
mod retention;
#[cfg(test)]
mod tests;
mod validation;

pub use deadlines::*;
pub use rescaling::*;
pub use retention::*;

use validation::{validate_schedule, validate_slot_duration_changes};

/// The `SLOT_DURATION_SCHEDULE` of the config, if the config has one.
pub fn slot_duration_schedule(spec: &ChainSpec) -> Option<&SlotDurationSchedule> {
    spec.features.slot_duration_schedule.as_ref()
}

/// Check the `SLOT_DURATION_SCHEDULE` of the config.
///
/// The genesis entry must match `SLOT_DURATION_MS`, each duration must be a positive multiple of
/// 1000, the start slot of each entry must fit in a `u64`, and each change after genesis must be
/// at the EIP-8198 fork epoch.
pub fn validate_slot_duration_schedule(
    spec: &ChainSpec,
    slots_per_epoch: u64,
) -> Result<(), String> {
    let Some(schedule) = slot_duration_schedule(spec) else {
        return Ok(());
    };
    let genesis_slot_duration_ms = u64::try_from(spec.get_slot_duration().as_millis())
        .map_err(|_| "SLOT_DURATION_MS does not fit in a u64".to_string())?;
    validate_schedule(schedule.as_vec(), genesis_slot_duration_ms, slots_per_epoch)?;
    validate_slot_duration_changes(
        schedule.as_vec(),
        spec.feature_fork_epoch(FeatureId::Eip8198),
    )
}
