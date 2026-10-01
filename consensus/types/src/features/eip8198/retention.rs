//! The data retention window, which keeps its length in milliseconds across slot duration changes.

use safe_arith::{ArithError, SafeArith};

use crate::core::{ChainSpec, Epoch, EthSpec};
use crate::features::{Active, Eip8198};

/// `MIN_BLOB_DATA_RETENTION_MS`. Without the config key, the length in milliseconds of
/// `MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS` epochs at the genesis slot duration.
pub fn min_blob_data_retention_ms<E: EthSpec>(spec: &ChainSpec) -> Result<u64, ArithError> {
    match spec.features.min_blob_data_retention_ms {
        Some(retention_ms) => Ok(retention_ms),
        None => spec
            .min_epochs_for_data_column_sidecars_requests
            .safe_mul(E::slots_per_epoch())?
            .safe_mul(spec.get_slot_duration_ms(Epoch::new(0))),
    }
}

/// Spec: `compute_blob_data_retention_start_epoch`.
pub fn compute_blob_data_retention_start_epoch<E: EthSpec>(
    spec: &ChainSpec,
    epoch: Epoch,
) -> Result<Epoch, ArithError> {
    let schedule = spec.slot_duration_schedule();
    let current_start_ms = schedule.compute_time_at_slot_ms(
        E::slots_per_epoch(),
        0,
        epoch.start_slot(E::slots_per_epoch()),
    )?;
    let Some(window_start_ms) =
        current_start_ms.checked_sub(min_blob_data_retention_ms::<E>(spec)?)
    else {
        return Ok(Epoch::new(0));
    };
    Ok(schedule
        .compute_slot_at_time_ms(E::slots_per_epoch(), 0, window_start_ms)?
        .epoch(E::slots_per_epoch()))
}

/// The first epoch of the data column serve range: the start of the data retention window, but
/// not before the Fulu fork epoch.
pub fn min_epoch_data_availability_boundary<E: EthSpec>(
    spec: &ChainSpec,
    current_epoch: Epoch,
    _active: Active<Eip8198>,
) -> Option<Epoch> {
    let deneb_fork_epoch = spec.deneb_fork_epoch?;
    let blob_retention_epoch =
        current_epoch.saturating_sub(spec.min_epochs_for_blob_sidecars_requests);
    let data_column_retention_epoch =
        current_epoch.saturating_sub(spec.min_epochs_for_data_column_sidecars_requests);
    if let Some(fulu_fork_epoch) = spec.fulu_fork_epoch
        && blob_retention_epoch >= fulu_fork_epoch
    {
        Some(std::cmp::max(
            fulu_fork_epoch,
            compute_blob_data_retention_start_epoch::<E>(spec, current_epoch)
                .unwrap_or(data_column_retention_epoch),
        ))
    } else {
        Some(std::cmp::max(deneb_fork_epoch, blob_retention_epoch))
    }
}
