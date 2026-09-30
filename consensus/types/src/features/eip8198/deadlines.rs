//! Deadlines within a slot, from the slot duration at the EIP-8198 fork epoch.

use std::time::Duration;

use safe_arith::{ArithError, SafeArith};

use crate::consts::bellatrix::BASIS_POINTS;
use crate::core::{ChainSpec, EthSpec, Slot};
use crate::features::{Active, Eip8198, FeatureId};

/// The slot duration at the EIP-8198 fork epoch.
fn fork_slot_duration_ms(spec: &ChainSpec) -> u64 {
    let fork_epoch = spec
        .feature_fork_epoch(FeatureId::Eip8198)
        .unwrap_or_default();
    spec.get_slot_duration_ms(fork_epoch)
}

/// Spec: `get_slot_component_duration_ms`, with the slot duration at the EIP-8198 fork epoch.
fn slot_component_duration(
    spec: &ChainSpec,
    component_basis_points: u64,
) -> Result<Duration, ArithError> {
    Ok(Duration::from_millis(
        component_basis_points
            .safe_mul(fork_slot_duration_ms(spec))?
            .safe_div(BASIS_POINTS)?,
    ))
}

fn deadline(spec: &ChainSpec, component_basis_points: u64) -> Duration {
    Duration::from_millis(
        component_basis_points
            .saturating_mul(fork_slot_duration_ms(spec))
            .saturating_div(BASIS_POINTS),
    )
}

/// Spec: `get_attestation_due_ms`.
pub fn get_attestation_due<E: EthSpec>(
    spec: &ChainSpec,
    _slot: Slot,
    _active: Active<Eip8198>,
) -> Duration {
    deadline(spec, spec.attestation_due_bps_gloas)
}

/// Spec: `get_payload_due_ms`.
pub fn get_payload_due<E: EthSpec>(
    spec: &ChainSpec,
    _slot: Slot,
    _active: Active<Eip8198>,
) -> Duration {
    deadline(spec, spec.payload_due_bps)
}

/// Spec: `get_payload_attestation_due_ms`.
pub fn get_payload_attestation_due<E: EthSpec>(
    spec: &ChainSpec,
    _slot: Slot,
    _active: Active<Eip8198>,
) -> Duration {
    deadline(spec, spec.payload_attestation_due_bps)
}

/// Spec: `get_aggregate_attestation_due_ms`.
pub fn get_aggregate_attestation_due<E: EthSpec>(
    spec: &ChainSpec,
    _slot: Slot,
    _active: Active<Eip8198>,
) -> Duration {
    deadline(spec, spec.aggregate_due_bps_gloas)
}

/// Spec: `get_contribution_due_ms`.
pub fn get_contribution_message_due<E: EthSpec>(
    spec: &ChainSpec,
    _slot: Slot,
    _active: Active<Eip8198>,
) -> Duration {
    deadline(spec, spec.contribution_due_bps_gloas)
}

/// Spec: `get_sync_message_due_ms`.
pub fn get_sync_message_due<E: EthSpec>(
    spec: &ChainSpec,
    _slot: Slot,
    _active: Active<Eip8198>,
) -> Duration {
    deadline(spec, spec.sync_message_due_bps_gloas)
}

/// Spec: `get_slot_component_duration_ms`.
pub fn compute_slot_component_duration_at<E: EthSpec>(
    spec: &ChainSpec,
    component_basis_points: u64,
    _slot: Slot,
    _active: Active<Eip8198>,
) -> Result<Duration, ArithError> {
    slot_component_duration(spec, component_basis_points)
}
