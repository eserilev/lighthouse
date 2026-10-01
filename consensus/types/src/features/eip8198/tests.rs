use super::*;
use crate::core::{Config, EthSpec, MainnetEthSpec, Slot};
use crate::features::{Eip8198, FeatureConfig};
use crate::fork::ForkName;

type E = MainnetEthSpec;

fn schedule(entries: &[(u64, u64)]) -> SlotDurationSchedule {
    SlotDurationSchedule::new(
        entries
            .iter()
            .map(|&(epoch, slot_duration_ms)| SlotDurationScheduleEntry {
                epoch: Epoch::new(epoch),
                slot_duration_ms,
            })
            .collect(),
    )
}

fn heze_spec() -> ChainSpec {
    let mut spec = ForkName::Gloas.make_genesis_spec(ChainSpec::mainnet());
    spec.heze_fork_epoch = Some(Epoch::new(5));
    spec
}

/// Load a spec from the config of `heze_spec` and the EIP-8198 config keys.
fn spec_from_config(
    eip8198_fork_epoch: Option<u64>,
    entries: Option<&[(u64, u64)]>,
) -> Option<ChainSpec> {
    let base = heze_spec();
    let config = Config::from_chain_spec::<E>(&base);
    let mut features = base.features.clone();
    features.eip8198_fork_epoch = eip8198_fork_epoch.map(Epoch::new);
    features.slot_duration_schedule = entries.map(schedule);
    let base = E::default_spec().with_feature_config(&FeatureConfig::from_spec(&features));
    config.apply_to_chain_spec::<E>(&base)
}

#[test]
fn gate() {
    let mut spec = heze_spec();
    assert!(spec.feature_enabled::<Eip8198>(Epoch::new(10)).is_none());

    spec.features.eip8198_fork_epoch = Some(Epoch::new(10));
    assert!(spec.feature_enabled::<Eip8198>(Epoch::new(9)).is_none());
    assert!(spec.feature_enabled::<Eip8198>(Epoch::new(10)).is_some());
    assert_eq!(
        spec.scheduled_features(),
        vec![(FeatureId::Eip8198, Epoch::new(10))]
    );
}

#[test]
fn validate_features_needs_eip8198_at_or_after_heze() {
    let mut spec = heze_spec();
    spec.features.eip8198_fork_epoch = Some(Epoch::new(5));
    assert_eq!(spec.validate_features(), Ok(()));

    spec.features.eip8198_fork_epoch = Some(Epoch::new(4));
    let error = spec.validate_features().expect_err("EIP-8198 before Heze");
    assert!(error.contains("is before the"), "{error}");
}

#[test]
fn slot_duration_schedule_config_key() {
    let config: FeatureConfig = yaml_serde::from_str(
        "EIP8198_FORK_EPOCH: 10\nSLOT_DURATION_SCHEDULE:\n  - EPOCH: 0\n    SLOT_DURATION_MS: 12000\n  - EPOCH: 10\n    SLOT_DURATION_MS: 6000\n",
    )
    .expect("valid feature config");
    let spec = heze_spec().with_feature_config(&config);
    assert_eq!(
        spec.features.slot_duration_schedule,
        Some(schedule(&[(0, 12000), (10, 6000)]))
    );

    let yaml = yaml_serde::to_string(&FeatureConfig::from_spec(&spec.features))
        .expect("serialize the feature config");
    assert!(yaml.contains("SLOT_DURATION_SCHEDULE"), "{yaml}");

    let yaml = yaml_serde::to_string(&FeatureConfig::from_spec(&heze_spec().features))
        .expect("serialize the feature config");
    assert!(!yaml.contains("SLOT_DURATION_SCHEDULE"), "{yaml}");
}

#[test]
fn slot_duration_changes_at_the_fork_epoch() {
    let spec = spec_from_config(Some(10), Some(&[(0, 12000), (10, 6000)]))
        .expect("change at the EIP-8198 fork epoch");
    assert_eq!(spec.get_slot_duration_ms(Epoch::new(9)), 12000);
    assert_eq!(spec.get_slot_duration_ms(Epoch::new(10)), 6000);
    assert_eq!(
        spec.slot_duration_schedule(),
        schedule(&[(0, 12000), (10, 6000)])
    );
    assert_eq!(spec.validate_features(), Ok(()));
}

#[test]
fn invalid_schedules_are_rejected() {
    for entries in [
        &[(0, 12000), (11, 6000)][..],
        &[(0, 6000), (10, 6000)],
        &[(0, 12000), (10, 6500)],
        &[(10, 6000)],
    ] {
        assert!(
            spec_from_config(Some(10), Some(entries)).is_none(),
            "{entries:?}"
        );
    }

    let schedule = schedule(&[(0, 12000), (10, 6000)]);
    assert_eq!(
        validate_slot_duration_changes(schedule.as_vec(), Some(Epoch::new(10))),
        Ok(())
    );
    let error = validate_slot_duration_changes(schedule.as_vec(), None)
        .expect_err("change without the fork");
    assert!(
        error.contains("is not at the EIP-8198 fork epoch"),
        "{error}"
    );
}

#[test]
fn slot_duration_schedule_rejects_invalid_schedules() {
    let invalid = [
        ("does not match SLOT_DURATION_MS", 12000, &[(0, 10000)][..]),
        ("not a positive multiple of 1000", 12500, &[(0, 12500)]),
        ("not a positive multiple of 1000", 0, &[(0, 0)]),
        (
            "the first entry must be at the genesis epoch",
            12000,
            &[(5, 12000)],
        ),
        (
            "multiple entries for epoch",
            12000,
            &[(0, 12000), (0, 12000)],
        ),
        (
            "does not fit in a u64",
            12000,
            &[(0, 12000), (u64::MAX, 6000)],
        ),
    ];
    for (expected, genesis_slot_duration_ms, entries) in invalid {
        let error = validate_schedule(
            schedule(entries).as_vec(),
            genesis_slot_duration_ms,
            E::slots_per_epoch(),
        )
        .expect_err(expected);
        assert!(
            error.contains(expected),
            "expected {expected:?}, got {error:?}"
        );
    }
}

#[test]
fn schedule_is_unused_without_the_fork() {
    let spec = spec_from_config(None, Some(&[(0, 12000), (10, 6000)]))
        .expect("the schedule is not checked without the fork");
    assert_eq!(spec.get_slot_duration_ms(Epoch::new(10)), 12000);
    assert_eq!(spec.slot_duration_schedule(), schedule(&[(0, 12000)]));

    let spec = spec_from_config(Some(10), None).expect("fork without a schedule");
    assert_eq!(spec.get_slot_duration_ms(Epoch::new(10)), 12000);
    assert_eq!(spec.slot_duration_schedule(), schedule(&[(0, 12000)]));
}

#[test]
fn slot_time_mapping_across_slot_duration_changes() {
    let mut spec = heze_spec();
    spec.features.eip8198_fork_epoch = Some(Epoch::new(10));
    spec.features.slot_duration_schedule = Some(schedule(&[(0, 12000), (10, 6000)]));
    let schedule = spec.slot_duration_schedule();
    let genesis_time_ms = 1_000_000;
    let time_at = |slot: u64| {
        schedule
            .compute_time_at_slot_ms(E::slots_per_epoch(), genesis_time_ms, Slot::new(slot))
            .expect("time of slot")
    };
    let slot_at = |time_ms: u64| {
        schedule
            .compute_slot_at_time_ms(E::slots_per_epoch(), genesis_time_ms, time_ms)
            .expect("slot at time")
            .as_u64()
    };

    assert_eq!(time_at(320), genesis_time_ms + 320 * 12000);
    assert_eq!(time_at(321), time_at(320) + 6000);
    for slot in 0..1024 {
        let slot_duration_ms = spec.get_slot_duration_ms(Epoch::new(slot / 32));
        assert_eq!(
            time_at(slot + 1) - time_at(slot),
            slot_duration_ms,
            "slot {slot}"
        );
        assert_eq!(slot_at(time_at(slot)), slot, "slot {slot}");
        assert_eq!(slot_at(time_at(slot + 1) - 1), slot, "slot {slot}");
    }
}
