use super::*;
use crate::per_block_processing::compute_timestamp_at_slot as production_compute_timestamp_at_slot;
use crate::{AllCaches, GloasVerificationContext, per_slot_processing};
use beacon_chain::test_utils::InteropGenesisBuilder;
use types::features::eip8198::get_base_reward_for_epoch;
use types::test_utils::generate_deterministic_keypairs;
use types::{Hash256, MinimalEthSpec, SlotDurationSchedule, SlotDurationScheduleEntry};

type E = MinimalEthSpec;

const FORK_EPOCH: u64 = 2;

fn heze_spec() -> ChainSpec {
    ForkName::Heze.make_genesis_spec(E::default_spec())
}

/// The slot duration halves from 6s to 3s at the EIP-8198 fork epoch.
fn eip8198_spec() -> ChainSpec {
    let mut spec = heze_spec();
    spec.features.eip8198_fork_epoch = Some(Epoch::new(FORK_EPOCH));
    spec.features.slot_duration_schedule = Some(SlotDurationSchedule::new(vec![
        SlotDurationScheduleEntry {
            epoch: Epoch::new(0),
            slot_duration_ms: 6000,
        },
        SlotDurationScheduleEntry {
            epoch: Epoch::new(FORK_EPOCH),
            slot_duration_ms: 3000,
        },
    ]));
    spec
}

fn state_at_epoch(spec: &ChainSpec, epoch: u64) -> BeaconState<E> {
    let keypairs = generate_deterministic_keypairs(16);
    let mut state = InteropGenesisBuilder::<E>::new()
        .build_genesis_state(&keypairs, 0, Hash256::repeat_byte(0x42), spec)
        .expect("genesis state");
    state.build_all_caches(spec).expect("caches");
    while state.slot() < Epoch::new(epoch).start_slot(E::slots_per_epoch()) {
        per_slot_processing(
            &mut state,
            None,
            GloasVerificationContext::FullVerification,
            spec,
        )
        .expect("slot processing");
    }
    state.build_all_caches(spec).expect("caches");
    state
}

fn active(spec: &ChainSpec, epoch: u64) -> Active<Eip8198> {
    spec.feature_enabled::<Eip8198>(Epoch::new(epoch))
        .expect("EIP-8198 is active")
}

#[test]
fn base_reward_per_increment_scales_with_the_slot_duration() {
    let spec = eip8198_spec();
    let total_active_balance = 16 * spec.max_effective_balance;
    let unscaled = spec.effective_balance_increment * spec.base_reward_factor
        / total_active_balance.integer_sqrt();
    let scaled = spec.effective_balance_increment * spec.base_reward_factor
        / 2
        / total_active_balance.integer_sqrt();

    let at = |epoch: u64| {
        BaseRewardPerIncrement::new(total_active_balance, Epoch::new(epoch), &spec)
            .expect("base reward per increment")
            .as_u64()
    };
    assert_eq!(at(FORK_EPOCH - 1), unscaled);
    assert_eq!(at(FORK_EPOCH), scaled);
    assert_eq!(
        BaseRewardPerIncrement::new(total_active_balance, Epoch::new(FORK_EPOCH), &heze_spec())
            .expect("base reward per increment")
            .as_u64(),
        unscaled
    );
}

#[test]
fn base_rewards_across_the_slot_duration_change() {
    let spec = eip8198_spec();
    let state = state_at_epoch(&spec, FORK_EPOCH);
    let on = active(&spec, FORK_EPOCH);
    let epoch_cache = state.epoch_cache();

    let current = get_base_reward_for_epoch(epoch_cache, 0, Epoch::new(FORK_EPOCH), on)
        .expect("current epoch base reward");
    let previous = get_base_reward_for_epoch(epoch_cache, 0, Epoch::new(FORK_EPOCH - 1), on)
        .expect("previous epoch base reward");
    assert_eq!(current, state.get_base_reward(0).expect("base reward"));
    assert_eq!(
        previous,
        state_at_epoch(&spec, FORK_EPOCH - 1)
            .get_base_reward(0)
            .expect("base reward")
    );
    assert!(current < previous);

    for epoch in [FORK_EPOCH - 2, FORK_EPOCH + 1] {
        assert!(matches!(
            get_base_reward_for_epoch(epoch_cache, 0, Epoch::new(epoch), on),
            Err(EpochCacheError::IncorrectEpoch { .. })
        ));
    }
}

#[test]
fn epoch_cache_has_no_previous_epoch_base_rewards_without_the_feature() {
    let spec = heze_spec();
    let state = state_at_epoch(&spec, FORK_EPOCH);
    assert_eq!(state.epoch_cache().previous_epoch_base_rewards(), Ok(None));
}

#[test]
fn churn_limits_scale_with_the_slot_duration() {
    let spec = eip8198_spec();
    let without = heze_spec();
    let state = state_at_epoch(&spec, FORK_EPOCH);
    let round = |churn: u64| churn - churn % spec.effective_balance_increment;

    let balance_churn = state
        .get_balance_churn_limit(&without)
        .expect("balance churn");
    assert_eq!(
        state.get_balance_churn_limit(&spec),
        Ok(round(balance_churn / 2))
    );

    let activation_churn = std::cmp::min(
        spec.max_per_epoch_activation_churn_limit_gloas,
        balance_churn,
    );
    assert_eq!(
        state.get_activation_exit_churn_limit(&spec),
        Ok(round(activation_churn / 2))
    );

    let consolidation_churn = state.get_total_active_balance().expect("total balance")
        / spec.consolidation_churn_limit_quotient;
    assert_eq!(
        state.get_consolidation_churn_limit(&spec),
        Ok(round(consolidation_churn / 2))
    );
}

#[test]
fn inactivity_penalty_scales_with_the_square_of_the_slot_duration() {
    let spec = eip8198_spec();
    let on = active(&spec, FORK_EPOCH);
    let unscaled =
        spec.inactivity_score_bias * spec.inactivity_penalty_quotient_for_fork(ForkName::Heze);
    assert_eq!(
        inactivity_penalty_denominator(ForkName::Heze, Epoch::new(FORK_EPOCH - 1), &spec, on),
        Ok(unscaled)
    );
    assert_eq!(
        inactivity_penalty_denominator(ForkName::Heze, Epoch::new(FORK_EPOCH), &spec, on),
        Ok(unscaled * 4)
    );
}

#[test]
fn payload_timestamps_follow_the_slot_duration_schedule() {
    let spec = eip8198_spec();
    let mut state = BeaconState::<E>::new(1_000, Default::default(), &spec);
    *state.genesis_time_mut() = 1_000;
    let fork_slot = Epoch::new(FORK_EPOCH).start_slot(E::slots_per_epoch());
    let fork_time = 1_000 + fork_slot.as_u64() * 6;

    for (slot, expected) in [
        (fork_slot - 1, fork_time - 6),
        (fork_slot, fork_time),
        (fork_slot + 1, fork_time + 3),
        (fork_slot + 10, fork_time + 30),
    ] {
        assert_eq!(
            production_compute_timestamp_at_slot(&state, slot, &spec),
            Ok(expected),
            "slot {slot}"
        );
    }
    assert_eq!(
        production_compute_timestamp_at_slot(&state, fork_slot + 1, &heze_spec()),
        Ok(fork_time + 6)
    );
}
