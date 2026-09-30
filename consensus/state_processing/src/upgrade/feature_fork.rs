use types::features::FeatureId;
use types::{BeaconState, ChainSpec, EthSpec, Fork};

/// Apply the fork of an experimental feature, which only changes the fork version.
pub fn upgrade_to_feature_fork<E: EthSpec>(
    state: &mut BeaconState<E>,
    spec: &ChainSpec,
    feature: FeatureId,
) {
    let fork = Fork {
        previous_version: state.fork().current_version,
        current_version: spec.features.fork_version(feature),
        epoch: state.current_epoch(),
    };
    *state.fork_mut() = fork;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AllCaches, GloasVerificationContext, per_slot_processing};
    use beacon_chain::test_utils::InteropGenesisBuilder;
    use types::test_utils::generate_deterministic_keypairs;
    use types::{Epoch, Eth1Data, ForkName, Hash256, MainnetEthSpec, MinimalEthSpec};

    #[test]
    fn feature_fork_changes_only_the_fork() {
        let spec = ChainSpec::mainnet();
        let mut state = BeaconState::<MainnetEthSpec>::new(0, Eth1Data::default(), &spec);
        *state.slot_mut() = Epoch::new(10).start_slot(32);
        let mut expected = state.clone();
        *expected.fork_mut() = Fork {
            previous_version: state.fork().current_version,
            current_version: spec.features.eip8198_fork_version,
            epoch: Epoch::new(10),
        };

        upgrade_to_feature_fork(&mut state, &spec, FeatureId::Eip8198);

        assert_eq!(state, expected);
    }

    fn assert_fork_follows_the_spec_across_epochs(spec: &ChainSpec) {
        type E = MinimalEthSpec;
        let keypairs = generate_deterministic_keypairs(16);
        let mut state = InteropGenesisBuilder::<E>::new()
            .build_genesis_state(&keypairs, 0, Hash256::repeat_byte(0x42), spec)
            .unwrap();
        state.build_all_caches(spec).unwrap();
        while state.slot() < Epoch::new(3).start_slot(E::slots_per_epoch()) {
            per_slot_processing(
                &mut state,
                None,
                GloasVerificationContext::FullVerification,
                spec,
            )
            .unwrap();
            assert_eq!(state.fork(), spec.fork_at_epoch(state.current_epoch()));
        }
        assert_eq!(
            state.fork().current_version,
            spec.features.eip8198_fork_version
        );
    }

    #[test]
    fn per_slot_processing_applies_the_feature_fork() {
        let mut spec = ForkName::Heze.make_genesis_spec(MinimalEthSpec::default_spec());
        spec.features.eip8198_fork_epoch = Some(Epoch::new(2));
        assert_fork_follows_the_spec_across_epochs(&spec);
    }

    #[test]
    fn per_slot_processing_applies_the_feature_fork_at_the_heze_fork_epoch() {
        let mut spec = ForkName::Gloas.make_genesis_spec(MinimalEthSpec::default_spec());
        spec.heze_fork_epoch = Some(Epoch::new(2));
        spec.features.eip8198_fork_epoch = Some(Epoch::new(2));
        assert_fork_follows_the_spec_across_epochs(&spec);
    }
}
