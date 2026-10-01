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

    type E = MinimalEthSpec;

    fn genesis_state(spec: &ChainSpec) -> BeaconState<E> {
        let keypairs = generate_deterministic_keypairs(16);
        InteropGenesisBuilder::<E>::new()
            .build_genesis_state(&keypairs, 0, Hash256::repeat_byte(0x42), spec)
            .unwrap()
    }

    fn assert_fork_follows_the_spec_across_epochs(spec: &ChainSpec) -> Fork {
        let mut state = genesis_state(spec);
        state.build_all_caches(spec).unwrap();
        assert_eq!(state.fork(), spec.fork_at_epoch(Epoch::new(0)));
        while state.slot() < Epoch::new(4).start_slot(E::slots_per_epoch()) {
            per_slot_processing(
                &mut state,
                None,
                GloasVerificationContext::FullVerification,
                spec,
            )
            .unwrap();
            assert_eq!(state.fork(), spec.fork_at_epoch(state.current_epoch()));
        }
        state.fork()
    }

    #[test]
    fn feature_fork_changes_only_the_fork() {
        let spec = ChainSpec::mainnet();
        let mut state = BeaconState::<MainnetEthSpec>::new(0, Eth1Data::default(), &spec);
        *state.slot_mut() = Epoch::new(10).start_slot(32);
        let mut expected = state.clone();
        *expected.fork_mut() = Fork {
            previous_version: state.fork().current_version,
            current_version: spec.features.heze_test_feature_fork_version,
            epoch: Epoch::new(10),
        };

        upgrade_to_feature_fork(&mut state, &spec, FeatureId::HezeTestFeature);

        assert_eq!(state, expected);
    }

    #[test]
    fn feature_forks_at_genesis() {
        let mut spec = ForkName::Heze.make_genesis_spec(E::default_spec());
        spec.features.heze_test_feature_fork_epoch = Some(Epoch::new(0));
        spec.features.gloas_test_feature_fork_epoch = Some(Epoch::new(0));

        let state = genesis_state(&spec);

        assert_eq!(
            state.fork(),
            Fork {
                previous_version: spec.features.heze_test_feature_fork_version,
                current_version: spec.features.gloas_test_feature_fork_version,
                epoch: Epoch::new(0),
            }
        );
        assert_eq!(state.fork(), spec.fork_at_epoch(Epoch::new(0)));
    }

    #[test]
    fn per_slot_processing_applies_the_feature_fork() {
        let mut spec = ForkName::Heze.make_genesis_spec(E::default_spec());
        spec.features.heze_test_feature_fork_epoch = Some(Epoch::new(2));
        let fork = assert_fork_follows_the_spec_across_epochs(&spec);
        assert_eq!(
            fork,
            Fork {
                previous_version: spec.heze_fork_version,
                current_version: spec.features.heze_test_feature_fork_version,
                epoch: Epoch::new(2),
            }
        );
    }

    #[test]
    fn per_slot_processing_applies_the_feature_fork_at_the_heze_fork_epoch() {
        let mut spec = ForkName::Gloas.make_genesis_spec(E::default_spec());
        spec.heze_fork_epoch = Some(Epoch::new(2));
        spec.features.heze_test_feature_fork_epoch = Some(Epoch::new(2));
        let fork = assert_fork_follows_the_spec_across_epochs(&spec);
        assert_eq!(
            fork,
            Fork {
                previous_version: spec.heze_fork_version,
                current_version: spec.features.heze_test_feature_fork_version,
                epoch: Epoch::new(2),
            }
        );
    }

    #[test]
    fn per_slot_processing_replaces_the_feature_fork_at_the_next_real_fork() {
        let mut spec = ForkName::Gloas.make_genesis_spec(E::default_spec());
        spec.heze_fork_epoch = Some(Epoch::new(3));
        spec.features.gloas_test_feature_fork_epoch = Some(Epoch::new(1));
        let fork = assert_fork_follows_the_spec_across_epochs(&spec);
        assert_eq!(
            fork,
            Fork {
                previous_version: spec.features.gloas_test_feature_fork_version,
                current_version: spec.heze_fork_version,
                epoch: Epoch::new(3),
            }
        );
    }
}
