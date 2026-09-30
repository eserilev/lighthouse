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
    use types::{Epoch, Eth1Data, Hash256, MainnetEthSpec};

    #[test]
    fn feature_fork_changes_only_the_fork() {
        let spec = ChainSpec::mainnet();
        let mut state = BeaconState::<MainnetEthSpec>::new(0, Eth1Data::default(), &spec);
        *state.slot_mut() = Epoch::new(10).start_slot(32);
        let before = state.fork();
        let root_before = Hash256::from(state.latest_block_header().state_root);

        upgrade_to_feature_fork(&mut state, &spec, FeatureId::Eip8198);

        assert_eq!(
            state.fork(),
            Fork {
                previous_version: before.current_version,
                current_version: spec.features.eip8198_fork_version,
                epoch: Epoch::new(10),
            }
        );
        assert_eq!(
            Hash256::from(state.latest_block_header().state_root),
            root_before
        );
    }
}
