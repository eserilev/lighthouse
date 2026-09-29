use types::{BeaconState, ChainSpec, EthSpec, Fork};

/// Apply the EIP-8198 fork, which only changes the fork version.
pub fn upgrade_to_eip8198<E: EthSpec>(state: &mut BeaconState<E>, spec: &ChainSpec) {
    let fork = Fork {
        previous_version: state.fork().current_version,
        current_version: spec.eip8198_fork_version,
        epoch: state.current_epoch(),
    };
    *state.fork_mut() = fork;
}
