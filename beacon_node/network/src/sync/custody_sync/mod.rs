use std::collections::{HashMap, HashSet};

use lighthouse_network::PeerAction;
use types::{ColumnIndex, Hash256};

mod custody_network_context;
mod service;
mod sync_data_column;

#[derive(Debug)]
pub enum ColumnProcessResult {
    /// The request was completed successfully. It carries whether it contained a data column.
    Success {
        block_root: Hash256,
        column_index: ColumnIndex,
    },
    /// The custody batch processing failed. It carries whether the processing imported any data column.
    FaultyFailure {
        block_root: Hash256,
        column_index: ColumnIndex,
        penalty: PeerAction,
    },
    NonFaultyFailure,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::sync::custody_sync::custody_network_context::CustodySyncNetworkContext;

    use beacon_chain::test_utils::BeaconChainHarness;
    use bls::Hash256;
    use lighthouse_network::{NetworkConfig, NetworkGlobals, SyncInfo, SyncStatus};
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    use types::{Epoch, EthSpec, MinimalEthSpec};

    #[test]
    fn request_batches_should_not_loop_infinitely() {
        let harness = BeaconChainHarness::builder(MinimalEthSpec)
            .default_spec()
            .deterministic_keypairs(4)
            .fresh_ephemeral_store()
            .build();

        let beacon_chain = harness.chain.clone();
        let slots_per_epoch = MinimalEthSpec::slots_per_epoch();

        let network_globals = Arc::new(NetworkGlobals::new_test_globals(
            vec![],
            Arc::new(NetworkConfig::default()),
            beacon_chain.spec.clone(),
        ));

        {
            let mut rng = StdRng::seed_from_u64(0xDEADBEEF0BAD5EEDu64);
            let peer_id = network_globals
                .peers
                .write()
                .__add_connected_peer_testing_only(
                    true,
                    &beacon_chain.spec,
                    k256::ecdsa::SigningKey::random(&mut rng).into(),
                );

            // Simulate finalized epoch and head being 2 epochs ahead
            let finalized_epoch = Epoch::new(40);
            let head_epoch = finalized_epoch + 2;
            let head_slot = head_epoch.start_slot(slots_per_epoch) + 1;

            network_globals.peers.write().update_sync_status(
                &peer_id,
                SyncStatus::Synced {
                    info: SyncInfo {
                        head_slot,
                        head_root: Hash256::random(),
                        finalized_epoch,
                        finalized_root: Hash256::random(),
                        earliest_available_slot: None,
                    },
                },
            );
        }

        let mut network = CustodySyncNetworkContext::new_for_testing(
            beacon_chain.clone(),
            network_globals.clone(),
            harness.runtime.task_executor.clone(),
        );

        // let mut backfill = BackFillSync::new(beacon_chain, network_globals);
        // backfill.set_state(BackFillState::Syncing);

        // // if this ends up running into an infinite loop, the test will overflow the stack pretty quickly.
        // let _ = backfill.request_batches(&mut network);
    }
}
