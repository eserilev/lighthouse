use std::{collections::HashSet, sync::Arc};

use beacon_chain::{BeaconChain, BeaconChainTypes};
use lighthouse_network::{
    service::api_types::{DataColumnsByRootRequestId, Id},
    types::BackFillState,
    NetworkGlobals, PeerAction, PeerId,
};
use tracing::{debug, info, instrument};
use types::{ColumnIndex, DataColumnSidecar, Epoch, Hash256, Slot};

use crate::sync::custody_sync::sync_data_column::Error as SyncDataColumnError;
use crate::sync::{
    backfill_sync::{BackFillError, SyncStart},
    custody_sync::{
        custody_network_context::CustodySyncNetworkContext,
        sync_data_column::{PeerGroup, SyncDataColumn},
        ColumnProcessResult,
    },
    network_context::RpcResponseError,
};
pub enum SyncColumnResult {
    Done {
        block_root: Hash256,
        slot: Slot,
        column_index: ColumnIndex,
    },
    Wait,
}

pub struct CustodySync<T: BeaconChainTypes> {
    sync_data_column: SyncDataColumn<T>,

    /// When a custody sync fails, we keep track of whether a new fully synced peer has joined.
    /// This signifies that we are able to attempt to restart a failed chain.
    restart_failed_sync: bool,

    /// Reference to the beacon chain to obtain initial starting points for custody sync.
    beacon_chain: Arc<BeaconChain<T>>,

    /// Reference to the network globals in order to obtain valid peers to backfill columns from
    /// (i.e synced peers).
    network_globals: Arc<NetworkGlobals<T::EthSpec>>,
}

impl<T: BeaconChainTypes> CustodySync<T> {
    #[instrument(parent = None,
        level = "info",
        name = "custody_backfill_sync",
        skip_all
    )]
    pub fn new(
        beacon_chain: Arc<BeaconChain<T>>,
        network_globals: Arc<NetworkGlobals<T::EthSpec>>,
    ) -> Self {
        // Determine if backfill is enabled or not.
        // If, for some reason a backfill has already been completed (or we've used a trusted
        // genesis root) then backfill has been completed.
        let anchor_info = beacon_chain.store.get_anchor_info();
        let state = if anchor_info.block_backfill_complete(beacon_chain.genesis_backfill_slot) {
            BackFillState::Completed
        } else {
            BackFillState::Paused
        };

        let custody_sync = Self {
            // TODO(custody-sync) none of this is correct
            sync_data_column: SyncDataColumn::new(
                Epoch::new(0),
                anchor_info.oldest_block_parent,
                anchor_info.oldest_block_slot,
                0,
                &[],
            ),
            restart_failed_sync: false,
            beacon_chain,
            network_globals,
        };

        // Update the global network state with the current backfill state.
        custody_sync.set_state(state);
        custody_sync
    }

    /// Pauses the backfill sync if it's currently syncing.
    #[instrument(parent = None,
        level = "info",
        fields(service = "custody_backfill_sync"),
        name = "custody_backfill_sync",
        skip_all
    )]
    pub fn pause(&mut self) {
        if let BackFillState::Syncing = self.state() {
            debug!("Custody sync paused");
            self.set_state(BackFillState::Paused);
        }
    }

    /// Starts or resumes syncing.
    ///
    /// If resuming is successful, reports back the current syncing metrics.
    #[must_use = "A failure here indicates the custody sync has failed and the global sync state should be updated"]
    #[instrument(parent = None,
        level = "info",
        fields(service = "backfill_sync"),
        name = "backfill_sync",
        skip_all
    )]
    pub fn start(
        &mut self,
        network: &mut CustodySyncNetworkContext<T>,
    ) -> Result<SyncStart, BackFillError> {
        match self.state() {
            BackFillState::Syncing => {} // already syncing ignore.
            BackFillState::Paused => {
                if self.sync_data_column.peer_count() == 0 {
                    // If there are peers to resume with, begin the resume.
                    debug!("Resuming custody sync");
                    self.set_state(BackFillState::Syncing);
                    self.continue_syncing_columns(network);
                } else {
                    return Ok(SyncStart::NotSyncing);
                }
            }
            BackFillState::Failed => {
                // Attempt to recover from a failed sync. All local variables should be reset and
                // cleared already for a fresh start.
                // We only attempt to restart a failed backfill sync if a new synced peer has been
                // added.
                if !self.restart_failed_sync {
                    return Ok(SyncStart::NotSyncing);
                }

                self.set_state(BackFillState::Syncing);

                debug!("Resuming a failed backfill sync");

                // begin requesting blocks from the peer pool, until all peers are exhausted.
                self.continue_syncing_columns(network);
            }
            BackFillState::Completed => return Ok(SyncStart::NotSyncing),
        }

        // TODO(custody-sync)
        Ok(SyncStart::Syncing {
            completed: todo!(),
            remaining: todo!(),
        })
    }

    /// A fully synced peer has joined us.
    /// If we are in a failed state, update a local variable to indicate we are able to restart
    /// the failed sync on the next attempt.
    #[instrument(parent = None,
        level = "info",
        fields(service = "custody_backfill_sync"),
        name = "custody_backfill_sync",
        skip_all
    )]
    pub fn fully_synced_peer_joined(&mut self) {
        if matches!(self.state(), BackFillState::Failed) {
            self.restart_failed_sync = true;
        }
    }

    pub fn add_peer(&mut self, peer_id: PeerId) {
        self.sync_data_column.add_peer(peer_id);
    }

    pub fn peer_disconnected(&mut self, peer_id: &PeerId) {
        self.sync_data_column.remove_peer(peer_id);

        if self.sync_data_column.peer_count() == 0 && self.state() == BackFillState::Syncing {
            info!(
                "reason" = "insufficient_synced_peers",
                "Custody sync paused"
            );
            self.set_state(BackFillState::Paused);
        }
    }

    pub fn on_data_column_download_result(
        &mut self,
        req_id: DataColumnsByRootRequestId,
        result: Result<(Arc<DataColumnSidecar<T::EthSpec>>, PeerGroup), RpcResponseError>,
        cx: &mut CustodySyncNetworkContext<T>,
    ) {
        if let Err(e) = self.sync_data_column.on_download_result(req_id, result, cx) {
            self.handle_outcome(Err(e), cx);
        }
    }

    pub fn on_data_column_process_result(
        &mut self,
        _id: Id,
        result: ColumnProcessResult,
        cx: &mut CustodySyncNetworkContext<T>,
    ) {
        let outcome = self.sync_data_column.on_process_result(result, cx);
        self.handle_outcome(outcome, cx);
    }

    fn continue_syncing_columns(&mut self, cx: &mut CustodySyncNetworkContext<T>) {
        // TODO(tree-sync): only ok to import the newest block
        let ok_to_import = true;
        let outcome = self.sync_data_column.continue_request(cx);
        self.handle_outcome(outcome.map(|_| SyncColumnResult::Wait), cx);
    }

    fn handle_outcome(
        &mut self,
        result: Result<SyncColumnResult, SyncDataColumnError>,
        cx: &mut CustodySyncNetworkContext<T>,
    ) {
        match result {
            Ok(SyncColumnResult::Done {
                block_root,
                slot,
                column_index,
            }) => {
                if self.is_complete(slot) {
                    info!("Custody sync completed");
                    self.set_state(BackFillState::Completed);
                } else {
                    let peers = self.sync_data_column.clone_peers();
                    // TODO(custody-sync) this is wrong
                    self.sync_data_column = SyncDataColumn::new(
                        Epoch::new(0),
                        block_root,
                        slot,
                        0,
                        &peers.into_iter().collect::<Vec<_>>(),
                    )
                }
            }
            Ok(SyncColumnResult::Wait) => {
                // Do nothing wait for future event
            }
            Err(e) => match e {
                SyncDataColumnError::InternalError(_) | SyncDataColumnError::TooManyErrors(_) => {
                    debug!(error = ?e, "Custody sync failed");
                    self.set_state(BackFillState::Failed);
                }
            },
        }
    }

    /// Updates the global network state indicating the current state of a backfill sync.
    #[instrument(parent = None,
        fields(service = "backfill_sync"),
        name = "backfill_sync",
        skip_all
    )]
    fn set_state(&self, state: BackFillState) {
        // *self.network_globals.backfill_state.write() = state;
        // TODO(custody-sync)
    }

    fn state(&self) -> BackFillState {
        // TODO(custody-sync)
        // self.network_globals.custody_sync_state.read().clone()
        todo!()
    }

    fn is_complete(&self, slot: Slot) -> bool {
        // TODO(custody-sync) were done once weve reached the DA window
        todo!()
    }
}
