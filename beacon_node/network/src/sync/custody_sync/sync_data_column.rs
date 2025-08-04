use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use beacon_chain::BeaconChainTypes;
use itertools::Itertools;
use lighthouse_network::{
    service::api_types::{
        CustodyId, CustodyRequester, DataColumnsByRootRequestId, DataColumnsByRootRequester,
        SingleLookupReqId,
    },
    PeerAction, PeerId,
};
use parking_lot::RwLock;
use types::{ColumnIndex, DataColumnSidecar, Epoch, EthSpec, Hash256, Slot};

use crate::sync::{
    custody_sync::{
        custody_network_context::CustodySyncNetworkContext, service::SyncColumnResult,
        ColumnProcessResult,
    },
    network_context::{DataColumnsByRootSingleBlockRequest, RpcResponseError},
};

pub struct PeerGroup(HashMap<ColumnIndex, PeerId>);

impl PeerGroup {
    pub(crate) fn blame_single(
        &self,
        column_index: ColumnIndex,
        peer_action: PeerAction,
    ) -> Option<(PeerId, PeerAction)> {
        self.0.get(&column_index).map(|peer| (*peer, peer_action))
    }
}

enum ColumnSyncingStatus<E: EthSpec> {
    AwaitingDownload,
    Downloading(DataColumnsByRootRequestId, Instant),
    AwaitingProcessing(Arc<DataColumnSidecar<E>>, PeerGroup, Instant),
    Processing(Arc<DataColumnSidecar<E>>, PeerGroup, Instant),
}

#[derive(Debug)]
pub enum Error {
    InternalError(String),
    TooManyErrors(String),
}

pub enum CustodySyncResult {
    Done {
        block_root: Hash256,
        indices: HashSet<ColumnIndex>,
    },
    Wait,
}

pub struct SyncDataColumn<T: BeaconChainTypes> {
    batch_id: Epoch,
    block_root: Hash256,
    block_slot: Slot,
    column_index: ColumnIndex,
    failed_peers: HashSet<PeerId>,
    peers: Arc<RwLock<HashSet<PeerId>>>,
    request: ColumnSyncingStatus<T::EthSpec>,
    download_errors: usize,
    process_errors: usize,
}

impl<T: BeaconChainTypes> SyncDataColumn<T> {
    pub fn new(
        batch_id: Epoch,
        block_root: Hash256,
        block_slot: Slot,
        column_index: ColumnIndex,
        initial_peers: &[PeerId],
    ) -> Self {
        Self {
            batch_id,
            block_root,
            block_slot,
            column_index,
            failed_peers: <_>::default(),
            peers: Arc::new(RwLock::new(HashSet::from_iter(
                initial_peers.iter().copied(),
            ))),
            request: ColumnSyncingStatus::AwaitingDownload,
            download_errors: 0,
            process_errors: 0,
        }
    }

    pub fn block_root(&self) -> &Hash256 {
        &self.block_root
    }

    pub fn slot(&self) -> Slot {
        self.block_slot
    }

    pub fn batch_id(&self) -> Epoch {
        self.batch_id
    }

    pub fn peer_count(&self) -> usize {
        self.peers.read().len()
    }

    pub fn clone_peers(&self) -> HashSet<PeerId> {
        self.peers.read().clone()
    }

    /// Returns whether the value was newly inserted
    pub fn add_peer(&self, peer: PeerId) -> bool {
        self.peers.write().insert(peer)
    }

    pub fn remove_peer(&self, peer: &PeerId) -> bool {
        self.peers.write().remove(peer)
    }

    pub fn is_syncing(&self) -> bool {
        !matches!(self.request, ColumnSyncingStatus::AwaitingDownload)
    }

    #[cfg(test)]
    pub fn is_processing(&self) -> bool {
        matches!(self.request, ColumnSyncingStatus::Processing(..))
    }

    pub fn on_download_result(
        &mut self,
        req_id: DataColumnsByRootRequestId,
        result: Result<(Arc<DataColumnSidecar<T::EthSpec>>, PeerGroup), RpcResponseError>,
        _cx: &mut CustodySyncNetworkContext<T>,
    ) -> Result<(), Error> {
        match &mut self.request {
            ColumnSyncingStatus::Downloading(expected_id, start_time) => {
                // metrics::observe_duration(
                //     &metrics::SYNC_BLOCK_DOWNLOADING_TIME,
                //     start_time.elapsed(),
                // );

                // TODO(custody-sync)
                const MAX_DOWNLOAD_ATTEMPTS: usize = 5;
                if req_id != *expected_id {
                    return Err(Error::InternalError(format!(
                        "Unexpected request ID {} != {}",
                        req_id, expected_id,
                    )));
                }
                match result {
                    Ok((data_column, peers)) => {
                        // debug!(id = %self.id, "Sync block downloaded");
                        self.request = ColumnSyncingStatus::AwaitingProcessing(
                            data_column,
                            peers,
                            Instant::now(),
                        );
                        Ok(())
                    }
                    Err(e) => {
                        // debug!(id = %self.id, error = ?e, "Sync block download error");
                        self.request = ColumnSyncingStatus::AwaitingDownload;

                        self.download_errors += 1;
                        if self.download_errors > MAX_DOWNLOAD_ATTEMPTS {
                            return Err(Error::TooManyErrors("download errors".to_owned()));
                        }

                        Ok(())
                    }
                }
            }
            _ => Err(Error::InternalError(
                "Lookup not in expected state Downloading".to_owned(),
            )),
        }
    }

    pub fn on_process_result(
        &mut self,
        result: ColumnProcessResult,
        cx: &mut CustodySyncNetworkContext<T>,
    ) -> Result<SyncColumnResult, Error> {
        match &mut self.request {
            ColumnSyncingStatus::Processing(data_column, peers, start_time) => {
                // metrics::observe_duration(
                //     &metrics::SYNC_BLOCK_PROCESSING_TIME,
                //     start_time.elapsed(),
                // );
                match result {
                    ColumnProcessResult::Success {
                        block_root,
                        column_index,
                    } => {
                        // debug!(id = %self.id, "Sync block process success");
                        // TODO(custody-sync)
                        Ok(SyncColumnResult::Done {
                            block_root: block_root,
                            slot: Slot::new(0),
                            column_index,
                        })
                    }
                    ColumnProcessResult::FaultyFailure {
                        block_root,
                        column_index,
                        penalty,
                    } => {
                        // debug!(id = %self.id, error, "Sync block process error");
                        if let Some((peer_id, penalty)) = peers.blame_single(column_index, penalty)
                        {
                            cx.report_peer(peer_id, penalty, "faulty_batch");
                            self.failed_peers.insert(peer_id);
                        };
                        // TODO(custody-sync)
                        const MAX_PROCESS_ATTEMPTS: usize = 5;

                        self.process_errors += 1;
                        if self.process_errors > MAX_PROCESS_ATTEMPTS {
                            return Err(Error::TooManyErrors("process errors".to_owned()));
                        }

                        self.request = ColumnSyncingStatus::AwaitingDownload;
                        Ok(SyncColumnResult::Wait)
                    }
                    ColumnProcessResult::NonFaultyFailure => todo!(),
                }
            }
            _ => Err(Error::InternalError(
                "Lookup not in expected state Processing".to_owned(),
            )),
        }
    }

    /// Make progress on the request. Note that a request can never finish on this call, thus it
    /// does not return `SyncBlockResult`.
    pub fn continue_request(&mut self, cx: &mut CustodySyncNetworkContext<T>) -> Result<(), Error> {
        match &mut self.request {
            ColumnSyncingStatus::AwaitingDownload => {
                // TODO(custody-sync) we need to figure out when to construct these guys
                let requester = DataColumnsByRootRequester::Custody(CustodyId {
                    requester: CustodyRequester(SingleLookupReqId {
                        lookup_id: 1,
                        req_id: 1,
                    }),
                });
                let peer_id = self.peers.read().iter().next().unwrap().clone();
                let request = DataColumnsByRootSingleBlockRequest {
                    block_root: *self.block_root(),
                    indices: vec![self.column_index],
                };
                match cx.data_columns_by_root_request(requester, peer_id, request, false) {
                    Ok(req_id) => {
                        self.request = ColumnSyncingStatus::Downloading(req_id, Instant::now());
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(Error::InternalError(format!(
                            "Error sending data column by root request: {e:?}"
                        )))
                    }
                };
            }
            ColumnSyncingStatus::Downloading(..) => Ok(()),
            ColumnSyncingStatus::AwaitingProcessing(data_column, peers, start_time) => {
                // TODO(custody-sync)
                // Here we verify and then import the data column to the store
                Ok(())
            }
            ColumnSyncingStatus::Processing(..) => Ok(()),
        }
    }
}
