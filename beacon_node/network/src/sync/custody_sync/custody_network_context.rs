use std::{collections::HashSet, sync::Arc};

use alloy_primitives::map::HashMap;
use beacon_chain::{BeaconChain, BeaconChainTypes, EngineState};
#[cfg(test)]
use lighthouse_network::NetworkGlobals;
use lighthouse_network::{
    rpc::RequestType,
    service::api_types::{
        AppRequestId, CustodyId, CustodyRequester, DataColumnsByRootRequestId,
        DataColumnsByRootRequester, Id, SingleLookupReqId, SyncRequestId,
    },
    PeerAction, PeerId, ReportSource,
};
#[cfg(test)]
use task_executor::TaskExecutor;
use tokio::sync::mpsc;
use tracing::{debug, span, warn, Level};
use types::{ColumnIndex, DataColumnSidecar, ForkContext, Hash256};

#[cfg(test)]
use types::EthSpec;

#[cfg(test)]
use crate::network_beacon_processor::TestBeaconChainType;
use crate::{
    network_beacon_processor::NetworkBeaconProcessor,
    sync::{
        network_context::{
            requests::ActiveRequests, DataColumnsByRootSingleBlockRequest, LookupRequestResult,
            RpcEvent, RpcResponseError, RpcResponseResult,
        },
        DataColumnsByRootRequestItems,
    },
    NetworkMessage,
};

/// Wraps a Network channel to employ various RPC related network functionality for the Custody Sync manager. This includes management of a global RPC request Id.
pub struct CustodySyncNetworkContext<T: BeaconChainTypes> {
    /// The network channel to relay messages to the Network service.
    network_send: mpsc::UnboundedSender<NetworkMessage<T::EthSpec>>,

    /// A sequential ID for all RPC requests.
    request_id: Id,

    /// A mapping of active DataColumnsByRoot requests
    data_columns_by_root_requests:
        ActiveRequests<DataColumnsByRootRequestId, DataColumnsByRootRequestItems<T::EthSpec>>,

    /// Whether the ee is online. If it's not, we don't allow access to the
    /// `beacon_processor_send`.
    execution_engine_state: EngineState,

    /// Sends work to the beacon processor via a channel.
    network_beacon_processor: Arc<NetworkBeaconProcessor<T>>,

    pub chain: Arc<BeaconChain<T>>,

    fork_context: Arc<ForkContext>,
}

#[cfg(test)]
impl<E: EthSpec> CustodySyncNetworkContext<TestBeaconChainType<E>> {
    pub fn new_for_testing(
        beacon_chain: Arc<BeaconChain<TestBeaconChainType<E>>>,
        network_globals: Arc<NetworkGlobals<E>>,
        task_executor: TaskExecutor,
    ) -> Self {
        use slot_clock::SlotClock;
        use types::Slot;

        let fork_context = Arc::new(ForkContext::new::<E>(
            beacon_chain.slot_clock.now().unwrap_or(Slot::new(0)),
            beacon_chain.genesis_validators_root,
            &beacon_chain.spec,
        ));
        let (network_tx, _network_rx) = mpsc::unbounded_channel();
        let (beacon_processor, _) = NetworkBeaconProcessor::null_for_testing(
            network_globals,
            mpsc::unbounded_channel().0,
            beacon_chain.clone(),
            task_executor,
        );

        CustodySyncNetworkContext::new(
            network_tx,
            Arc::new(beacon_processor),
            beacon_chain,
            fork_context,
        )
    }
}

impl<T: BeaconChainTypes> CustodySyncNetworkContext<T> {
    pub fn new(
        network_send: mpsc::UnboundedSender<NetworkMessage<T::EthSpec>>,
        network_beacon_processor: Arc<NetworkBeaconProcessor<T>>,
        chain: Arc<BeaconChain<T>>,
        fork_context: Arc<ForkContext>,
    ) -> Self {
        let span = span!(
            Level::INFO,
            "CustoySyncNetworkContext",
            service = "custody_network_context"
        );
        let _enter = span.enter();
        CustodySyncNetworkContext {
            network_send,
            execution_engine_state: EngineState::Online, // always assume `Online` at the start
            request_id: 1,
            data_columns_by_root_requests: ActiveRequests::new("data_columns_by_root"),
            network_beacon_processor,
            chain,
            fork_context,
        }
    }

    pub fn batch_data_columns_by_root_request(
        &mut self,
        block_root: Hash256,
        column_indices_and_peers: HashMap<ColumnIndex, PeerId>,
    ) {
        for (column_index, peer_id) in column_indices_and_peers.iter() {
            let requester = DataColumnsByRootRequester::Custody(CustodyId {
                requester: CustodyRequester(SingleLookupReqId {
                    lookup_id: 1,
                    req_id: 1,
                }),
            });
            let request = DataColumnsByRootSingleBlockRequest {
                block_root: block_root,
                indices: vec![*column_index],
            };
            // TODO(custody-sync)
            let _ = self.data_columns_by_root_request(requester, *peer_id, request, false);
        }
    }

    /// Request to send a single `data_columns_by_root` request to the network.
    pub fn data_columns_by_root_request(
        &mut self,
        requester: DataColumnsByRootRequester,
        peer_id: PeerId,
        request: DataColumnsByRootSingleBlockRequest,
        expect_max_responses: bool,
    ) -> Result<DataColumnsByRootRequestId, &'static str> {
        let span = span!(
            Level::INFO,
            "CustodySyncNetworkContext",
            service = "custody_network_context"
        );
        let _enter = span.enter();

        let id = DataColumnsByRootRequestId {
            id: self.next_id(),
            requester,
        };

        self.send_network_msg(NetworkMessage::SendRequest {
            peer_id,
            request: RequestType::DataColumnsByRoot(
                request
                    .clone()
                    .try_into_request(self.fork_context.current_fork_name(), &self.chain.spec)?,
            ),
            app_request_id: AppRequestId::CustodySync(SyncRequestId::DataColumnsByRoot(id)),
        })?;

        debug!(
            method = "DataColumnsByRoot",
            block_root = ?request.block_root,
            indices = ?request.indices,
            peer = %peer_id,
            %id,
            "Sync RPC request sent"
        );

        self.data_columns_by_root_requests.insert(
            id,
            peer_id,
            expect_max_responses,
            DataColumnsByRootRequestItems::new(request),
        );

        Ok(id)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn on_data_columns_by_root_response(
        &mut self,
        id: DataColumnsByRootRequestId,
        peer_id: PeerId,
        rpc_event: RpcEvent<Arc<DataColumnSidecar<T::EthSpec>>>,
    ) -> Option<RpcResponseResult<Vec<Arc<DataColumnSidecar<T::EthSpec>>>>> {
        let resp = self
            .data_columns_by_root_requests
            .on_response(id, rpc_event);
        self.on_rpc_response_result(id, "DataColumnsByRoot", resp, peer_id, |_| 1)
    }

    /// Sends an arbitrary network message.
    fn send_network_msg(&self, msg: NetworkMessage<T::EthSpec>) -> Result<(), &'static str> {
        let span = span!(
            Level::INFO,
            "CustodySyncNetworkContext",
            service = "custody_network_context"
        );
        let _enter = span.enter();

        self.network_send.send(msg).map_err(|_| {
            debug!("Could not send message to the network service");
            "Network channel send Failed"
        })
    }

    fn on_rpc_response_result<I: std::fmt::Display, R, F: FnOnce(&R) -> usize>(
        &mut self,
        id: I,
        method: &'static str,
        resp: Option<RpcResponseResult<R>>,
        peer_id: PeerId,
        get_count: F,
    ) -> Option<RpcResponseResult<R>> {
        match &resp {
            None => {}
            Some(Ok((v, _))) => {
                debug!(
                    %id,
                    method,
                    count = get_count(v),
                    "Sync RPC request completed"
                );
            }
            Some(Err(e)) => {
                debug!(
                    %id,
                    method,
                    error = ?e,
                    "Sync RPC request error"
                );
            }
        }
        if let Some(Err(RpcResponseError::VerifyError(e))) = &resp {
            self.report_peer(peer_id, PeerAction::LowToleranceError, e.into());
        }
        resp
    }

    /// Reports to the scoring algorithm the behaviour of a peer.
    pub fn report_peer(&self, peer_id: PeerId, action: PeerAction, msg: &'static str) {
        let span = span!(
            Level::INFO,
            "CustodySyncNetworkContext",
            service = "custody_network_context"
        );
        let _enter = span.enter();

        debug!(%peer_id, %action, %msg, "Sync reporting peer");
        self.network_send
            .send(NetworkMessage::ReportPeer {
                peer_id,
                action,
                source: ReportSource::CustodySyncService,
                msg,
            })
            .unwrap_or_else(|e| {
                warn!(error = %e, "Could not report peer: channel failed");
            });
    }

    pub fn next_id(&mut self) -> Id {
        let id = self.request_id;
        self.request_id += 1;
        id
    }
}
