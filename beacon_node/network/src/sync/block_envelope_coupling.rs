use std::{collections::HashMap, sync::Arc};

use beacon_chain::get_block_root;
use lighthouse_network::{PeerId, service::api_types::{
    BlocksByRangeRequestId, DataColumnsByRangeRequestId, ExecutionPayloadEnvelopesByRangeRequestId,
}};
use tracing::Span;
use types::{
    ChainSpec, ColumnIndex, DataColumnSidecarList, EthSpec, Hash256, SignedBeaconBlock, SignedExecutionPayloadEnvelope, execution_payload_envelope
};

use crate::sync::block_sidecar_coupling::{ByRangeRequest, CouplingError};

pub struct BlockAndPayload<E: EthSpec> {
    block_root: Hash256,
    block: Arc<SignedBeaconBlock<E>>,
    execution_payload_envelope: Arc<SignedExecutionPayloadEnvelope<E>>,
}

impl<E: EthSpec> BlockAndPayload<E> {
    pub fn new(
        block_root: Hash256,
        block: Arc<SignedBeaconBlock<E>>,
        execution_payload_envelope: Arc<SignedExecutionPayloadEnvelope<E>>,
    ) -> Self {
        Self {
            block_root,
            block,
            execution_payload_envelope
        }
    }

    pub fn block_root(
        &self
    ) -> Hash256 {
        get_block_root(&self.block)
    }
}

pub struct DataColumnRequests<E: EthSpec> {
    requests: HashMap<
        DataColumnsByRangeRequestId,
        ByRangeRequest<DataColumnsByRangeRequestId, DataColumnSidecarList<E>>,
    >,
    /// The column indices corresponding to the request
    column_peers: HashMap<DataColumnsByRangeRequestId, Vec<ColumnIndex>>,
    expected_custody_columns: Vec<ColumnIndex>,
    attempt: usize,
}

pub struct RangeBlockEnvelopeRequest<E: EthSpec> {
    /// Blocks we have received awaiting their corresponds payload envelope and data column sidecar.
    blocks_request: ByRangeRequest<BlocksByRangeRequestId, Vec<Arc<SignedBeaconBlock<E>>>>,
    /// Execution payload envelopes we have received awaiting to be paired with their corresponding block and data column sidecar.
    execution_payload_envelopes_requests: ByRangeRequest<
        ExecutionPayloadEnvelopesByRangeRequestId,
        Vec<Arc<SignedExecutionPayloadEnvelope<E>>>,
    >,
    /// Data column sidecars we have received awaiting to be paired with their corresponding payload
    data_column_requests: DataColumnRequests<E>,
    request_span: Span,
}

impl<E: EthSpec> RangeBlockEnvelopeRequest<E> {
    /// Creates a new range request for blocks and their associated data (blobs or data columns).
    ///
    /// # Arguments
    /// * `blocks_req_id` - Request ID for the blocks
    /// * `payloads_req_id` - Optional request ID for blobs (pre-Fulu fork)
    /// * `data_column_req_ids` - Optional tuple of (request_id->column_indices pairs, expected_custody_columns) for Fulu fork
    #[allow(clippy::type_complexity)]
    pub fn new(
        blocks_req_id: BlocksByRangeRequestId,
        payloads_request_id: ExecutionPayloadEnvelopesByRangeRequestId,
        data_column_req_ids: (
            Vec<(DataColumnsByRangeRequestId, Vec<ColumnIndex>)>,
            Vec<ColumnIndex>,
        ),
        request_span: Span,
    ) -> Self {
        let (requests, expected_custody_columns) = data_column_req_ids;
        let column_peers: HashMap<_, _> = requests.into_iter().collect();
        let data_column_requests = DataColumnRequests {
            requests: column_peers
                .keys()
                .map(|id| (*id, ByRangeRequest::Active(*id)))
                .collect(),
            column_peers,
            expected_custody_columns,
            attempt: 0,
        };

        Self {
            blocks_request: ByRangeRequest::Active(blocks_req_id),
            execution_payload_envelopes_requests: ByRangeRequest::Active(payloads_request_id),
            data_column_requests,
            request_span,
        }
    }

    /// Modifies `self` by inserting a new `DataColumnsByRangeRequestId` for a formerly failed
    /// request for some columns.
    pub fn reinsert_failed_column_requests(
        &mut self,
        failed_column_requests: Vec<(DataColumnsByRangeRequestId, Vec<u64>)>,
    ) {
        for (request, columns) in failed_column_requests.into_iter() {
            self.data_column_requests
                .requests
                .insert(request, ByRangeRequest::Active(request));
            self.data_column_requests
                .column_peers
                .insert(request, columns);
        }
    }

    /// Adds received blocks to the request.
    ///
    /// Returns an error if the request ID doesn't match the expected blocks request.
    pub fn add_blocks(
        &mut self,
        req_id: BlocksByRangeRequestId,
        blocks: Vec<Arc<SignedBeaconBlock<E>>>,
    ) -> Result<(), String> {
        self.blocks_request.finish(req_id, blocks)
    }

    /// Adds received payloads to the request.
    ///
    /// Returns an error if the request ID doesn't match the expected payloads request.
    pub fn add_payloads(
        &mut self,
        req_id: ExecutionPayloadEnvelopesByRangeRequestId,
        payloads: Vec<Arc<SignedExecutionPayloadEnvelope<E>>>,
    ) -> Result<(), String> {
        self.execution_payload_envelopes_requests
            .finish(req_id, payloads)
    }

    /// Adds received custody columns to the request.
    ///
    /// Returns an error if the request ID is unknown.
    pub fn add_custody_columns(
        &mut self,
        req_id: DataColumnsByRangeRequestId,
        data_columns: DataColumnSidecarList<E>,
    ) -> Result<(), String> {
        let req = self
            .data_column_requests
            .requests
            .get_mut(&req_id)
            .ok_or(format!("unknown data columns by range req_id {req_id}"))?;
        req.finish(req_id, data_columns)
    }

    /// Attempts to construct RPC blocks from all received components.
    ///
    /// Returns `None` if not all expected requests have completed.
    /// Returns `Some(Ok(_))` with valid RPC blocks if all data is present and valid.
    /// Returns `Some(Err(_))` if there are issues coupling blocks with their data.
    pub fn responses(
        &mut self,
        spec: &ChainSpec,
    ) -> Option<Result<(), CouplingError>> {
        let Some(blocks) = self.blocks_request.to_finished() else {
            return None;
        };

        let Some(payloads) = self.execution_payload_envelopes_requests.to_finished() else {
            return None;
        };

        let mut data_columns = vec![];
        let mut column_to_peer_id: HashMap<u64, PeerId> = HashMap::new();
        for req in self.data_column_requests.requests.values() {
            let Some(data) = req.to_finished() else {
                return None;
            };
            data_columns.extend(data.clone())
        }

        // An "attempt" is complete here after we have received a response for all the
        // requests we made. i.e. `req.to_finished()` returns Some for all requests.
        self.data_column_requests.attempt += 1;

        // Note: this assumes that only 1 peer is responsible for a column
        // with a batch.
        for (id, columns) in self.data_column_requests.column_peers {
            for column in columns {
                column_to_peer_id.insert(*column, id.peer);
            }
        }

        let resp = Self::responses_with_custody_columns(
            blocks.to_vec(),
            data_columns,
            column_to_peer_id,
            expected_custody_columns,
            *attempt,
        );

        if let Err(CouplingError::DataColumnPeerFailure {
            error: _,
            faulty_peers,
            action: _,
            exceeded_retries: _,
        }) = &resp
        {
            for (_, peer) in faulty_peers.iter() {
                // find the req id associated with the peer and
                // delete it from the entries as we are going to make
                // a separate attempt for those components.
                self.data_column_requests.requests.retain(|&k, _| k.peer != *peer);
            }
        }

        Some(resp)
    }

    fn couple_blocks_and_payloads(
        &self,
        blocks: Vec<Arc<SignedBeaconBlock<E>>>,
        payloads: Vec<Arc<SignedExecutionPayloadEnvelope<E>>>,
    ) -> Result<Vec<BlockAndPayload<E>>, CouplingError> {
        let coupled_blocks_and_payloads = vec![];
        let block_root_to_payload = payloads
            .into_iter()
            .map(|payload| (payload.message().beacon_block_root(), payload))
            .collect::<HashMap<_, _>>();
        for block in blocks {
            let block_root = get_block_root(&block);

        }

        Ok(coupled_blocks_and_payloads)
    }


    fn responses_with_custody_columns(
        payloads: Vec<Arc<SignedExecutionPayloadEnvelope<E>>>,
        data_columns: DataColumnSidecarList<E>,
        column_to_peer: HashMap<u64, PeerId>,
        expects_custody_columns: &[ColumnIndex],
        attempt: usize,
    ) -> Result<Vec<RpcBlock<E>>, CouplingError> {
        // Group data columns by block_root and index
        let mut data_columns_by_block =
            HashMap::<Hash256, HashMap<ColumnIndex, Arc<DataColumnSidecar<E>>>>::new();

        for column in data_columns {
            let block_root = column.block_root();
            let index = column.index;
            if data_columns_by_block
                .entry(block_root)
                .or_default()
                .insert(index, column)
                .is_some()
            {
                // `DataColumnsByRangeRequestItems` ensures that we do not request any duplicated indices across all peers
                // we request the data from.
                // If there are duplicated indices, its likely a peer sending us the same index multiple times.
                // However we can still proceed even if there are extra columns, just log an error.
                tracing::debug!(?block_root, ?index, "Repeated column for block_root");
                continue;
            }
        }

        // Now iterate all blocks ensuring that the block roots of each block and data column match,
        // plus we have columns for our custody requirements
        let mut available_payloads = Vec::with_capacity(blocks.len());

        let exceeded_retries = attempt >= MAX_COLUMN_RETRIES;
        for payload in payloads {
            let block_root = get_block_root(&block);
            rpc_blocks.push(if block.num_expected_blobs() > 0 {
                let Some(mut data_columns_by_index) = data_columns_by_block.remove(&block_root)
                else {
                    let responsible_peers = column_to_peer.iter().map(|c| (*c.0, *c.1)).collect();
                    return Err(CouplingError::DataColumnPeerFailure {
                        error: format!("No columns for block {block_root:?} with data"),
                        faulty_peers: responsible_peers,
                        action: PeerAction::LowToleranceError,
                        exceeded_retries,

                    });
                };

                let mut custody_columns = vec![];
                let mut naughty_peers = vec![];
                for index in expects_custody_columns {
                    // Safe to convert to `CustodyDataColumn`: we have asserted that the index of
                    // this column is in the set of `expects_custody_columns` and with the expected
                    // block root, so for the expected epoch of this batch.
                    if let Some(data_column) = data_columns_by_index.remove(index) {
                        custody_columns.push(CustodyDataColumn::from_asserted_custody(data_column));
                    } else {
                        let Some(responsible_peer) = column_to_peer.get(index) else {
                            return Err(CouplingError::InternalError(format!("Internal error, no request made for column {}", index)));
                        };
                        naughty_peers.push((*index, *responsible_peer));
                    }
                }
                if !naughty_peers.is_empty() {
                    return Err(CouplingError::DataColumnPeerFailure {
                        error: format!("Peers did not return column for block_root {block_root:?} {naughty_peers:?}"),
                        faulty_peers: naughty_peers,
                        action: PeerAction::LowToleranceError,
                        exceeded_retries
                    });
                }

                // Assert that there are no columns left
                if !data_columns_by_index.is_empty() {
                    let remaining_indices = data_columns_by_index.keys().collect::<Vec<_>>();
                    // log the error but don't return an error, we can still progress with extra columns.
                    tracing::debug!(
                        ?block_root,
                        ?remaining_indices,
                        "Not all columns consumed for block"
                    );
                }

                RpcBlock::new_with_custody_columns(Some(block_root), block, custody_columns)
                    .map_err(|e| CouplingError::InternalError(format!("{:?}", e)))?
            } else {
                // Block has no data, expects zero columns
                RpcBlock::new_without_blobs(Some(block_root), block)
            });
        }

        // Assert that there are no columns left for other blocks
        if !data_columns_by_block.is_empty() {
            let remaining_roots = data_columns_by_block.keys().collect::<Vec<_>>();
            // log the error but don't return an error, we can still progress with responses.
            // this is most likely an internal error with overrequesting or a client bug.
            tracing::debug!(?remaining_roots, "Not all columns consumed for block");
        }

        Ok(rpc_blocks)
    }

}
