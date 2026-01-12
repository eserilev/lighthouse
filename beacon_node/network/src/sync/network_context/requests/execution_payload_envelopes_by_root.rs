use lighthouse_network::rpc::ExecutionPayloadEnvelopesByRootRequest;
use std::sync::Arc;
use types::{EthSpec, ForkContext, Hash256, SignedExecutionPayloadEnvelope};

use super::{ActiveRequestItems, LookupVerifyError};

#[derive(Debug, Clone)]
pub struct ExecutionPayloadEnvelopesByRootSingleBlockRequest(pub Hash256);

impl ExecutionPayloadEnvelopesByRootSingleBlockRequest {
    pub fn into_request(
        self,
        spec: &ForkContext,
    ) -> Result<ExecutionPayloadEnvelopesByRootRequest, String> {
        ExecutionPayloadEnvelopesByRootRequest::new(vec![self.0], spec)
    }
}

pub struct ExecutionPayloadEnvelopesByRootRequestItems<E: EthSpec> {
    request: ExecutionPayloadEnvelopesByRootSingleBlockRequest,
    items: Vec<Arc<SignedExecutionPayloadEnvelope<E>>>,
}

impl<E: EthSpec> ExecutionPayloadEnvelopesByRootRequestItems<E> {
    pub fn new(request: ExecutionPayloadEnvelopesByRootSingleBlockRequest) -> Self {
        Self {
            request,
            items: vec![],
        }
    }
}

impl<E: EthSpec> ActiveRequestItems for ExecutionPayloadEnvelopesByRootRequestItems<E> {
    type Item = Arc<SignedExecutionPayloadEnvelope<E>>;

    /// Appends a chunk to this multi-item request. If all expected chunks are received, this
    /// method returns `Some`, resolving the request before the stream terminator.
    /// The active request SHOULD be dropped after `add_response` returns an error
    fn add(&mut self, payload_envelope: Self::Item) -> Result<bool, LookupVerifyError> {
        let block_root = payload_envelope.message.beacon_block_root;
        if self.request.0 != block_root {
            return Err(LookupVerifyError::UnrequestedBlockRoot(block_root));
        }

        self.items.push(payload_envelope);

        // theres only one payload per block so return true here always
        Ok(true)
    }

    fn consume(&mut self) -> Vec<Self::Item> {
        std::mem::take(&mut self.items)
    }
}
