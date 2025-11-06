use super::{ActiveRequestItems, LookupVerifyError};
use lighthouse_network::rpc::methods::ExecutionPayloadEnvelopesByRangeRequest;
use std::sync::Arc;
use types::{EthSpec, SignedExecutionPayloadEnvelope};

/// Accumulates results of a payload_envelopes_by_range request. Only returns items after receiving the
/// stream termination.
pub struct ExecutionPayloadEnvelopesByRangeRequestItems<E: EthSpec> {
    request: ExecutionPayloadEnvelopesByRangeRequest,
    items: Vec<Arc<SignedExecutionPayloadEnvelope<E>>>,
}

impl<E: EthSpec> ExecutionPayloadEnvelopesByRangeRequestItems<E> {
    pub fn new(request: ExecutionPayloadEnvelopesByRangeRequest) -> Self {
        Self {
            request,
            items: vec![],
        }
    }
}

impl<E: EthSpec> ActiveRequestItems for ExecutionPayloadEnvelopesByRangeRequestItems<E> {
    type Item = Arc<SignedExecutionPayloadEnvelope<E>>;

    fn add(&mut self, payload: Self::Item) -> Result<bool, LookupVerifyError> {
        if payload.message().slot().as_u64() < self.request.start_slot
            || payload.message().slot().as_u64() >= self.request.start_slot + self.request.count
        {
            return Err(LookupVerifyError::UnrequestedSlot(payload.message().slot()));
        }
        if self
            .items
            .iter()
            .any(|existing| existing.message().slot() == payload.message().slot())
        {
            // DuplicatedData is a common error for all components, default index to 0
            return Err(LookupVerifyError::DuplicatedData(
                payload.message().slot(),
                0,
            ));
        }

        self.items.push(payload);

        Ok(self.items.len() >= self.request.count as usize)
    }

    fn consume(&mut self) -> Vec<Self::Item> {
        std::mem::take(&mut self.items)
    }
}
