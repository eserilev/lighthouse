use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use types::{
    BeaconState, ChainSpec, DataColumnSidecarList, EthSpec, ExecutionBlockHash, Hash256,
    SignedBeaconBlock, SignedExecutionPayloadEnvelope,
};

use crate::data_column_verification::CustodyDataColumn;

pub struct AvailableBlockAndEnvelope<E: EthSpec> {
    block_root: Hash256,
    block: Arc<SignedBeaconBlock<E>>,
    available_envelope: Option<Arc<AvailableEnvelope<E>>>,
}

impl<E: EthSpec> AvailableBlockAndEnvelope<E> {
    pub fn new(
        block_root: Hash256,
        block: Arc<SignedBeaconBlock<E>>,
        envelope: Option<Arc<SignedExecutionPayloadEnvelope<E>>>,
        columns: Vec<CustodyDataColumn<E>>,
        spec: Arc<ChainSpec>,
    ) -> Self {
        let Some(envelope) = envelope else {
            return Self {
                block_root,
                block,
                available_envelope: None,
            };
        };

        let columns_available_timestamp = if columns.len() > 0 {
            Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_else(|_| Duration::from_secs(0)),
            )
        } else {
            None
        };

        let data_columns = columns
            .iter()
            .map(|c| c.as_data_column().clone())
            .collect::<Vec<_>>();

        let available_envelope = Some(Arc::new(AvailableEnvelope {
            block_hash: envelope.message().payload().block_hash(),
            envelope,
            columns: data_columns,
            columns_available_timestamp,
            spec,
        }));

        Self {
            block_root,
            block,
            available_envelope,
        }
    }
}

#[derive(PartialEq)]
pub struct EnvelopeImportData<E: EthSpec> {
    pub block_root: Hash256,
    pub parent_block: Arc<SignedBeaconBlock<E>>,
    pub post_state: Box<BeaconState<E>>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct AvailableEnvelope<E: EthSpec> {
    block_hash: ExecutionBlockHash,
    envelope: Arc<SignedExecutionPayloadEnvelope<E>>,
    columns: DataColumnSidecarList<E>,
    /// Timestamp at which this block first became available (UNIX timestamp, time since 1970).
    columns_available_timestamp: Option<Duration>,
    pub spec: Arc<ChainSpec>,
}
pub enum MaybeAvailableEnvelope<E: EthSpec> {
    Available(AvailableEnvelope<E>),
    AvailabilityPending {
        block_hash: ExecutionBlockHash,
        envelope: Arc<SignedExecutionPayloadEnvelope<E>>,
    },
}
