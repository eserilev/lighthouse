use std::{fmt, sync::Arc};

use beacon_processor::scheduler::work_reprocessing_queue::ReadyWork;
use beacon_processor::{
    BeaconProcessorConfig, GossipAggregatePackage, GossipAttestationPackage, IgnoredRpcBlock,
    QueuedAggregate, QueuedBackfillBatch, QueuedGossipBlock, QueuedLightClientUpdate,
    QueuedRpcBlock, QueuedSamplingRequest, QueuedUnaggregate, WorkEvent, WorkType,
};
use lighthouse_network::NetworkGlobals;
use slot_clock::SlotClock;
use strum::AsRefStr;
use tokio::sync::mpsc;
use types::{BeaconState, ChainSpec, EthSpec, Hash256, Slot};

use crate::{
    AsyncFn, BeaconProcessor, BlockingFn, BlockingOrAsync, Clock, GenericWork, GenericWorkEvent,
    Scheduler,
};

/// Indicates the type of work to be performed and therefore its priority and
/// queuing specifics.
pub enum Work<E: EthSpec> {
    GossipAttestation {
        attestation: Box<GossipAttestationPackage<E>>,
        process_individual: Box<dyn FnOnce(GossipAttestationPackage<E>) + Send + Sync>,
        process_batch: Box<dyn FnOnce(Vec<GossipAttestationPackage<E>>) + Send + Sync>,
    },
    UnknownBlockAttestation {
        process_fn: BlockingFn,
    },
    GossipAttestationBatch {
        attestations: Vec<GossipAttestationPackage<E>>,
        process_batch: Box<dyn FnOnce(Vec<GossipAttestationPackage<E>>) + Send + Sync>,
    },
    GossipAggregate {
        aggregate: Box<GossipAggregatePackage<E>>,
        process_individual: Box<dyn FnOnce(GossipAggregatePackage<E>) + Send + Sync>,
        process_batch: Box<dyn FnOnce(Vec<GossipAggregatePackage<E>>) + Send + Sync>,
    },
    UnknownBlockAggregate {
        process_fn: BlockingFn,
    },
    UnknownLightClientOptimisticUpdate {
        parent_root: Hash256,
        process_fn: BlockingFn,
    },
    UnknownBlockSamplingRequest {
        process_fn: BlockingFn,
    },
    GossipAggregateBatch {
        aggregates: Vec<GossipAggregatePackage<E>>,
        process_batch: Box<dyn FnOnce(Vec<GossipAggregatePackage<E>>) + Send + Sync>,
    },
    GossipBlock(AsyncFn),
    GossipBlobSidecar(AsyncFn),
    GossipDataColumnSidecar(AsyncFn),
    DelayedImportBlock {
        beacon_block_slot: Slot,
        beacon_block_root: Hash256,
        process_fn: AsyncFn,
    },
    GossipVoluntaryExit(BlockingFn),
    GossipProposerSlashing(BlockingFn),
    GossipAttesterSlashing(BlockingFn),
    GossipSyncSignature(BlockingFn),
    GossipSyncContribution(BlockingFn),
    GossipLightClientFinalityUpdate(BlockingFn),
    GossipLightClientOptimisticUpdate(BlockingFn),
    RpcBlock {
        process_fn: AsyncFn,
    },
    RpcBlobs {
        process_fn: AsyncFn,
    },
    RpcCustodyColumn(AsyncFn),
    RpcVerifyDataColumn(AsyncFn),
    SamplingResult(AsyncFn),
    IgnoredRpcBlock {
        process_fn: BlockingFn,
    },
    ChainSegment(AsyncFn),
    ChainSegmentBackfill(AsyncFn),
    Status(BlockingFn),
    BlocksByRangeRequest(AsyncFn),
    BlocksByRootsRequest(AsyncFn),
    BlobsByRangeRequest(BlockingFn),
    BlobsByRootsRequest(BlockingFn),
    DataColumnsByRootsRequest(BlockingFn),
    DataColumnsByRangeRequest(BlockingFn),
    GossipBlsToExecutionChange(BlockingFn),
    LightClientBootstrapRequest(BlockingFn),
    LightClientOptimisticUpdateRequest(BlockingFn),
    LightClientFinalityUpdateRequest(BlockingFn),
    LightClientUpdatesByRangeRequest(BlockingFn),
    ApiRequestP0(BlockingOrAsync),
    ApiRequestP1(BlockingOrAsync),
    Reprocess(ReprocessQueueMessage),
    GossipCanonicalBlock(AsyncFn),
    RpcCanonicalBlock {
        process_fn: AsyncFn,
    },
}

/// Messages that the scheduler can receive.
#[derive(AsRefStr)]
pub enum ReprocessQueueMessage {
    /// A block that has been received early and we should queue for later processing.
    EarlyBlock(QueuedGossipBlock),
    /// A gossip block for hash `X` is being imported, we should queue the rpc block for the same
    /// hash until the gossip block is imported.
    RpcBlock(QueuedRpcBlock),
    /// A block that was successfully processed. We use this to handle attestations updates
    /// for unknown blocks.
    BlockImported {
        block_root: Hash256,
        parent_root: Hash256,
    },
    /// A new `LightClientOptimisticUpdate` has been produced. We use this to handle light client
    /// updates for unknown parent blocks.
    NewLightClientOptimisticUpdate { parent_root: Hash256 },
    /// An unaggregated attestation that references an unknown block.
    UnknownBlockUnaggregate(QueuedUnaggregate),
    /// An aggregated attestation that references an unknown block.
    UnknownBlockAggregate(QueuedAggregate),
    /// A light client optimistic update that references a parent root that has not been seen as a parent.
    UnknownLightClientOptimisticUpdate(QueuedLightClientUpdate),
    /// A sampling request that references an unknown block.
    UnknownBlockSamplingRequest(QueuedSamplingRequest),
    /// A new backfill batch that needs to be scheduled for processing.
    BackfillSync(QueuedBackfillBatch),
}

impl<E: EthSpec> fmt::Debug for Work<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Into::<&'static str>::into(self.work_type_str()))
    }
}

impl<E: EthSpec> GenericWork for Work<E> {
    fn work_type_str(&self) -> &'static str {
        todo!()
    }

    fn work_type<Y>(&self) -> Y {
        todo!()
    }

    fn processing_type(&self) -> crate::ProcessingType {
        todo!()
    }

    fn is_priority_work(&self) -> bool {
        todo!()
    }

    fn reprocess_work<R>(&self) -> Option<R> {
        todo!()
    }

    fn drop_under_global_condition(&self) -> bool {
        todo!()
    }

    fn calculate_deadline<T>(&self, clock: T) -> Option<std::time::Duration> {
        todo!()
    }
}

impl BeaconProcessor<BeaconProcessorConfig> {
    /// Spawns the "manager" task which checks the receiver end of the returned `Sender` for
    /// messages which contain some new work which will be:
    ///
    /// - Performed immediately, if a worker is available.
    /// - Queued for later processing, if no worker is currently available.
    ///
    /// Only `self.config.max_workers` will ever be spawned at one time. Each worker is a `tokio` task
    /// started with `spawn_blocking`.
    ///
    /// The optional `work_journal_tx` allows for an outside process to receive a log of all work
    /// events processed by `self`. This should only be used during testing.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_manager<S: SlotClock + Clock + 'static, E: EthSpec>(
        self,
        network_globals: Arc<NetworkGlobals<E>>,
        beacon_state: &BeaconState<E>,
        event_rx: mpsc::Receiver<GenericWorkEvent<Work<E>>>,
        work_journal_tx: Option<mpsc::Sender<&'static str>>,
        slot_clock: S,
        spec: &ChainSpec,
    ) -> Result<(), String>
    where
        S: Send + Sync + 'static,
    {
        let scheduler = Scheduler::new(self, slot_clock);

        let f = |network_globals: &Arc<NetworkGlobals<E>>| {
            network_globals.sync_state.read().is_syncing()
        };

        scheduler.run::<ReadyWork, ReprocessQueueMessage, WorkType, NetworkGlobals<E>>(
            network_globals,
            event_rx,
            work_journal_tx,
            f,
        )
    }
}

impl<E: EthSpec> From<ReadyWork> for GenericWorkEvent<Work<E>> {
    fn from(ready_work: ReadyWork) -> Self {
        match ready_work {
            ReadyWork::Block(QueuedGossipBlock {
                beacon_block_slot,
                beacon_block_root,
                process_fn,
            }) => Self {
                drop_during_sync: false,
                work: Work::DelayedImportBlock {
                    beacon_block_slot,
                    beacon_block_root,
                    process_fn,
                },
            },
            ReadyWork::RpcBlock(QueuedRpcBlock {
                beacon_block_root: _,
                process_fn,
                ignore_fn: _,
            }) => Self {
                drop_during_sync: false,
                work: Work::RpcBlock { process_fn },
            },
            ReadyWork::IgnoredRpcBlock(IgnoredRpcBlock { process_fn }) => Self {
                drop_during_sync: false,
                work: Work::IgnoredRpcBlock { process_fn },
            },
            ReadyWork::Unaggregate(QueuedUnaggregate {
                beacon_block_root: _,
                process_fn,
            }) => Self {
                drop_during_sync: true,
                work: Work::UnknownBlockAttestation { process_fn },
            },
            ReadyWork::Aggregate(QueuedAggregate {
                process_fn,
                beacon_block_root: _,
            }) => Self {
                drop_during_sync: true,
                work: Work::UnknownBlockAggregate { process_fn },
            },
            ReadyWork::LightClientUpdate(QueuedLightClientUpdate {
                parent_root,
                process_fn,
            }) => Self {
                drop_during_sync: true,
                work: Work::UnknownLightClientOptimisticUpdate {
                    parent_root,
                    process_fn,
                },
            },
            ReadyWork::SamplingRequest(QueuedSamplingRequest { process_fn, .. }) => Self {
                drop_during_sync: true,
                work: Work::UnknownBlockSamplingRequest { process_fn },
            },
            ReadyWork::BackfillSync(QueuedBackfillBatch(process_fn)) => Self {
                drop_during_sync: false,
                work: Work::ChainSegmentBackfill(process_fn),
            },
        }
    }
}
