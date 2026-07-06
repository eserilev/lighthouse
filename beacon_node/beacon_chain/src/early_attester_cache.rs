use crate::data_availability_checker::{AvailableBlock, AvailableBlockData};
use crate::{BeaconChainError as Error, metrics};
use parking_lot::RwLock;
use proto_array::Block as ProtoBlock;
use std::sync::Arc;
use tracing::instrument;
use types::*;

pub struct CacheItem<E: EthSpec> {
    /*
     * Values used to create attestations.
     */
    epoch: Epoch,
    beacon_block_root: Hash256,
    source: Checkpoint,
    target: Checkpoint,
    /*
     * Values used to make the block available.
     */
    block: Arc<SignedBeaconBlock<E>>,
    blobs: Option<BlobSidecarList<E>>,
    data_columns: Option<DataColumnSidecarList<E>>,
    proto_block: ProtoBlock,
}

/// Provides a single-item cache which allows for attesting to blocks before those blocks have
/// reached the database.
///
/// This cache stores enough information to allow Lighthouse to:
///
/// - Produce an attestation without using `chain.canonical_head`.
/// - Verify that a block root exists (i.e., will be imported in the future) during attestation
///   verification.
/// - Provide a block which can be sent to peers via RPC.
#[derive(Default)]
pub struct EarlyAttesterCache<E: EthSpec> {
    item: RwLock<Option<CacheItem<E>>>,
}

impl<E: EthSpec> EarlyAttesterCache<E> {
    /// Removes the cached item, meaning that all future calls to `Self::try_attest` will return
    /// `None` until a new cache item is added.
    pub fn clear(&self) {
        *self.item.write() = None
    }

    /// Updates the cache item, so that `Self::try_attest` with return `Some` when given suitable
    /// parameters.
    pub fn add_head_block(
        &self,
        beacon_block_root: Hash256,
        block: &AvailableBlock<E>,
        proto_block: ProtoBlock,
        state: &BeaconState<E>,
    ) -> Result<(), Error> {
        let epoch = state.current_epoch();
        let source = state.current_justified_checkpoint();
        let target_slot = epoch.start_slot(E::slots_per_epoch());
        let target = Checkpoint {
            epoch,
            root: if state.slot() <= target_slot {
                beacon_block_root
            } else {
                *state.get_block_root(target_slot)?
            },
        };

        let (blobs, data_columns) = match block.data() {
            AvailableBlockData::NoData => (None, None),
            AvailableBlockData::Blobs(blobs) => (Some(blobs.clone()), None),
            AvailableBlockData::DataColumns(data_columns) => (None, Some(data_columns.clone())),
        };

        let item = CacheItem {
            epoch,
            beacon_block_root,
            source,
            target,
            block: block.block_cloned(),
            blobs,
            data_columns,
            proto_block,
        };

        *self.item.write() = Some(item);

        Ok(())
    }

    /// Will return `Some(attestation_data)` if all the following conditions are met:
    ///
    /// - There is a cache `item` present.
    /// - If `request_slot` is in the same epoch as `item.epoch`.
    ///
    /// Post gloas an additional condition must be met:
    /// - `request_slot` is the same slot as `item.block.slot` (i.e. a same slot attestation).
    ///
    /// Non-same-slot Gloas attestations need `data.index` set from the canonical payload
    /// status, which the cache doesn't track. Returning `None` falls through to fork choice.
    #[instrument(skip_all, fields(%request_slot), level = "debug")]
    pub fn try_attest(&self, request_slot: Slot, spec: &ChainSpec) -> Option<AttestationData> {
        let lock = self.item.read();
        let item = lock.as_ref()?;

        let request_epoch = request_slot.epoch(E::slots_per_epoch());
        if request_epoch != item.epoch {
            return None;
        }

        if request_slot < item.block.slot() {
            return None;
        }

        let is_same_slot_attestation = request_slot == item.block.slot();
        if spec.fork_name_at_slot::<E>(request_slot).gloas_enabled() && !is_same_slot_attestation {
            return None;
        }

        metrics::inc_counter(&metrics::BEACON_EARLY_ATTESTER_CACHE_HITS);

        // Same-slot attestations have `index == 0` in Gloas, and pre-gloas the index is
        // always 0.
        Some(AttestationData {
            slot: request_slot,
            index: 0,
            beacon_block_root: item.beacon_block_root,
            source: item.source,
            target: item.target,
        })
    }

    /// Returns `true` if `block_root` matches the cached item.
    pub fn contains_block(&self, block_root: Hash256) -> bool {
        self.item
            .read()
            .as_ref()
            .is_some_and(|item| item.beacon_block_root == block_root)
    }

    /// Returns the block, if `block_root` matches the cached item.
    pub fn get_block(&self, block_root: Hash256) -> Option<Arc<SignedBeaconBlock<E>>> {
        self.item
            .read()
            .as_ref()
            .filter(|item| item.beacon_block_root == block_root)
            .map(|item| item.block.clone())
    }

    /// Returns the blobs, if `block_root` matches the cached item.
    pub fn get_blobs(&self, block_root: Hash256) -> Option<BlobSidecarList<E>> {
        self.item
            .read()
            .as_ref()
            .filter(|item| item.beacon_block_root == block_root)
            .and_then(|item| item.blobs.clone())
    }

    /// Returns the data columns, if `block_root` matches the cached item.
    pub fn get_data_columns(&self, block_root: Hash256) -> Option<DataColumnSidecarList<E>> {
        self.item
            .read()
            .as_ref()
            .filter(|item| item.beacon_block_root == block_root)
            .and_then(|item| item.data_columns.clone())
    }

    /// Returns the proto-array block, if `block_root` matches the cached item.
    pub fn get_proto_block(&self, block_root: Hash256) -> Option<ProtoBlock> {
        self.item
            .read()
            .as_ref()
            .filter(|item| item.beacon_block_root == block_root)
            .map(|item| item.proto_block.clone())
    }

    /// Fetch the slot and block root of the current head block.
    pub fn get_head_block_root(&self) -> Option<(Slot, Hash256)> {
        self.item
            .read()
            .as_ref()
            .map(|item| (item.block.slot(), item.beacon_block_root))
    }
}
