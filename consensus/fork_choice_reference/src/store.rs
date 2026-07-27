use std::collections::{BTreeMap, BTreeSet};
use types::{Checkpoint, Epoch, Hash256, Slot};

/// Spec: `PayloadStatus` (`uint8`). Discriminants must match the spec's
/// `PAYLOAD_STATUS_*` constants — `get_payload_status_tiebreaker` relies on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadStatus {
    Empty = 0,
    Full = 1,
    Pending = 2,
}

/// Spec: `ForkChoiceNode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForkChoiceNode {
    pub root: Hash256,
    pub payload_status: PayloadStatus,
}

/// Spec: `LatestMessage`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatestMessage {
    pub slot: Slot,
    pub root: Hash256,
    pub payload_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub slot: Slot,
    pub parent_root: Hash256,
    /// The payload status of this block's parent as claimed by this block's bid, standing in for
    /// the spec's bid block-hash comparison in `get_parent_payload_status`. `Pending` is invalid.
    pub parent_payload_status: PayloadStatus,
    pub proposer_index: u64,
    /// Spec: `store.block_timeliness[root][PTC_TIMELINESS_INDEX]`.
    pub ptc_timely: bool,
    /// Spec: `store.block_timeliness[root][ATTESTATION_TIMELINESS_INDEX]`.
    pub attestation_timely: bool,
    /// Spec: `store.block_states[root].current_justified_checkpoint`.
    pub justified_checkpoint: Checkpoint,
    /// Spec: `store.unrealized_justifications[root]`.
    pub unrealized_justified_checkpoint: Checkpoint,
}

/// The subset of the spec `Store` needed for head selection, with checkpoint-derived values
/// (per-block justified checkpoints, balances) provided as givens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Store {
    pub current_slot: Slot,
    pub justified_checkpoint: Checkpoint,
    pub finalized_checkpoint: Checkpoint,
    pub blocks: BTreeMap<Hash256, Block>,
    /// Spec: `store.payloads` keys — blocks whose envelope has been delivered and verified.
    pub payload_revealed: BTreeSet<Hash256>,
    /// Spec: `store.payload_timeliness_vote`. Missing entry means no PTC votes.
    pub payload_timeliness_votes: BTreeMap<Hash256, Vec<Option<bool>>>,
    /// Spec: `store.payload_data_availability_vote`. Missing entry means no PTC votes.
    pub payload_data_availability_votes: BTreeMap<Hash256, Vec<Option<bool>>>,
    pub latest_messages: BTreeMap<u64, LatestMessage>,
    pub equivocating_indices: BTreeSet<u64>,
    /// Effective balances of active, unslashed validators in the justified state; zero for
    /// validators that are inactive or slashed.
    pub balances: Vec<u64>,
    pub proposer_boost_root: Hash256,
    pub ptc_size: usize,
    pub slots_per_epoch: u64,
    /// Spec: `PROPOSER_SCORE_BOOST` (percent).
    pub proposer_score_boost: u64,
    /// Spec: `REORG_HEAD_WEIGHT_THRESHOLD` (percent).
    pub reorg_head_weight_threshold: u64,
    /// Spec: `EFFECTIVE_BALANCE_INCREMENT`.
    pub effective_balance_increment: u64,
    /// Spec: `REORG_PARENT_WEIGHT_THRESHOLD` (percent).
    pub reorg_parent_weight_threshold: u64,
    /// Spec: `REORG_MAX_EPOCHS_SINCE_FINALIZATION`.
    pub reorg_max_epochs_since_finalization: u64,
    /// Spec: milliseconds into the proposal slot (`is_proposing_on_time`).
    pub time_into_slot_ms: u64,
    /// Spec: `get_proposer_reorg_cutoff_ms()`.
    pub proposer_reorg_cutoff_ms: u64,
}

impl Store {
    /// Spec: `PAYLOAD_TIMELY_THRESHOLD` / `DATA_AVAILABILITY_TIMELY_THRESHOLD` (`PTC_SIZE // 2`).
    pub fn ptc_threshold(&self) -> usize {
        self.ptc_size / 2
    }

    /// Spec: `compute_epoch_at_slot`.
    pub fn epoch_at_slot(&self, slot: Slot) -> Epoch {
        slot.epoch(self.slots_per_epoch)
    }
}
