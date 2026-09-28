use crate::config::{HistoricalRootsLimit, JustificationValidatorsLimit};
use crate::{BlockHeader, Checkpoint, Slot, Validators};
use fixed_bytes::Hash256;
use ssz_derive::{Decode, Encode};
use ssz_types::{BitList, VariableList};
use tree_hash_derive::TreeHash;

pub type HistoricalBlockHashes = VariableList<Hash256, HistoricalRootsLimit>;
pub type JustificationRoots = VariableList<Hash256, HistoricalRootsLimit>;
pub type JustifiedSlots = BitList<HistoricalRootsLimit>;
pub type JustificationValidators = BitList<JustificationValidatorsLimit>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Encode, Decode, TreeHash)]
pub struct GenesisConfig {
    pub genesis_time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct State {
    pub config: GenesisConfig,
    pub slot: Slot,
    pub latest_block_header: BlockHeader,
    pub latest_justified: Checkpoint,
    pub latest_finalized: Checkpoint,
    pub historical_block_hashes: HistoricalBlockHashes,
    pub justified_slots: JustifiedSlots,
    pub validators: Validators,
    pub justifications_roots: JustificationRoots,
    pub justifications_validators: JustificationValidators,
}
