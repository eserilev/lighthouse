use crate::{AggregatedAttestations, MultiMessageAggregate, Slot, ValidatorIndex};
use fixed_bytes::Hash256;
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct BlockBody {
    pub attestations: AggregatedAttestations,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Encode, Decode, TreeHash)]
pub struct BlockHeader {
    pub slot: Slot,
    pub proposer_index: ValidatorIndex,
    pub parent_root: Hash256,
    pub state_root: Hash256,
    pub body_root: Hash256,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct Block {
    pub slot: Slot,
    pub proposer_index: ValidatorIndex,
    pub parent_root: Hash256,
    pub state_root: Hash256,
    pub body: BlockBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SignedBlock {
    pub block: Block,
    pub proof: MultiMessageAggregate,
}
