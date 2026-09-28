use crate::Slot;
use fixed_bytes::Hash256;
use ssz_derive::{Decode, Encode};
use tree_hash_derive::TreeHash;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Encode, Decode, TreeHash)]
pub struct Checkpoint {
    pub root: Hash256,
    pub slot: Slot,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Encode, Decode, TreeHash)]
pub struct AttestationData {
    pub slot: Slot,
    pub head: Checkpoint,
    pub target: Checkpoint,
    pub source: Checkpoint,
}
