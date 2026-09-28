use crate::config::{ProofBytesLimit, ValidatorRegistryLimit};
use ssz_derive::{Decode, Encode};
use ssz_types::{BitList, VariableList};
use tree_hash_derive::TreeHash;

pub type AggregationBits = BitList<ValidatorRegistryLimit>;
pub type ProofBytes = VariableList<u8, ProofBytesLimit>;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SingleMessageAggregate {
    pub participants: AggregationBits,
    pub proof: ProofBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct MultiMessageAggregate {
    pub proof: ProofBytes,
}
