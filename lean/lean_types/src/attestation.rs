use crate::config::ValidatorRegistryLimit;
use crate::{AggregationBits, AttestationData, Signature, SingleMessageAggregate, ValidatorIndex};
use ssz_derive::{Decode, Encode};
use ssz_types::VariableList;
use tree_hash_derive::TreeHash;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode, TreeHash)]
pub struct Attestation {
    pub validator_index: ValidatorIndex,
    pub data: AttestationData,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SignedAttestation {
    pub validator_index: ValidatorIndex,
    pub data: AttestationData,
    pub signature: Signature,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct AggregatedAttestation {
    pub aggregation_bits: AggregationBits,
    pub data: AttestationData,
}

pub type AggregatedAttestations = VariableList<AggregatedAttestation, ValidatorRegistryLimit>;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct SignedAggregatedAttestation {
    pub data: AttestationData,
    pub proof: SingleMessageAggregate,
}
