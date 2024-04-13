use crate::test_utils::TestRandom;
use crate::{AttestationData, BitList, EthSpec};

use derivative::Derivative;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use ssz::Decode;
use ssz_derive::{Decode, Encode};
use superstruct::superstruct;
use test_random_derive::TestRandom;
use tree_hash_derive::TreeHash;

/// An attestation that has been included in the state but not yet fully processed.
///
/// Spec v0.12.1
#[superstruct(
    variants(Base, Electra),
    variant_attributes(
        derive(
            Debug,
            Clone,
            Serialize,
            Deserialize,
            Decode,
            Encode,
            TestRandom,
            Derivative,
            arbitrary::Arbitrary,
            TreeHash,
        ),
        derivative(PartialEq, Hash(bound = "E: EthSpec")),
        serde(bound = "E: EthSpec", deny_unknown_fields),
        arbitrary(bound = "E: EthSpec"),
    )
)]
#[derive(
    Debug,
    Clone,
    Serialize,
    TreeHash,
    Encode,
    Derivative,
    Deserialize,
    arbitrary::Arbitrary,
    PartialEq,
)]
#[serde(untagged)]
#[tree_hash(enum_behaviour = "transparent")]
#[ssz(enum_behaviour = "transparent")]
#[serde(bound = "E: EthSpec", deny_unknown_fields)]
#[arbitrary(bound = "E: EthSpec")]
pub struct PendingAttestation<E: EthSpec> {
    pub aggregation_bits: BitList<E::MaxValidatorsPerCommitteePerSlot>,
    pub data: AttestationData,
    #[serde(with = "serde_utils::quoted_u64")]
    pub inclusion_delay: u64,
    #[serde(with = "serde_utils::quoted_u64")]
    pub proposer_index: u64,
    #[superstruct(only(Electra))]
    pub committee_bits: BitList<E::MaxCommitteesPerSlot>,
}

impl<E: EthSpec> Decode for PendingAttestation<E> {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, ssz::DecodeError> {
        if let Ok(result) = PendingAttestationBase::from_ssz_bytes(bytes) {
            return Ok(PendingAttestation::Base(result));
        }

        if let Ok(result) = PendingAttestationElectra::from_ssz_bytes(bytes) {
            return Ok(PendingAttestation::Electra(result));
        }

        Err(ssz::DecodeError::BytesInvalid(String::from(
            "bytes not valid for any fork variant",
        )))
    }
}

impl<E: EthSpec> TestRandom for PendingAttestation<E> {
    fn random_for_test(_rng: &mut impl RngCore) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;

    ssz_and_tree_hash_tests!(PendingAttestation<MainnetEthSpec>);
}
