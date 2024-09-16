use types::{IndexedAttestation, SingleAttestation, SubnetId};

use crate::BeaconChainTypes;

/// Wraps an `SingleAttestation` that has been fully verified for propagation on the gossip network.
pub struct VerifiedSingleAttestation<'a, T: BeaconChainTypes> {
    attestation: &'a SingleAttestation,
    indexed_attestation: IndexedAttestation<T::EthSpec>,
    subnet_id: SubnetId,
}