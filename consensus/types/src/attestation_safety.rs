use crate::{Attestation, EthSpec, Hash256, PublicKeyBytes};

#[derive(PartialEq, Debug)]
pub enum Safe {
    /// Casting the exact same data (block or attestation) twice is never slashable.
    SameData,
    /// Incoming data is safe from slashing, and is not a duplicate.
    Valid,
}

#[derive(Clone)]
pub struct AttestationSafety<E: EthSpec> {
    pub attestation: Attestation<E>,
    pub validator_pubkey: PublicKeyBytes,
    pub domain: Hash256,
    pub safe: Safe
}