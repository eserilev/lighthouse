mod aggregation;
mod attestation;
mod block;
mod checkpoint;
pub mod config;
mod slot;
mod state;
mod validator;
mod xmss;

pub use aggregation::{AggregationBits, MultiMessageAggregate, ProofBytes, SingleMessageAggregate};
pub use attestation::{
    AggregatedAttestation, AggregatedAttestations, Attestation, SignedAggregatedAttestation,
    SignedAttestation,
};
pub use block::{Block, BlockBody, BlockHeader, SignedBlock};
pub use checkpoint::{AttestationData, Checkpoint};
pub use fixed_bytes::Hash256;
pub use slot::Slot;
pub use ssz_types::{BitList, FixedVector, VariableList};
pub use state::{
    GenesisConfig, HistoricalBlockHashes, JustificationRoots, JustificationValidators,
    JustifiedSlots, State,
};
pub use validator::{PublicKeyBytes, Validator, Validators, ValidatorsError};
pub use xmss::{
    Fp, HashDigestList, HashDigestVector, HashTreeOpening, Parameter, PublicKey, Randomness,
    Signature, SignatureError,
};

pub type ValidatorIndex = u64;
