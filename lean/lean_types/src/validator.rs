use crate::ValidatorIndex;
use crate::config::ValidatorRegistryLimit;
use fixed_bytes::Hash256;
use ssz::{Decode, DecodeError, Encode};
use ssz_derive::{Decode, Encode};
use ssz_types::{FixedVector, VariableList, typenum::U52};
use std::ops::Deref;
use tree_hash::{PackedEncoding, TreeHash, TreeHashType};
use tree_hash_derive::TreeHash;

pub type PublicKeyBytes = FixedVector<u8, U52>;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct Validator {
    pub attestation_public_key: PublicKeyBytes,
    pub proposal_public_key: PublicKeyBytes,
    pub index: ValidatorIndex,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValidatorsError {
    IndexMismatch {
        position: usize,
        index: ValidatorIndex,
    },
    TooManyValidators(ssz_types::Error),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Validators(VariableList<Validator, ValidatorRegistryLimit>);

impl Validators {
    pub fn new(validators: Vec<Validator>) -> Result<Self, ValidatorsError> {
        check_index_matches_position(&validators)?;
        VariableList::new(validators)
            .map(Self)
            .map_err(ValidatorsError::TooManyValidators)
    }
}

fn check_index_matches_position(validators: &[Validator]) -> Result<(), ValidatorsError> {
    validators
        .iter()
        .enumerate()
        .try_for_each(|(position, validator)| {
            if usize::try_from(validator.index).ok() == Some(position) {
                Ok(())
            } else {
                Err(ValidatorsError::IndexMismatch {
                    position,
                    index: validator.index,
                })
            }
        })
}

impl Deref for Validators {
    type Target = VariableList<Validator, ValidatorRegistryLimit>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Encode for Validators {
    fn is_ssz_fixed_len() -> bool {
        <VariableList<Validator, ValidatorRegistryLimit> as Encode>::is_ssz_fixed_len()
    }

    fn ssz_bytes_len(&self) -> usize {
        self.0.ssz_bytes_len()
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        self.0.ssz_append(buf)
    }
}

impl Decode for Validators {
    fn is_ssz_fixed_len() -> bool {
        <VariableList<Validator, ValidatorRegistryLimit> as Decode>::is_ssz_fixed_len()
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let validators = VariableList::from_ssz_bytes(bytes)?;
        check_index_matches_position(&validators)
            .map_err(|e| DecodeError::BytesInvalid(format!("{e:?}")))?;
        Ok(Self(validators))
    }
}

impl TreeHash for Validators {
    fn tree_hash_type() -> TreeHashType {
        <VariableList<Validator, ValidatorRegistryLimit> as TreeHash>::tree_hash_type()
    }

    fn tree_hash_packed_encoding(&self) -> PackedEncoding {
        self.0.tree_hash_packed_encoding()
    }

    fn tree_hash_packing_factor() -> usize {
        <VariableList<Validator, ValidatorRegistryLimit> as TreeHash>::tree_hash_packing_factor()
    }

    fn tree_hash_root(&self) -> Hash256 {
        self.0.tree_hash_root()
    }
}
