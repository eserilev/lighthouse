use crate::config::{XMSS_DIMENSION, XMSS_LOG_LIFETIME, XmssNodeListLimit};
use ssz::{BYTES_PER_LENGTH_OFFSET, Decode, DecodeError, Encode, SszDecoderBuilder, SszEncoder};
use ssz_derive::{Decode, Encode};
use ssz_types::{
    FixedVector, VariableList,
    typenum::{U5, U7, U8, Unsigned},
};
use tree_hash_derive::TreeHash;

pub type Fp = u32;
pub type HashDigestVector = FixedVector<Fp, U8>;
pub type HashDigestList = VariableList<HashDigestVector, XmssNodeListLimit>;
pub type Parameter = FixedVector<Fp, U5>;
pub type Randomness = FixedVector<Fp, U7>;

const FP_BYTES: usize = 4;
const HASH_DIGEST_BYTES: usize = U8::USIZE * FP_BYTES;
const RANDOMNESS_BYTES: usize = U7::USIZE * FP_BYTES;
const SIGNATURE_FIXED_PART_BYTES: usize = 2 * BYTES_PER_LENGTH_OFFSET + RANDOMNESS_BYTES;
pub const SIGNATURE_BYTES: usize = SIGNATURE_FIXED_PART_BYTES
    + BYTES_PER_LENGTH_OFFSET
    + (XMSS_LOG_LIFETIME + XMSS_DIMENSION) * HASH_DIGEST_BYTES;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct HashTreeOpening {
    pub siblings: HashDigestList,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
pub struct PublicKey {
    pub root: HashDigestVector,
    pub parameter: Parameter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignatureError {
    SiblingCount(usize),
    HashCount(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, TreeHash)]
pub struct Signature {
    path: HashTreeOpening,
    rho: Randomness,
    hashes: HashDigestList,
}

impl Signature {
    pub fn new(
        path: HashTreeOpening,
        rho: Randomness,
        hashes: HashDigestList,
    ) -> Result<Self, SignatureError> {
        if path.siblings.len() != XMSS_LOG_LIFETIME {
            return Err(SignatureError::SiblingCount(path.siblings.len()));
        }
        if hashes.len() != XMSS_DIMENSION {
            return Err(SignatureError::HashCount(hashes.len()));
        }
        Ok(Self { path, rho, hashes })
    }

    pub fn path(&self) -> &HashTreeOpening {
        &self.path
    }

    pub fn rho(&self) -> &Randomness {
        &self.rho
    }

    pub fn hashes(&self) -> &HashDigestList {
        &self.hashes
    }
}

impl Encode for Signature {
    fn is_ssz_fixed_len() -> bool {
        true
    }

    fn ssz_fixed_len() -> usize {
        SIGNATURE_BYTES
    }

    fn ssz_bytes_len(&self) -> usize {
        SIGNATURE_BYTES
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        let mut encoder = SszEncoder::container(buf, SIGNATURE_FIXED_PART_BYTES);
        encoder.append(&self.path);
        encoder.append(&self.rho);
        encoder.append(&self.hashes);
        encoder.finalize();
    }
}

impl Decode for Signature {
    fn is_ssz_fixed_len() -> bool {
        true
    }

    fn ssz_fixed_len() -> usize {
        SIGNATURE_BYTES
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() != SIGNATURE_BYTES {
            return Err(DecodeError::InvalidByteLength {
                len: bytes.len(),
                expected: SIGNATURE_BYTES,
            });
        }

        let mut builder = SszDecoderBuilder::new(bytes);
        builder.register_type::<HashTreeOpening>()?;
        builder.register_type::<Randomness>()?;
        builder.register_type::<HashDigestList>()?;
        let mut decoder = builder.build()?;

        Self::new(
            decoder.decode_next()?,
            decoder.decode_next()?,
            decoder.decode_next()?,
        )
        .map_err(|e| DecodeError::BytesInvalid(format!("{e:?}")))
    }
}
