use crate::{Case, Outcome, decode_hex};
use lean_types::*;
use serde::Deserialize;
use ssz::{Decode, Encode};
use ssz_types::{
    BitVector,
    typenum::{U4, U8, U16},
};
use tree_hash::TreeHash;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SszCase {
    type_name: String,
    serialized: String,
    root: String,
    raw_bytes: Option<String>,
    rejection_reason: Option<String>,
}

impl Case for SszCase {
    const CATEGORY: &'static str = "ssz";

    fn run(&self) -> Result<Outcome, String> {
        match self.type_name.as_str() {
            "Checkpoint" => self.check::<Checkpoint>(),
            "AttestationData" => self.check::<AttestationData>(),
            "Attestation" => self.check::<Attestation>(),
            "SignedAttestation" => self.check::<SignedAttestation>(),
            "AggregatedAttestation" => self.check::<AggregatedAttestation>(),
            "SignedAggregatedAttestation" => self.check::<SignedAggregatedAttestation>(),
            "BlockBody" => self.check::<BlockBody>(),
            "BlockHeader" => self.check::<BlockHeader>(),
            "Block" => self.check::<Block>(),
            "SignedBlock" => self.check::<SignedBlock>(),
            "Config" => self.check::<GenesisConfig>(),
            "State" => self.check::<State>(),
            "Validator" => self.check::<Validator>(),
            "Validators" => self.check::<Validators>(),
            "PublicKey" => self.check::<PublicKey>(),
            "Signature" => self.check::<Signature>(),
            "HashTreeOpening" => self.check::<HashTreeOpening>(),
            "SingleMessageAggregate" => self.check::<SingleMessageAggregate>(),
            "MultiMessageAggregate" => self.check::<MultiMessageAggregate>(),
            "Fp" => self.check::<Fp>(),
            "Uint32" => self.check::<u32>(),
            "Bytes4" => self.check::<FixedVector<u8, U4>>(),
            "DecodeBitlist8" => self.check::<BitList<U8>>(),
            "DecodeBitvector16" => self.check::<BitVector<U16>>(),
            "HashTreeLayer" => Ok(Outcome::Skipped("HashTreeLayer: XMSS key material")),
            "AttestationSubnets" | "Status" | "BlocksByRootRequest" => {
                Ok(Outcome::Skipped("networking containers"))
            }
            other => Err(format!("unknown type {other}")),
        }
    }
}

impl SszCase {
    fn check<T: Encode + Decode + TreeHash>(&self) -> Result<Outcome, String> {
        if let Some(reason) = &self.rejection_reason {
            let raw_bytes = self
                .raw_bytes
                .as_deref()
                .ok_or("rejection case without rawBytes")?;
            return match T::from_ssz_bytes(&decode_hex(raw_bytes)?) {
                Ok(_) => Err(format!("decoded bytes the spec rejects with {reason}")),
                Err(_) => Ok(Outcome::Passed),
            };
        }

        let serialized = decode_hex(&self.serialized)?;
        let value = T::from_ssz_bytes(&serialized).map_err(|e| format!("decode: {e:?}"))?;

        if value.as_ssz_bytes() != serialized {
            return Err("re-encoded bytes differ from serialized".into());
        }

        let root = decode_hex(&self.root)?;
        let computed_root = value.tree_hash_root();
        if computed_root.as_slice() != root.as_slice() {
            return Err(format!("root: expected {}, got {computed_root}", self.root));
        }

        Ok(Outcome::Passed)
    }
}
