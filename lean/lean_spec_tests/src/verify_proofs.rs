use crate::json::{self, JsonAttestationData, JsonBytes, JsonList};
use crate::{Case, Outcome, decode_hex};
use lean_crypto::verify_single_message_aggregate_proof;
use lean_types::{PublicKeyBytes, SingleMessageAggregate, Slot};
use serde::Deserialize;
use tree_hash::TreeHash;

const INVALID_SIGNATURE: &str = "INVALID_SIGNATURE";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifySingleMessageProofsCase {
    attestation_data: JsonAttestationData,
    public_keys: Vec<String>,
    aggregation_bits: JsonList<bool>,
    message: String,
    slot: u64,
    proof: JsonBytes,
    rejection_reason: Option<String>,
}

impl Case for VerifySingleMessageProofsCase {
    const CATEGORY: &'static str = "verify_single_message_proofs";

    fn run(&self) -> Result<Outcome, String> {
        let message = json::hash(&self.message)?;
        let attestation_data = self.attestation_data.to_attestation_data()?;
        if attestation_data.tree_hash_root() != message {
            return Err("message is not the attestation data root".into());
        }

        let public_keys = self
            .public_keys
            .iter()
            .map(|public_key| {
                PublicKeyBytes::new(decode_hex(public_key)?).map_err(|e| format!("{e:?}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let aggregate = SingleMessageAggregate {
            participants: json::bitlist(&self.aggregation_bits.data)?,
            proof: self.proof.to_proof_bytes()?,
        };
        let result = verify_single_message_aggregate_proof(
            &aggregate,
            &public_keys,
            message,
            Slot::new(self.slot),
        );

        match (self.rejection_reason.as_deref(), result) {
            (Some(INVALID_SIGNATURE), Err(_)) => Ok(Outcome::Passed),
            (Some(expected), Err(error)) => Err(format!("expected {expected}, got {error:?}")),
            (Some(expected), Ok(())) => Err(format!("expected {expected}, got a valid proof")),
            (None, Err(error)) => Err(format!("unexpected error {error:?}")),
            (None, Ok(())) => Ok(Outcome::Passed),
        }
    }
}
