use crate::json::{JsonSignedBlock, JsonState};
use crate::{Case, Outcome};
use lean_crypto::{Error, verify_block_signatures};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifySignaturesCase {
    anchor_state: JsonState,
    signed_block: JsonSignedBlock,
    rejection_reason: Option<String>,
}

impl Case for VerifySignaturesCase {
    const CATEGORY: &'static str = "verify_signatures";

    fn run(&self) -> Result<Outcome, String> {
        let state = self.anchor_state.to_state()?;
        let signed_block = self.signed_block.to_signed_block()?;
        let result = verify_block_signatures(&signed_block, &state.validators);

        match (&self.rejection_reason, result) {
            (Some(expected), Err(error)) if rejection_reason(&error) == expected => {
                Ok(Outcome::Passed)
            }
            (Some(expected), Err(error)) => Err(format!("expected {expected}, got {error:?}")),
            (Some(expected), Ok(())) => Err(format!("expected {expected}, got a valid proof")),
            (None, Err(error)) => Err(format!("unexpected error {error:?}")),
            (None, Ok(())) => Ok(Outcome::Passed),
        }
    }
}

fn rejection_reason(error: &Error) -> &'static str {
    match error {
        Error::EmptyAggregationBits => "EMPTY_AGGREGATION_BITS",
        Error::ValidatorIndexOutOfRange { .. } => "VALIDATOR_INDEX_OUT_OF_RANGE",
        Error::ProposerIndexOutOfRange { .. } => "PROPOSER_INDEX_OUT_OF_RANGE",
        _ => "INVALID_BLOCK_PROOF",
    }
}
