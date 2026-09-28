use crate::{Case, Outcome};
use lean_types::Slot;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JustifiabilityCase {
    slot: u64,
    finalized_slot: u64,
    output: JustifiabilityOutput,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JustifiabilityOutput {
    delta: i128,
    is_justifiable: bool,
}

impl Case for JustifiabilityCase {
    const CATEGORY: &'static str = "justifiability";

    fn run(&self) -> Result<Outcome, String> {
        let delta = i128::from(self.slot) - i128::from(self.finalized_slot);
        if delta != self.output.delta {
            return Err(format!(
                "delta: expected {}, got {delta}",
                self.output.delta
            ));
        }

        let is_justifiable =
            Slot::new(self.slot).is_justifiable_after(Slot::new(self.finalized_slot));
        if is_justifiable != self.output.is_justifiable {
            return Err(format!(
                "is_justifiable: expected {}, got {is_justifiable}",
                self.output.is_justifiable
            ));
        }

        Ok(Outcome::Passed)
    }
}
