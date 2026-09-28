use crate::json::{self, JsonBlock, JsonList, JsonState, JsonValidator};
use crate::{Case, Outcome};
use lean_state_transition::{Error, generate_genesis, process_block, state_transition};
use lean_types::{Hash256, Slot, State};
use serde::Deserialize;
use serde::de::IgnoredAny;
use std::fmt::Debug;
use tree_hash::TreeHash;

const PROCESS_BLOCK_WITHOUT_SLOTS: &[&str] = &[
    "test_block_at_parent_slot_rejected_when_slot_processing_skipped",
    "test_block_with_wrong_slot",
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateTransitionCase {
    pre: JsonState,
    blocks: Vec<JsonBlock>,
    post: Option<PostExpectation>,
    post_state_root: Option<String>,
    rejection_reason: Option<String>,
    #[serde(rename = "_info")]
    info: Info,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Info {
    test_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PostExpectation {
    slot: Option<u64>,
    config_genesis_time: Option<u64>,
    latest_block_header_slot: Option<u64>,
    latest_block_header_proposer_index: Option<u64>,
    latest_block_header_parent_root: Option<String>,
    latest_block_header_state_root: Option<String>,
    latest_block_header_body_root: Option<String>,
    latest_justified_slot: Option<u64>,
    latest_justified_root: Option<String>,
    #[serde(rename = "latestJustifiedRootLabel")]
    _latest_justified_root_label: Option<IgnoredAny>,
    latest_finalized_slot: Option<u64>,
    latest_finalized_root: Option<String>,
    #[serde(rename = "latestFinalizedRootLabel")]
    _latest_finalized_root_label: Option<IgnoredAny>,
    historical_block_hashes: Option<JsonList<String>>,
    historical_block_hashes_count: Option<usize>,
    justified_slots: Option<JsonList<bool>>,
    justifications_roots: Option<JsonList<String>>,
    justifications_roots_count: Option<usize>,
    #[serde(rename = "justificationsRootsLabels")]
    _justifications_roots_labels: Option<IgnoredAny>,
    justifications_validators: Option<JsonList<bool>>,
    justifications_validators_count: Option<usize>,
    validator_count: Option<usize>,
    validators: Option<JsonList<JsonValidator>>,
}

impl Case for StateTransitionCase {
    const CATEGORY: &'static str = "state_transition";

    fn run(&self) -> Result<Outcome, String> {
        let mut state = self.pre.to_state()?;
        check_genesis(&state)?;

        let blocks = self
            .blocks
            .iter()
            .map(JsonBlock::to_block)
            .collect::<Result<Vec<_>, _>>()?;

        if blocks.is_empty() && self.rejection_reason.is_some() {
            return Ok(Outcome::Skipped(
                "rejection raised while the generator built a block, no block to run",
            ));
        }

        let process_block_only = PROCESS_BLOCK_WITHOUT_SLOTS
            .iter()
            .any(|test| self.info.test_id.contains(&format!("::{test}[")));

        let result = blocks.iter().try_for_each(|block| {
            if process_block_only {
                process_block(&mut state, block)
            } else {
                state_transition(&mut state, block)
            }
        });

        match (&self.rejection_reason, result) {
            (Some(expected), Err(error)) if rejection_reason(&error) == expected => {
                Ok(Outcome::Passed)
            }
            (Some(expected), Err(error)) => Err(format!("expected {expected}, got {error:?}")),
            (Some(expected), Ok(())) => Err(format!("expected {expected}, got a valid state")),
            (None, Err(error)) => Err(format!("unexpected error {error:?}")),
            (None, Ok(())) => self.check_post(&state).map(|()| Outcome::Passed),
        }
    }
}

impl StateTransitionCase {
    fn check_post(&self, state: &State) -> Result<(), String> {
        let mut mismatches = vec![];

        if let Some(expected) = &self.post_state_root {
            compare(
                &mut mismatches,
                "postStateRoot",
                json::hash(expected)?,
                state.tree_hash_root(),
            );
        }

        let Some(post) = &self.post else {
            return mismatches_to_result(mismatches);
        };
        let header = &state.latest_block_header;

        compare_option(
            &mut mismatches,
            "slot",
            post.slot.map(Slot::new),
            state.slot,
        );
        compare_option(
            &mut mismatches,
            "configGenesisTime",
            post.config_genesis_time,
            state.config.genesis_time,
        );
        compare_option(
            &mut mismatches,
            "latestBlockHeaderSlot",
            post.latest_block_header_slot.map(Slot::new),
            header.slot,
        );
        compare_option(
            &mut mismatches,
            "latestBlockHeaderProposerIndex",
            post.latest_block_header_proposer_index,
            header.proposer_index,
        );
        compare_option(
            &mut mismatches,
            "latestBlockHeaderParentRoot",
            hash_option(&post.latest_block_header_parent_root)?,
            header.parent_root,
        );
        compare_option(
            &mut mismatches,
            "latestBlockHeaderStateRoot",
            hash_option(&post.latest_block_header_state_root)?,
            header.state_root,
        );
        compare_option(
            &mut mismatches,
            "latestBlockHeaderBodyRoot",
            hash_option(&post.latest_block_header_body_root)?,
            header.body_root,
        );
        compare_option(
            &mut mismatches,
            "latestJustifiedSlot",
            post.latest_justified_slot.map(Slot::new),
            state.latest_justified.slot,
        );
        compare_option(
            &mut mismatches,
            "latestJustifiedRoot",
            hash_option(&post.latest_justified_root)?,
            state.latest_justified.root,
        );
        compare_option(
            &mut mismatches,
            "latestFinalizedSlot",
            post.latest_finalized_slot.map(Slot::new),
            state.latest_finalized.slot,
        );
        compare_option(
            &mut mismatches,
            "latestFinalizedRoot",
            hash_option(&post.latest_finalized_root)?,
            state.latest_finalized.root,
        );
        compare_option(
            &mut mismatches,
            "historicalBlockHashesCount",
            post.historical_block_hashes_count,
            state.historical_block_hashes.len(),
        );
        compare_option(
            &mut mismatches,
            "justificationsRootsCount",
            post.justifications_roots_count,
            state.justifications_roots.len(),
        );
        compare_option(
            &mut mismatches,
            "justificationsValidatorsCount",
            post.justifications_validators_count,
            state.justifications_validators.len(),
        );
        compare_option(
            &mut mismatches,
            "validatorCount",
            post.validator_count,
            state.validators.len(),
        );

        if let Some(expected) = &post.historical_block_hashes {
            compare(
                &mut mismatches,
                "historicalBlockHashes",
                json::hashes(&expected.data)?,
                state.historical_block_hashes.clone(),
            );
        }
        if let Some(expected) = &post.justified_slots {
            compare(
                &mut mismatches,
                "justifiedSlots",
                json::bitlist(&expected.data)?,
                state.justified_slots.clone(),
            );
        }
        if let Some(expected) = &post.justifications_roots {
            compare(
                &mut mismatches,
                "justificationsRoots",
                json::hashes(&expected.data)?,
                state.justifications_roots.clone(),
            );
        }
        if let Some(expected) = &post.justifications_validators {
            compare(
                &mut mismatches,
                "justificationsValidators",
                json::bitlist(&expected.data)?,
                state.justifications_validators.clone(),
            );
        }
        if let Some(expected) = &post.validators {
            compare(
                &mut mismatches,
                "validators",
                json::validators(&expected.data)?,
                state.validators.clone(),
            );
        }

        mismatches_to_result(mismatches)
    }
}

fn check_genesis(state: &State) -> Result<(), String> {
    let is_genesis = state.slot == Slot::new(0)
        && state.historical_block_hashes.is_empty()
        && state.latest_block_header.state_root == Hash256::ZERO
        && state.justified_slots.is_empty()
        && state.justifications_roots.is_empty()
        && state.justifications_validators.is_empty();
    if !is_genesis {
        return Ok(());
    }

    let genesis = generate_genesis(state.config.genesis_time, state.validators.clone())
        .map_err(|e| format!("generate_genesis: {e:?}"))?;
    if genesis != *state {
        return Err("pre state differs from generate_genesis".into());
    }
    Ok(())
}

fn hash_option(value: &Option<String>) -> Result<Option<Hash256>, String> {
    value.as_deref().map(json::hash).transpose()
}

fn compare<T: PartialEq + Debug>(
    mismatches: &mut Vec<String>,
    field: &str,
    expected: T,
    actual: T,
) {
    if expected != actual {
        mismatches.push(format!("{field}: expected {expected:?}, got {actual:?}"));
    }
}

fn compare_option<T: PartialEq + Debug>(
    mismatches: &mut Vec<String>,
    field: &str,
    expected: Option<T>,
    actual: T,
) {
    if let Some(expected) = expected {
        compare(mismatches, field, expected, actual);
    }
}

fn mismatches_to_result(mismatches: Vec<String>) -> Result<(), String> {
    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(mismatches.join("; "))
    }
}

fn rejection_reason(error: &Error) -> &'static str {
    match error {
        Error::BlockSlotNotInFuture { .. } => "BLOCK_SLOT_NOT_IN_FUTURE",
        Error::BlockSlotMismatch { .. } => "BLOCK_SLOT_MISMATCH",
        Error::BlockOlderThanLatestHeader { .. } => "BLOCK_OLDER_THAN_LATEST_HEADER",
        Error::WrongProposer { .. } => "WRONG_PROPOSER",
        Error::ParentRootMismatch { .. } => "PARENT_ROOT_MISMATCH",
        Error::StateRootMismatch { .. } => "STATE_ROOT_MISMATCH",
        Error::EmptyValidatorRegistry => "EMPTY_VALIDATOR_REGISTRY",
        Error::TooManyAttestationData { .. } => "TOO_MANY_ATTESTATION_DATA",
        Error::JustificationVotesLengthMismatch { .. } => "JUSTIFICATION_VOTES_LENGTH_MISMATCH",
        Error::ZeroHashJustificationRoot => "ZERO_HASH_JUSTIFICATION_ROOT",
        Error::EmptyAggregationBits => "EMPTY_AGGREGATION_BITS",
        Error::ValidatorIndexOutOfRange { .. } => "VALIDATOR_INDEX_OUT_OF_RANGE",
        Error::JustifiedSlotOutOfRange { .. } => "JUSTIFIED_SLOT_OUT_OF_RANGE",
        Error::Bitfield(_) | Error::SszTypes(_) | Error::Arith(_) => "INTERNAL",
    }
}
