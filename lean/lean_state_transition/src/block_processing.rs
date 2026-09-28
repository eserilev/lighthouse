use crate::justification::bitlist_from_bools;
use crate::{Error, extend_justified_slots, is_slot_justified, process_slots};
use lean_types::config::MAX_ATTESTATIONS_DATA;
use lean_types::{
    AggregatedAttestation, AggregationBits, AttestationData, Block, BlockHeader, Checkpoint,
    Hash256, HistoricalBlockHashes, Slot, State, ValidatorIndex, VariableList,
};
use safe_arith::SafeArith;
use std::collections::{BTreeMap, HashMap, HashSet};
use tree_hash::TreeHash;

pub fn state_transition(state: &mut State, block: &Block) -> Result<(), Error> {
    process_slots(state, block.slot)?;
    process_block(state, block)?;

    let computed_state_root = state.tree_hash_root();
    if block.state_root != computed_state_root {
        return Err(Error::StateRootMismatch {
            block_state_root: block.state_root,
            computed_state_root,
        });
    }

    Ok(())
}

pub fn process_block(state: &mut State, block: &Block) -> Result<(), Error> {
    process_block_header(state, block)?;
    process_attestations(state, &block.body.attestations)
}

pub fn proposer_index_for_slot(
    slot: Slot,
    validator_count: usize,
) -> Result<ValidatorIndex, Error> {
    if validator_count == 0 {
        return Err(Error::EmptyValidatorRegistry);
    }
    Ok(slot.as_u64().safe_rem(validator_count as u64)?)
}

pub fn process_block_header(state: &mut State, block: &Block) -> Result<(), Error> {
    let parent_header = state.latest_block_header;
    let parent_root = parent_header.tree_hash_root();

    if block.slot != state.slot {
        return Err(Error::BlockSlotMismatch {
            block_slot: block.slot,
            state_slot: state.slot,
        });
    }

    if block.slot <= parent_header.slot {
        return Err(Error::BlockOlderThanLatestHeader {
            block_slot: block.slot,
            latest_header_slot: parent_header.slot,
        });
    }

    let expected_proposer = proposer_index_for_slot(state.slot, state.validators.len())?;
    if block.proposer_index != expected_proposer {
        return Err(Error::WrongProposer {
            expected: expected_proposer,
            actual: block.proposer_index,
        });
    }

    if block.parent_root != parent_root {
        return Err(Error::ParentRootMismatch {
            expected: parent_root,
            actual: block.parent_root,
        });
    }

    if parent_header.slot == Slot::new(0) {
        let genesis_checkpoint = Checkpoint {
            root: parent_root,
            slot: Slot::new(0),
        };
        state.latest_justified = genesis_checkpoint;
        state.latest_finalized = genesis_checkpoint;
    }

    let num_empty_slots = block
        .slot
        .as_u64()
        .safe_sub(parent_header.slot.as_u64())?
        .safe_sub(1)?;
    state.historical_block_hashes.push(parent_root)?;
    for _ in 0..num_empty_slots {
        state.historical_block_hashes.push(Hash256::ZERO)?;
    }

    let last_materialized_slot = Slot::new(block.slot.as_u64().safe_sub(1)?);
    extend_justified_slots(
        &mut state.justified_slots,
        state.latest_finalized.slot,
        last_materialized_slot,
    )?;

    state.latest_block_header = BlockHeader {
        slot: block.slot,
        proposer_index: block.proposer_index,
        parent_root: block.parent_root,
        state_root: Hash256::ZERO,
        body_root: block.body.tree_hash_root(),
    };

    Ok(())
}

pub fn process_attestations(
    state: &mut State,
    attestations: &[AggregatedAttestation],
) -> Result<(), Error> {
    let distinct_attestation_data = attestations
        .iter()
        .map(|attestation| &attestation.data)
        .collect::<HashSet<_>>()
        .len();
    if distinct_attestation_data > MAX_ATTESTATIONS_DATA {
        return Err(Error::TooManyAttestationData {
            count: distinct_attestation_data,
        });
    }

    let validator_count = state.validators.len();
    if validator_count == 0 {
        return Err(Error::EmptyValidatorRegistry);
    }

    let expected_vote_count = state.justifications_roots.len().safe_mul(validator_count)?;
    if state.justifications_validators.len() != expected_vote_count {
        return Err(Error::JustificationVotesLengthMismatch {
            expected: expected_vote_count,
            actual: state.justifications_validators.len(),
        });
    }

    if state.justifications_roots.contains(&Hash256::ZERO) {
        return Err(Error::ZeroHashJustificationRoot);
    }

    let flat_votes: Vec<bool> = state.justifications_validators.iter().collect();
    let mut justifications: BTreeMap<Hash256, Vec<bool>> = state
        .justifications_roots
        .iter()
        .copied()
        .zip(flat_votes.chunks(validator_count).map(<[bool]>::to_vec))
        .collect();

    let mut latest_justified = state.latest_justified;
    let mut latest_finalized = state.latest_finalized;
    let mut finalized_slot = latest_finalized.slot;
    let mut justified_slots = state.justified_slots.clone();

    let first_unfinalized_slot = finalized_slot.as_u64().safe_add(1)?;
    let root_to_slot: HashMap<Hash256, Slot> = state
        .historical_block_hashes
        .iter()
        .enumerate()
        .skip(usize::try_from(first_unfinalized_slot).unwrap_or(usize::MAX))
        .map(|(slot, root)| (*root, Slot::new(slot as u64)))
        .collect();

    for attestation in attestations {
        let AttestationData { source, target, .. } = attestation.data;

        if !is_slot_justified(&justified_slots, finalized_slot, source.slot)? {
            continue;
        }

        if is_slot_justified(&justified_slots, finalized_slot, target.slot)? {
            continue;
        }

        if !lies_on_chain(&attestation.data, &state.historical_block_hashes) {
            continue;
        }

        if target.slot <= source.slot {
            continue;
        }

        if !target.slot.is_justifiable_after(finalized_slot) {
            continue;
        }

        let voters = voting_validator_indices(&attestation.aggregation_bits)?;
        if let Some(&index) = voters.iter().find(|index| **index >= validator_count) {
            return Err(Error::ValidatorIndexOutOfRange {
                index,
                validator_count,
            });
        }

        let votes = justifications
            .entry(target.root)
            .or_insert_with(|| vec![false; validator_count]);
        for index in voters {
            if let Some(vote) = votes.get_mut(index) {
                *vote = true;
            }
        }
        let vote_count = votes.iter().filter(|vote| **vote).count();

        if vote_count.safe_mul(3)? < validator_count.safe_mul(2)? {
            continue;
        }

        if target.slot > latest_justified.slot {
            latest_justified = target;
        }

        let justified_index = target.slot.justified_index_after(finalized_slot).ok_or(
            Error::JustifiedSlotOutOfRange {
                slot: target.slot,
                finalized_slot,
            },
        )?;
        justified_slots
            .set(justified_index, true)
            .map_err(|_| Error::JustifiedSlotOutOfRange {
                slot: target.slot,
                finalized_slot,
            })?;

        justifications.remove(&target.root);

        let justifiable_slot_between = (source.slot.as_u64().safe_add(1)?..target.slot.as_u64())
            .any(|slot| Slot::new(slot).is_justifiable_after(finalized_slot));
        if source.slot <= finalized_slot || justifiable_slot_between {
            continue;
        }

        let finalized_delta = source.slot.as_u64().safe_sub(finalized_slot.as_u64())?;
        latest_finalized = source;
        finalized_slot = source.slot;

        justified_slots = bitlist_from_bools(
            justified_slots
                .iter()
                .skip(usize::try_from(finalized_delta).unwrap_or(usize::MAX)),
        )?;
        justifications.retain(|root, _| {
            root_to_slot
                .get(root)
                .is_some_and(|slot| *slot > finalized_slot)
        });
    }

    state.justifications_roots = VariableList::new(justifications.keys().copied().collect())?;
    state.justifications_validators = bitlist_from_bools(justifications.into_values().flatten())?;
    state.justified_slots = justified_slots;
    state.latest_justified = latest_justified;
    state.latest_finalized = latest_finalized;

    Ok(())
}

fn lies_on_chain(data: &AttestationData, historical_block_hashes: &HistoricalBlockHashes) -> bool {
    [data.source, data.target, data.head]
        .iter()
        .all(|checkpoint| {
            checkpoint.root != Hash256::ZERO
                && usize::try_from(checkpoint.slot.as_u64())
                    .ok()
                    .and_then(|slot| historical_block_hashes.get(slot))
                    == Some(&checkpoint.root)
        })
}

fn voting_validator_indices(aggregation_bits: &AggregationBits) -> Result<Vec<usize>, Error> {
    let indices: Vec<usize> = aggregation_bits
        .iter()
        .enumerate()
        .filter_map(|(index, bit)| bit.then_some(index))
        .collect();
    if indices.is_empty() {
        return Err(Error::EmptyAggregationBits);
    }
    Ok(indices)
}
