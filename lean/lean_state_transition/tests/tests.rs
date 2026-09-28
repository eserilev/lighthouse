use lean_state_transition::*;
use lean_types::*;
use proptest::collection::vec;
use proptest::prelude::*;
use tree_hash::TreeHash;

fn validators(count: usize) -> Validators {
    Validators::new(
        (0..count)
            .map(|index| Validator {
                attestation_public_key: FixedVector::new(vec![0; 52]).unwrap(),
                proposal_public_key: FixedVector::new(vec![0; 52]).unwrap(),
                index: index as u64,
            })
            .collect(),
    )
    .unwrap()
}

fn genesis(validator_count: usize) -> State {
    generate_genesis(0, validators(validator_count)).unwrap()
}

fn empty_block(state: &State, slot: Slot) -> Block {
    let mut advanced = state.clone();
    process_slots(&mut advanced, slot).unwrap();
    let mut block = Block {
        slot,
        proposer_index: proposer_index_for_slot(slot, state.validators.len()).unwrap(),
        parent_root: advanced.latest_block_header.tree_hash_root(),
        state_root: Hash256::ZERO,
        body: BlockBody::default(),
    };
    process_block(&mut advanced, &block).unwrap();
    block.state_root = advanced.tree_hash_root();
    block
}

fn chain(validator_count: usize, slot_gaps: &[u64]) -> State {
    let mut state = genesis(validator_count);
    let mut slot = 0;
    for gap in slot_gaps {
        slot += gap;
        let block = empty_block(&state, Slot::new(slot));
        state_transition(&mut state, &block).unwrap();
    }
    state
}

fn bitlist<N: ssz_types::typenum::Unsigned + Clone>(bits: &[bool]) -> BitList<N> {
    let mut bitlist = BitList::with_capacity(bits.len()).unwrap();
    for (index, bit) in bits.iter().enumerate() {
        bitlist.set(index, *bit).unwrap();
    }
    bitlist
}

fn checkpoint_at(state: &State, slot: usize) -> Checkpoint {
    Checkpoint {
        root: state.historical_block_hashes[slot],
        slot: Slot::new(slot as u64),
    }
}

#[test]
fn empty_registry_rejects_block() {
    let state = genesis(0);
    let mut advanced = state.clone();
    process_slots(&mut advanced, Slot::new(1)).unwrap();
    let block = Block {
        slot: Slot::new(1),
        proposer_index: 0,
        parent_root: advanced.latest_block_header.tree_hash_root(),
        state_root: Hash256::ZERO,
        body: BlockBody::default(),
    };

    let mut state = state;
    assert_eq!(
        state_transition(&mut state, &block),
        Err(Error::EmptyValidatorRegistry)
    );
}

#[test]
fn process_slots_rejects_non_future_slot() {
    let mut state = genesis(4);
    process_slots(&mut state, Slot::new(2)).unwrap();
    for target_slot in [0, 1, 2] {
        assert_eq!(
            process_slots(&mut state.clone(), Slot::new(target_slot)),
            Err(Error::BlockSlotNotInFuture {
                state_slot: Slot::new(2),
                target_slot: Slot::new(target_slot),
            })
        );
    }
}

#[test]
fn process_slots_caches_the_pre_block_state_root_once() {
    let state = genesis(4);
    let genesis_root = state.tree_hash_root();
    let mut advanced = state;
    process_slots(&mut advanced, Slot::new(5)).unwrap();
    assert_eq!(advanced.slot, Slot::new(5));
    assert_eq!(advanced.latest_block_header.state_root, genesis_root);
}

#[test]
fn skipped_slots_record_zero_hashes() {
    let state = chain(4, &[3]);
    assert_eq!(state.historical_block_hashes.len(), 3);
    assert_ne!(state.historical_block_hashes[0], Hash256::ZERO);
    assert_eq!(state.historical_block_hashes[1], Hash256::ZERO);
    assert_eq!(state.historical_block_hashes[2], Hash256::ZERO);
}

#[test]
fn is_slot_justified_reads_the_tracked_range() {
    let justified_slots: JustifiedSlots = bitlist(&[false, true]);
    let finalized = Slot::new(10);
    assert_eq!(
        is_slot_justified(&justified_slots, finalized, Slot::new(9)),
        Ok(true)
    );
    assert_eq!(
        is_slot_justified(&justified_slots, finalized, Slot::new(10)),
        Ok(true)
    );
    assert_eq!(
        is_slot_justified(&justified_slots, finalized, Slot::new(11)),
        Ok(false)
    );
    assert_eq!(
        is_slot_justified(&justified_slots, finalized, Slot::new(12)),
        Ok(true)
    );
    assert_eq!(
        is_slot_justified(&justified_slots, finalized, Slot::new(13)),
        Err(Error::JustifiedSlotOutOfRange {
            slot: Slot::new(13),
            finalized_slot: finalized,
        })
    );
}

#[test]
fn extend_justified_slots_only_grows() {
    let original: JustifiedSlots = bitlist(&[true, false]);
    let finalized = Slot::new(10);

    for slot in [5, 10, 11, 12] {
        let mut justified_slots = original.clone();
        extend_justified_slots(&mut justified_slots, finalized, Slot::new(slot)).unwrap();
        assert_eq!(justified_slots, original, "slot {slot}");
    }

    let mut justified_slots = original.clone();
    extend_justified_slots(&mut justified_slots, finalized, Slot::new(15)).unwrap();
    assert_eq!(
        justified_slots,
        bitlist::<config::HistoricalRootsLimit>(&[true, false, false, false, false])
    );
}

#[derive(Debug, Clone)]
struct AttestationSpec {
    source: usize,
    target: usize,
    head: usize,
    random_target_root: Option<Hash256>,
    bits: Vec<bool>,
}

fn attestation_spec(validator_count: usize) -> impl Strategy<Value = AttestationSpec> {
    (
        any::<prop::sample::Index>(),
        any::<prop::sample::Index>(),
        any::<prop::sample::Index>(),
        prop::option::weighted(0.1, any::<[u8; 32]>().prop_map(Hash256::from)),
        vec(prop::bool::weighted(0.8), validator_count),
    )
        .prop_map(
            |(source, target, head, random_target_root, bits)| AttestationSpec {
                source: source.index(usize::MAX),
                target: target.index(usize::MAX),
                head: head.index(usize::MAX),
                random_target_root,
                bits,
            },
        )
}

fn scenario() -> impl Strategy<Value = (usize, Vec<u64>, Vec<Vec<AttestationSpec>>)> {
    (1usize..=8).prop_flat_map(|validator_count| {
        (
            Just(validator_count),
            vec(1u64..=3, 1..10),
            vec(vec(attestation_spec(validator_count), 0..10), 1..4),
        )
    })
}

fn build_attestations(state: &State, specs: &[AttestationSpec]) -> Vec<AggregatedAttestation> {
    let history_len = state.historical_block_hashes.len();
    specs
        .iter()
        .map(|spec| {
            let mut target = checkpoint_at(state, spec.target % history_len);
            if let Some(root) = spec.random_target_root {
                target.root = root;
            }
            AggregatedAttestation {
                aggregation_bits: bitlist(&spec.bits),
                data: AttestationData {
                    slot: state.slot,
                    head: checkpoint_at(state, spec.head % history_len),
                    target,
                    source: checkpoint_at(state, spec.source % history_len),
                },
            }
        })
        .collect()
}

fn check_invariants(pre: &State, post: &State) -> Result<(), TestCaseError> {
    let validator_count = post.validators.len();
    prop_assert_eq!(
        post.justifications_validators.len(),
        post.justifications_roots.len() * validator_count
    );
    prop_assert!(
        post.justifications_roots
            .windows(2)
            .all(|pair| pair[0] < pair[1])
    );
    prop_assert!(!post.justifications_roots.contains(&Hash256::ZERO));
    prop_assert!(post.latest_finalized.slot <= post.latest_justified.slot);
    prop_assert!(post.latest_finalized.slot >= pre.latest_finalized.slot);
    prop_assert!(post.latest_justified.slot >= pre.latest_justified.slot);
    let highest_justified_slot = post
        .justified_slots
        .iter()
        .enumerate()
        .filter_map(|(index, justified)| justified.then_some(index as u64))
        .max()
        .map(|index| post.latest_finalized.slot.as_u64() + 1 + index);
    if let Some(slot) = highest_justified_slot {
        prop_assert!(slot <= post.latest_justified.slot.as_u64());
    }
    prop_assert_eq!(
        post.justified_slots.len() as u64,
        post.latest_block_header
            .slot
            .as_u64()
            .saturating_sub(1)
            .saturating_sub(post.latest_finalized.slot.as_u64())
    );
    Ok(())
}

proptest! {
    #[test]
    fn process_attestations_keeps_invariants((validator_count, slot_gaps, batches) in scenario()) {
        let mut state = chain(validator_count, &slot_gaps);
        for specs in batches {
            let attestations = build_attestations(&state, &specs);
            let pre = state.clone();
            match process_attestations(&mut state, &attestations) {
                Ok(()) => check_invariants(&pre, &state)?,
                Err(Error::EmptyAggregationBits | Error::TooManyAttestationData { .. }) => {
                    state = pre;
                }
                Err(error) => prop_assert!(false, "unexpected error {:?}", error),
            }
        }
    }

    #[test]
    fn full_vote_justifies_a_justifiable_target(
        validator_count in 1usize..=8,
        slot_gaps in vec(1u64..=3, 2..10),
        target in any::<prop::sample::Index>(),
    ) {
        let mut state = chain(validator_count, &slot_gaps);
        let history_len = state.historical_block_hashes.len();
        let target_slot = 1 + target.index(history_len - 1);
        let target = checkpoint_at(&state, target_slot);
        prop_assume!(target.root != Hash256::ZERO);
        prop_assume!(target.slot.is_justifiable_after(state.latest_finalized.slot));

        let source = checkpoint_at(&state, 0);
        let attestation = AggregatedAttestation {
            aggregation_bits: bitlist(&vec![true; validator_count]),
            data: AttestationData { slot: state.slot, head: target, target, source },
        };
        process_attestations(&mut state, &[attestation]).unwrap();

        prop_assert!(is_slot_justified(&state.justified_slots, state.latest_finalized.slot, target.slot).unwrap());
        prop_assert!(state.latest_justified.slot >= target.slot);
    }

    #[test]
    fn minority_vote_does_not_justify(
        validator_count in 2usize..=8,
        slot_gaps in vec(1u64..=3, 2..10),
        target in any::<prop::sample::Index>(),
        voters in any::<prop::sample::Index>(),
    ) {
        let mut state = chain(validator_count, &slot_gaps);
        let history_len = state.historical_block_hashes.len();
        let target = checkpoint_at(&state, 1 + target.index(history_len - 1));
        prop_assume!(target.root != Hash256::ZERO);

        let max_minority = (2 * validator_count).div_ceil(3) - 1;
        prop_assume!(max_minority > 0);
        let voter_count = 1 + voters.index(max_minority);
        let bits: Vec<bool> = (0..validator_count).map(|index| index < voter_count).collect();
        let attestation = AggregatedAttestation {
            aggregation_bits: bitlist(&bits),
            data: AttestationData { slot: state.slot, head: target, target, source: checkpoint_at(&state, 0) },
        };
        let pre = state.clone();
        process_attestations(&mut state, &[attestation]).unwrap();

        prop_assert_eq!(state.latest_justified, pre.latest_justified);
        prop_assert_eq!(state.justified_slots, pre.justified_slots);
    }

    #[test]
    fn consecutive_full_votes_finalize_the_first_target(
        validator_count in 1usize..=8,
        slot_gaps in vec(1u64..=2, 3..10),
        first in any::<prop::sample::Index>(),
    ) {
        let mut state = chain(validator_count, &slot_gaps);
        let history_len = state.historical_block_hashes.len();
        let first_slot = 1 + first.index(history_len - 2);
        let first = checkpoint_at(&state, first_slot);
        let second = checkpoint_at(&state, first_slot + 1);
        prop_assume!(first.root != Hash256::ZERO && second.root != Hash256::ZERO);
        prop_assume!(first.slot.is_justifiable_after(state.latest_finalized.slot));
        prop_assume!(second.slot.is_justifiable_after(state.latest_finalized.slot));

        let full_vote = |source: Checkpoint, target: Checkpoint| AggregatedAttestation {
            aggregation_bits: bitlist(&vec![true; validator_count]),
            data: AttestationData { slot: state.slot, head: target, target, source },
        };
        let attestations = [
            full_vote(checkpoint_at(&state, 0), first),
            full_vote(first, second),
        ];
        process_attestations(&mut state, &attestations).unwrap();

        prop_assert_eq!(state.latest_finalized, first);
        prop_assert_eq!(state.latest_justified, second);
    }

    #[test]
    fn justifiable_slot_between_blocks_finalization(
        validator_count in 1usize..=8,
        slot_gaps in vec(1u64..=2, 4..10),
        first in any::<prop::sample::Index>(),
    ) {
        let mut state = chain(validator_count, &slot_gaps);
        let history_len = state.historical_block_hashes.len();
        let first_slot = 1 + first.index(history_len - 3);
        let first = checkpoint_at(&state, first_slot);
        let skipped = Slot::new(first_slot as u64 + 1);
        let last = checkpoint_at(&state, first_slot + 2);
        prop_assume!(first.root != Hash256::ZERO && last.root != Hash256::ZERO);
        prop_assume!(first.slot.is_justifiable_after(state.latest_finalized.slot));
        prop_assume!(skipped.is_justifiable_after(state.latest_finalized.slot));
        prop_assume!(last.slot.is_justifiable_after(state.latest_finalized.slot));

        let full_vote = |source: Checkpoint, target: Checkpoint| AggregatedAttestation {
            aggregation_bits: bitlist(&vec![true; validator_count]),
            data: AttestationData { slot: state.slot, head: target, target, source },
        };
        let attestations = [
            full_vote(checkpoint_at(&state, 0), first),
            full_vote(first, last),
        ];
        let pre = state.clone();
        process_attestations(&mut state, &attestations).unwrap();

        prop_assert_eq!(state.latest_finalized, pre.latest_finalized);
        prop_assert_eq!(state.latest_justified, last);
    }
}
