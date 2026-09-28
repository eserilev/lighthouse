use lean_types::config::{XMSS_DIMENSION, XMSS_LOG_LIFETIME};
use lean_types::*;
use proptest::collection::vec;
use proptest::prelude::*;
use ssz::{Decode, Encode};
use std::collections::HashSet;
use tree_hash::TreeHash;

const SIGNATURE_BYTES: usize = 2536;
const SIGNATURE_HASHES_OFFSET: std::ops::Range<usize> = 32..36;

fn hash() -> impl Strategy<Value = Hash256> {
    any::<[u8; 32]>().prop_map(Hash256::from)
}

fn slot() -> impl Strategy<Value = Slot> {
    any::<u64>().prop_map(Slot::new)
}

fn checkpoint() -> impl Strategy<Value = Checkpoint> {
    (hash(), slot()).prop_map(|(root, slot)| Checkpoint { root, slot })
}

fn attestation_data() -> impl Strategy<Value = AttestationData> {
    (slot(), checkpoint(), checkpoint(), checkpoint()).prop_map(|(slot, head, target, source)| {
        AttestationData {
            slot,
            head,
            target,
            source,
        }
    })
}

fn bitlist<N: ssz_types::typenum::Unsigned + Clone + std::fmt::Debug>(
    max_len: usize,
) -> impl Strategy<Value = BitList<N>> {
    vec(any::<bool>(), 0..max_len).prop_map(|bits| {
        let mut bitlist = BitList::with_capacity(bits.len()).unwrap();
        for (index, bit) in bits.into_iter().enumerate() {
            bitlist.set(index, bit).unwrap();
        }
        bitlist
    })
}

fn aggregated_attestation() -> impl Strategy<Value = AggregatedAttestation> {
    (bitlist(64), attestation_data()).prop_map(|(aggregation_bits, data)| AggregatedAttestation {
        aggregation_bits,
        data,
    })
}

fn proof_bytes() -> impl Strategy<Value = ProofBytes> {
    vec(any::<u8>(), 0..256).prop_map(|bytes| ProofBytes::new(bytes).unwrap())
}

fn block() -> impl Strategy<Value = Block> {
    (
        slot(),
        any::<u64>(),
        hash(),
        hash(),
        vec(aggregated_attestation(), 0..4),
    )
        .prop_map(
            |(slot, proposer_index, parent_root, state_root, attestations)| Block {
                slot,
                proposer_index,
                parent_root,
                state_root,
                body: BlockBody {
                    attestations: VariableList::new(attestations).unwrap(),
                },
            },
        )
}

fn hash_digest() -> impl Strategy<Value = HashDigestVector> {
    any::<[u32; 8]>().prop_map(|digest| FixedVector::new(digest.to_vec()).unwrap())
}

fn hash_digests(len: usize) -> impl Strategy<Value = HashDigestList> {
    vec(hash_digest(), len).prop_map(|digests| VariableList::new(digests).unwrap())
}

fn signature() -> impl Strategy<Value = Signature> {
    (
        hash_digests(XMSS_LOG_LIFETIME),
        any::<[u32; 7]>(),
        hash_digests(XMSS_DIMENSION),
    )
        .prop_map(|(siblings, rho, hashes)| {
            Signature::new(
                HashTreeOpening { siblings },
                FixedVector::new(rho.to_vec()).unwrap(),
                hashes,
            )
            .unwrap()
        })
}

fn validators(max_len: usize) -> impl Strategy<Value = Vec<Validator>> {
    vec((any::<[u8; 52]>(), any::<[u8; 52]>()), 0..max_len).prop_map(|keys| {
        keys.into_iter()
            .enumerate()
            .map(|(index, (attestation_key, proposal_key))| Validator {
                attestation_public_key: FixedVector::new(attestation_key.to_vec()).unwrap(),
                proposal_public_key: FixedVector::new(proposal_key.to_vec()).unwrap(),
                index: index as u64,
            })
            .collect()
    })
}

fn state() -> impl Strategy<Value = State> {
    (
        (any::<u64>(), slot(), block(), checkpoint(), checkpoint()),
        (
            vec(hash(), 0..16),
            bitlist(64),
            validators(8),
            vec(hash(), 0..4),
            bitlist(64),
        ),
    )
        .prop_map(
            |(
                (genesis_time, slot, header_block, latest_justified, latest_finalized),
                (historical, justified_slots, validators, roots, votes),
            )| State {
                config: GenesisConfig { genesis_time },
                slot,
                latest_block_header: BlockHeader {
                    slot: header_block.slot,
                    proposer_index: header_block.proposer_index,
                    parent_root: header_block.parent_root,
                    state_root: header_block.state_root,
                    body_root: header_block.body.tree_hash_root(),
                },
                latest_justified,
                latest_finalized,
                historical_block_hashes: VariableList::new(historical).unwrap(),
                justified_slots,
                validators: Validators::new(validators).unwrap(),
                justifications_roots: VariableList::new(roots).unwrap(),
                justifications_validators: votes,
            },
        )
}

fn assert_round_trip<T: Encode + Decode + TreeHash + PartialEq + std::fmt::Debug>(value: &T) {
    let bytes = value.as_ssz_bytes();
    assert_eq!(bytes.len(), value.ssz_bytes_len());
    let decoded = T::from_ssz_bytes(&bytes).unwrap();
    assert_eq!(&decoded, value);
    assert_eq!(decoded.tree_hash_root(), value.tree_hash_root());
}

fn naive_justifiable_deltas(max_delta: u64) -> HashSet<u64> {
    let mut deltas: HashSet<u64> = (0..=5).collect();
    for n in 0.. {
        if n * n > max_delta {
            break;
        }
        deltas.insert(n * n);
        deltas.insert(n * (n + 1));
    }
    deltas
}

#[test]
fn justifiable_matches_naive_definition_for_small_deltas() {
    let max_delta = 1 << 16;
    let justifiable = naive_justifiable_deltas(max_delta);
    for delta in 0..=max_delta {
        assert_eq!(
            Slot::new(delta).is_justifiable_after(Slot::new(0)),
            justifiable.contains(&delta),
            "delta {delta}"
        );
    }
}

proptest! {
    #[test]
    fn checkpoint_round_trips(value in checkpoint()) {
        assert_round_trip(&value);
    }

    #[test]
    fn attestation_data_round_trips(value in attestation_data()) {
        assert_round_trip(&value);
    }

    #[test]
    fn signed_attestation_round_trips(
        validator_index in any::<u64>(),
        data in attestation_data(),
        signature in signature(),
    ) {
        assert_round_trip(&SignedAttestation { validator_index, data, signature });
    }

    #[test]
    fn signed_aggregated_attestation_round_trips(
        data in attestation_data(),
        participants in bitlist(64),
        proof in proof_bytes(),
    ) {
        assert_round_trip(&SignedAggregatedAttestation {
            data,
            proof: SingleMessageAggregate { participants, proof },
        });
    }

    #[test]
    fn signed_block_round_trips(block in block(), proof in proof_bytes()) {
        assert_round_trip(&SignedBlock { block, proof: MultiMessageAggregate { proof } });
    }

    #[test]
    fn state_round_trips(state in state()) {
        assert_round_trip(&state);
    }

    #[test]
    fn signature_encodes_to_fixed_length(signature in signature()) {
        prop_assert_eq!(signature.as_ssz_bytes().len(), SIGNATURE_BYTES);
    }

    #[test]
    fn signature_decoding_is_canonical(
        signature in signature(),
        position in 0..SIGNATURE_BYTES,
        byte in any::<u8>(),
    ) {
        let mut bytes = signature.as_ssz_bytes();
        bytes[position] = byte;
        if let Ok(decoded) = Signature::from_ssz_bytes(&bytes) {
            prop_assert_eq!(decoded.as_ssz_bytes(), bytes);
        }
    }

    #[test]
    fn signature_rejects_moved_hashes_offset(signature in signature(), shift in 1u32..=64) {
        let bytes = signature.as_ssz_bytes();
        let offset = u32::from_le_bytes(bytes[SIGNATURE_HASHES_OFFSET].try_into().unwrap());
        for moved in [offset.wrapping_sub(shift * 32), offset.wrapping_add(shift * 32), offset + shift] {
            let mut tampered = bytes.clone();
            tampered[SIGNATURE_HASHES_OFFSET].copy_from_slice(&moved.to_le_bytes());
            prop_assert!(Signature::from_ssz_bytes(&tampered).is_err());
        }
    }

    #[test]
    fn signature_new_rejects_wrong_counts(
        sibling_count in 0usize..40,
        hash_count in 0usize..50,
        rho in any::<[u32; 7]>(),
    ) {
        prop_assume!(sibling_count != XMSS_LOG_LIFETIME || hash_count != XMSS_DIMENSION);
        let digests = |len| VariableList::new(vec![FixedVector::new(vec![0; 8]).unwrap(); len]).unwrap();
        let signature = Signature::new(
            HashTreeOpening { siblings: digests(sibling_count) },
            FixedVector::new(rho.to_vec()).unwrap(),
            digests(hash_count),
        );
        prop_assert!(signature.is_err());
    }

    #[test]
    fn signature_rejects_wrong_lengths(bytes in vec(any::<u8>(), 0..3000)) {
        prop_assume!(bytes.len() != SIGNATURE_BYTES);
        prop_assert!(Signature::from_ssz_bytes(&bytes).is_err());
    }

    #[test]
    fn validators_reject_index_not_matching_position(
        validators in validators(8),
        position in any::<prop::sample::Index>(),
        index in any::<u64>(),
    ) {
        prop_assume!(!validators.is_empty());
        let position = position.index(validators.len());
        prop_assume!(index != position as u64);
        let mut validators = validators;
        let encoded_valid = VariableList::<Validator, config::ValidatorRegistryLimit>::new(validators.clone())
            .unwrap()
            .as_ssz_bytes();
        prop_assert_eq!(Validators::from_ssz_bytes(&encoded_valid).unwrap().len(), validators.len());

        validators[position].index = index;
        let encoded_invalid = VariableList::<Validator, config::ValidatorRegistryLimit>::new(validators.clone())
            .unwrap()
            .as_ssz_bytes();
        prop_assert!(Validators::from_ssz_bytes(&encoded_invalid).is_err());
        prop_assert!(Validators::new(validators).is_err());
    }

    #[test]
    fn squares_and_pronics_are_justifiable(n in 0u64..=u32::MAX as u64, finalized in any::<u64>()) {
        for delta in [n * n, n * (n + 1)] {
            if let Some(slot) = finalized.checked_add(delta) {
                prop_assert!(Slot::new(slot).is_justifiable_after(Slot::new(finalized)));
            }
        }
    }

    #[test]
    fn gaps_between_squares_are_not_justifiable(
        n in 3u64..u32::MAX as u64,
        gap in any::<prop::sample::Index>(),
        finalized in any::<u64>(),
    ) {
        let offset = gap.index(2 * n as usize) as u64 + 1;
        prop_assume!(offset != n);
        let delta = n * n + offset;
        if let Some(slot) = finalized.checked_add(delta) {
            prop_assert!(!Slot::new(slot).is_justifiable_after(Slot::new(finalized)));
        }
    }

    #[test]
    fn slots_before_finalized_are_not_justifiable(slot in slot(), finalized in slot()) {
        prop_assume!(slot < finalized);
        prop_assert!(!slot.is_justifiable_after(finalized));
    }

    #[test]
    fn justified_index_matches_definition(slot in slot(), finalized in slot()) {
        let expected = (slot > finalized).then(|| (slot.as_u64() - finalized.as_u64() - 1) as usize);
        prop_assert_eq!(slot.justified_index_after(finalized), expected);
    }
}
