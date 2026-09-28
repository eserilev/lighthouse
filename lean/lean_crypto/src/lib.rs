use lean_types::{
    AggregationBits, Hash256, PublicKeyBytes, SignedBlock, SingleMessageAggregate, Slot, Validators,
};
use leansig_wrapper::{XmssPublicKey, xmss_public_key_from_ssz};
use rec_aggregation::{
    MultiMessageAggregateSignature, SingleMessageAggregateSignature, init_aggregation_bytecode,
    verify_multi_message_aggregate, verify_single_message_aggregate,
};
use std::panic::{AssertUnwindSafe, catch_unwind};
use tree_hash::TreeHash;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    EmptyAggregationBits,
    ValidatorIndexOutOfRange {
        index: usize,
        validator_count: usize,
    },
    ProposerIndexOutOfRange {
        index: u64,
        validator_count: usize,
    },
    InvalidPublicKey,
    SlotOutOfRange(Slot),
    PublicKeyCountMismatch {
        expected: usize,
        actual: usize,
    },
    MessageCountMismatch {
        expected: usize,
        actual: usize,
    },
    DecompressionFailed,
    MessageMismatch {
        component: usize,
    },
    SlotMismatch {
        component: usize,
    },
    VerificationFailed(String),
    VerifierPanicked,
}

pub fn verify_single_message_aggregate_proof(
    aggregate: &SingleMessageAggregate,
    public_keys: &[PublicKeyBytes],
    message: Hash256,
    slot: Slot,
) -> Result<(), Error> {
    let participant_count = participant_indices(&aggregate.participants)?.len();
    if public_keys.len() != participant_count {
        return Err(Error::PublicKeyCountMismatch {
            expected: participant_count,
            actual: public_keys.len(),
        });
    }

    let public_keys = decode_public_keys(public_keys.iter())?;
    let slot = slot_to_u32(slot)?;
    let proof = &aggregate.proof;

    guard_verifier(|| {
        let signature =
            SingleMessageAggregateSignature::decompress_without_pubkeys(proof, public_keys)
                .ok_or(Error::DecompressionFailed)?;
        if signature.info.without_pubkeys.message != message.0 {
            return Err(Error::MessageMismatch { component: 0 });
        }
        if signature.info.without_pubkeys.slot != slot {
            return Err(Error::SlotMismatch { component: 0 });
        }
        verify_single_message_aggregate(&signature)
            .map(|_| ())
            .map_err(|e| Error::VerificationFailed(format!("{e:?}")))
    })
}

pub fn verify_block_signatures(
    signed_block: &SignedBlock,
    validators: &Validators,
) -> Result<(), Error> {
    let block = &signed_block.block;
    let validator_count = validators.len();
    let mut public_keys_per_message = vec![];
    let mut messages = vec![];

    for attestation in block.body.attestations.iter() {
        let voters = participant_indices(&attestation.aggregation_bits)?;
        let public_keys = voters
            .iter()
            .map(|index| {
                validators
                    .get(*index)
                    .map(|validator| &validator.attestation_public_key)
                    .ok_or(Error::ValidatorIndexOutOfRange {
                        index: *index,
                        validator_count,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        public_keys_per_message.push(public_keys);
        messages.push((attestation.data.tree_hash_root(), attestation.data.slot));
    }

    let proposer = usize::try_from(block.proposer_index)
        .ok()
        .and_then(|index| validators.get(index))
        .ok_or(Error::ProposerIndexOutOfRange {
            index: block.proposer_index,
            validator_count,
        })?;
    public_keys_per_message.push(vec![&proposer.proposal_public_key]);
    messages.push((block.tree_hash_root(), block.slot));

    verify_multi_message_proof(
        &public_keys_per_message,
        &messages,
        &signed_block.proof.proof,
    )
}

fn verify_multi_message_proof(
    public_keys_per_message: &[Vec<&PublicKeyBytes>],
    messages: &[(Hash256, Slot)],
    proof: &[u8],
) -> Result<(), Error> {
    if public_keys_per_message.len() != messages.len() {
        return Err(Error::MessageCountMismatch {
            expected: public_keys_per_message.len(),
            actual: messages.len(),
        });
    }

    let messages = messages
        .iter()
        .map(|(message, slot)| Ok((message.0, slot_to_u32(*slot)?)))
        .collect::<Result<Vec<_>, Error>>()?;
    let public_keys_per_message = public_keys_per_message
        .iter()
        .map(|public_keys| decode_public_keys(public_keys.iter().copied()))
        .collect::<Result<Vec<_>, _>>()?;

    guard_verifier(|| {
        let signature = MultiMessageAggregateSignature::decompress_without_pubkeys(
            proof,
            public_keys_per_message,
        )
        .ok_or(Error::DecompressionFailed)?;
        if signature.info.len() != messages.len() {
            return Err(Error::MessageCountMismatch {
                expected: messages.len(),
                actual: signature.info.len(),
            });
        }
        for (component, (info, (message, slot))) in signature.info.iter().zip(&messages).enumerate()
        {
            if info.without_pubkeys.message != *message {
                return Err(Error::MessageMismatch { component });
            }
            if info.without_pubkeys.slot != *slot {
                return Err(Error::SlotMismatch { component });
            }
        }
        verify_multi_message_aggregate(&signature)
            .map(|_| ())
            .map_err(|e| Error::VerificationFailed(format!("{e:?}")))
    })
}

fn participant_indices(aggregation_bits: &AggregationBits) -> Result<Vec<usize>, Error> {
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

fn decode_public_keys<'a>(
    public_keys: impl Iterator<Item = &'a PublicKeyBytes>,
) -> Result<Vec<XmssPublicKey>, Error> {
    public_keys
        .map(|public_key| {
            xmss_public_key_from_ssz(public_key).map_err(|()| Error::InvalidPublicKey)
        })
        .collect()
}

fn slot_to_u32(slot: Slot) -> Result<u32, Error> {
    u32::try_from(slot.as_u64()).map_err(|_| Error::SlotOutOfRange(slot))
}

fn guard_verifier(verify: impl FnOnce() -> Result<(), Error>) -> Result<(), Error> {
    init_aggregation_bytecode();
    catch_unwind(AssertUnwindSafe(verify)).unwrap_or(Err(Error::VerifierPanicked))
}
