mod block_processing;
mod genesis;
mod justification;
mod slot_processing;

pub use block_processing::{
    process_attestations, process_block, process_block_header, proposer_index_for_slot,
    state_transition,
};
pub use genesis::generate_genesis;
pub use justification::{extend_justified_slots, is_slot_justified};
pub use slot_processing::process_slots;

use lean_types::{Hash256, Slot, ValidatorIndex};
use safe_arith::ArithError;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    BlockSlotNotInFuture {
        state_slot: Slot,
        target_slot: Slot,
    },
    BlockSlotMismatch {
        block_slot: Slot,
        state_slot: Slot,
    },
    BlockOlderThanLatestHeader {
        block_slot: Slot,
        latest_header_slot: Slot,
    },
    WrongProposer {
        expected: ValidatorIndex,
        actual: ValidatorIndex,
    },
    ParentRootMismatch {
        expected: Hash256,
        actual: Hash256,
    },
    StateRootMismatch {
        block_state_root: Hash256,
        computed_state_root: Hash256,
    },
    EmptyValidatorRegistry,
    TooManyAttestationData {
        count: usize,
    },
    JustificationVotesLengthMismatch {
        expected: usize,
        actual: usize,
    },
    ZeroHashJustificationRoot,
    EmptyAggregationBits,
    ValidatorIndexOutOfRange {
        index: usize,
        validator_count: usize,
    },
    JustifiedSlotOutOfRange {
        slot: Slot,
        finalized_slot: Slot,
    },
    Bitfield(ssz::BitfieldError),
    SszTypes(ssz_types::Error),
    Arith(ArithError),
}

impl From<ssz::BitfieldError> for Error {
    fn from(e: ssz::BitfieldError) -> Self {
        Error::Bitfield(e)
    }
}

impl From<ssz_types::Error> for Error {
    fn from(e: ssz_types::Error) -> Self {
        Error::SszTypes(e)
    }
}

impl From<ArithError> for Error {
    fn from(e: ArithError) -> Self {
        Error::Arith(e)
    }
}
