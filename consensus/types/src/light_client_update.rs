use super::{BeaconBlockHeader, EthSpec, FixedVector, Hash256, Slot, SyncAggregate, SyncCommittee};
use crate::{
    beacon_state,
    light_client_header::{
        LightClientHeaderAltair, LightClientHeaderCapella, LightClientHeaderDeneb,
    },
    BeaconBlock, BeaconState, ChainSpec, ForkName, ForkVersionDeserialize, LightClientHeader,
    SignedBeaconBlock,
};
use safe_arith::ArithError;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use ssz_derive::{Decode, Encode};
use ssz_types::typenum::{U4, U5, U6};
use std::sync::Arc;
use tree_hash::TreeHash;

pub const FINALIZED_ROOT_INDEX: usize = 105;
pub const CURRENT_SYNC_COMMITTEE_INDEX: usize = 54;
pub const NEXT_SYNC_COMMITTEE_INDEX: usize = 55;
pub const EXECUTION_PAYLOAD_INDEX: usize = 25;

pub type FinalizedRootProofLen = U6;
pub type CurrentSyncCommitteeProofLen = U5;
pub type ExecutionPayloadProofLen = U4;

pub type NextSyncCommitteeProofLen = U5;

pub const FINALIZED_ROOT_PROOF_LEN: usize = 6;
pub const CURRENT_SYNC_COMMITTEE_PROOF_LEN: usize = 5;
pub const NEXT_SYNC_COMMITTEE_PROOF_LEN: usize = 5;
pub const EXECUTION_PAYLOAD_PROOF_LEN: usize = 4;

#[derive(Debug, PartialEq, Clone)]
pub enum Error {
    SszTypesError(ssz_types::Error),
    BeaconStateError(beacon_state::Error),
    ArithError(ArithError),
    AltairForkNotActive,
    NotEnoughSyncCommitteeParticipants,
    MismatchingPeriods,
    InvalidFinalizedBlock,
    BeaconBlockBodyError,
}

impl From<ssz_types::Error> for Error {
    fn from(e: ssz_types::Error) -> Error {
        Error::SszTypesError(e)
    }
}

impl From<beacon_state::Error> for Error {
    fn from(e: beacon_state::Error) -> Error {
        Error::BeaconStateError(e)
    }
}

impl From<ArithError> for Error {
    fn from(e: ArithError) -> Error {
        Error::ArithError(e)
    }
}

/// A LightClientUpdate is the update we request solely to either complete the bootstraping process,
/// or to sync up to the last committee period, we need to have one ready for each ALTAIR period
/// we go over, note: there is no need to keep all of the updates from [ALTAIR_PERIOD, CURRENT_PERIOD].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode, arbitrary::Arbitrary)]
#[serde(bound = "T: EthSpec")]
#[arbitrary(bound = "T: EthSpec")]
pub struct LightClientUpdate<T: EthSpec> {
    /// The last `BeaconBlockHeader` from the last attested block by the sync committee.
    pub attested_header: LightClientHeader<T>,
    /// The `SyncCommittee` used in the next period.
    pub next_sync_committee: Arc<SyncCommittee<T>>,
    /// Merkle proof for next sync committee
    pub next_sync_committee_branch: FixedVector<Hash256, NextSyncCommitteeProofLen>,
    /// The last `BeaconBlockHeader` from the last attested finalized block (end of epoch).
    pub finalized_header: LightClientHeader<T>,
    /// Merkle proof attesting finalized header.
    pub finality_branch: FixedVector<Hash256, FinalizedRootProofLen>,
    /// current sync aggreggate
    pub sync_aggregate: SyncAggregate<T>,
    /// Slot of the sync aggregated singature
    pub signature_slot: Slot,
}

impl<T: EthSpec> LightClientUpdate<T> {
    pub fn new(
        chain_spec: ChainSpec,
        beacon_state: BeaconState<T>,
        block: BeaconBlock<T>,
        attested_state: &mut BeaconState<T>,
        attested_block: SignedBeaconBlock<T>,
        finalized_block: SignedBeaconBlock<T>,
    ) -> Result<Self, Error> {
        let sync_aggregate = block.body().sync_aggregate()?;
        if sync_aggregate.num_set_bits() < chain_spec.min_sync_committee_participants as usize {
            return Err(Error::NotEnoughSyncCommitteeParticipants);
        }

        let signature_period = block.epoch().sync_committee_period(&chain_spec)?;
        // Compute and validate attested header.
        let mut attested_header = attested_state.latest_block_header().clone();
        attested_header.state_root = attested_state.tree_hash_root();
        let attested_period = attested_header
            .slot
            .epoch(T::slots_per_epoch())
            .sync_committee_period(&chain_spec)?;
        if attested_period != signature_period {
            return Err(Error::MismatchingPeriods);
        }
        // Build finalized header from finalized block
        let finalized_header = BeaconBlockHeader {
            slot: finalized_block.slot(),
            proposer_index: finalized_block.message().proposer_index(),
            parent_root: finalized_block.parent_root(),
            state_root: finalized_block.state_root(),
            body_root: finalized_block.message().body_root(),
        };
        if finalized_header.tree_hash_root() != beacon_state.finalized_checkpoint().root {
            return Err(Error::InvalidFinalizedBlock);
        }
        let next_sync_committee_branch =
            attested_state.compute_merkle_proof(NEXT_SYNC_COMMITTEE_INDEX)?;
        let finality_branch = attested_state.compute_merkle_proof(FINALIZED_ROOT_INDEX)?;

        let (attested_header, finalized_header) =
            match chain_spec.fork_name_at_epoch(beacon_state.slot().epoch(T::slots_per_epoch())) {
                ForkName::Base => return Err(Error::AltairForkNotActive),
                ForkName::Merge | ForkName::Altair => (
                    LightClientHeaderAltair::block_to_light_client_header(attested_block)?.into(),
                    LightClientHeaderAltair::block_to_light_client_header(finalized_block)?.into(),
                ),
                ForkName::Capella => (
                    LightClientHeaderCapella::block_to_light_client_header(attested_block)?.into(),
                    LightClientHeaderCapella::block_to_light_client_header(finalized_block)?.into(),
                ),
                ForkName::Deneb => (
                    LightClientHeaderDeneb::block_to_light_client_header(attested_block)?.into(),
                    LightClientHeaderDeneb::block_to_light_client_header(finalized_block)?.into(),
                ),
            };

        Ok(Self {
            attested_header,
            next_sync_committee: attested_state.next_sync_committee()?.clone(),
            next_sync_committee_branch: FixedVector::new(next_sync_committee_branch)?,
            finalized_header,
            finality_branch: FixedVector::new(finality_branch)?,
            sync_aggregate: sync_aggregate.clone(),
            signature_slot: block.slot(),
        })
    }
}

impl<T: EthSpec> ForkVersionDeserialize for LightClientUpdate<T> {
    fn deserialize_by_fork<'de, D: Deserializer<'de>>(
        value: Value,
        fork_name: ForkName,
    ) -> Result<Self, D::Error> {
        match fork_name {
            ForkName::Altair | ForkName::Merge | ForkName::Capella | ForkName::Deneb => {
                Ok(serde_json::from_value::<LightClientUpdate<T>>(value)
                    .map_err(serde::de::Error::custom))?
            }
            ForkName::Base => Err(serde::de::Error::custom(format!(
                "LightClientUpdate failed to deserialize: unsupported fork '{}'",
                fork_name
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::MainnetEthSpec;
    use ssz_types::typenum::Unsigned;

    // ssz_tests!(LightClientUpdate<MainnetEthSpec>);

    #[test]
    fn finalized_root_params() {
        assert!(2usize.pow(FINALIZED_ROOT_PROOF_LEN as u32) <= FINALIZED_ROOT_INDEX);
        assert!(2usize.pow(FINALIZED_ROOT_PROOF_LEN as u32 + 1) > FINALIZED_ROOT_INDEX);
        assert_eq!(FinalizedRootProofLen::to_usize(), FINALIZED_ROOT_PROOF_LEN);
    }

    #[test]
    fn current_sync_committee_params() {
        assert!(
            2usize.pow(CURRENT_SYNC_COMMITTEE_PROOF_LEN as u32) <= CURRENT_SYNC_COMMITTEE_INDEX
        );
        assert!(
            2usize.pow(CURRENT_SYNC_COMMITTEE_PROOF_LEN as u32 + 1) > CURRENT_SYNC_COMMITTEE_INDEX
        );
        assert_eq!(
            CurrentSyncCommitteeProofLen::to_usize(),
            CURRENT_SYNC_COMMITTEE_PROOF_LEN
        );
    }

    #[test]
    fn next_sync_committee_params() {
        assert!(2usize.pow(NEXT_SYNC_COMMITTEE_PROOF_LEN as u32) <= NEXT_SYNC_COMMITTEE_INDEX);
        assert!(2usize.pow(NEXT_SYNC_COMMITTEE_PROOF_LEN as u32 + 1) > NEXT_SYNC_COMMITTEE_INDEX);
        assert_eq!(
            NextSyncCommitteeProofLen::to_usize(),
            NEXT_SYNC_COMMITTEE_PROOF_LEN
        );
    }
}
