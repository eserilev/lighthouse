//! Ethereum Consensus types
// Clippy lint set up
#![cfg_attr(
    not(test),
    deny(
        clippy::arithmetic_side_effects,
        clippy::disallowed_methods,
        clippy::indexing_slicing
    )
)]

#[macro_use]
pub mod test_utils;

pub mod attestation;
pub mod block;
pub mod builder;
pub mod consolidation;
pub mod core;
pub mod data;
pub mod deposit;
pub mod execution;
pub mod exit;
pub mod fork;
<<<<<<< HEAD
pub mod fork_data;
pub mod fork_name;
pub mod graffiti;
pub mod historical_batch;
pub mod historical_summary;
pub mod inclusion_list;
pub mod inclusion_list_committee;
pub mod inclusion_list_duty;
pub mod indexed_attestation;
pub mod light_client_bootstrap;
pub mod light_client_finality_update;
pub mod light_client_optimistic_update;
pub mod light_client_update;
pub mod pending_attestation;
pub mod pending_consolidation;
pub mod pending_deposit;
pub mod pending_partial_withdrawal;
pub mod proposer_preparation_data;
pub mod proposer_slashing;
pub mod relative_epoch;
pub mod selection_proof;
pub mod shuffling_id;
pub mod signed_aggregate_and_proof;
pub mod signed_beacon_block;
pub mod signed_beacon_block_header;
pub mod signed_bls_to_execution_change;
pub mod signed_contribution_and_proof;
pub mod signed_voluntary_exit;
pub mod signing_data;
pub mod sync_committee_subscription;
pub mod sync_duty;
pub mod validator;
pub mod validator_subscription;
pub mod voluntary_exit;
pub mod withdrawal_credentials;
pub mod withdrawal_request;
#[macro_use]
pub mod slot_epoch_macros;
pub mod activation_queue;
pub mod config_and_preset;
pub mod execution_block_header;
pub mod execution_requests;
pub mod fork_context;
pub mod participation_flags;
pub mod payload;
pub mod preset;
pub mod slot_epoch;
pub mod subnet_id;
pub mod sync_aggregate;
pub mod sync_aggregator_selection_data;
=======
pub mod kzg_ext;
pub mod light_client;
pub mod slashing;
pub mod state;
>>>>>>> 2ce6b51269708a1c28c69a3241028522ebc153df
pub mod sync_committee;
pub mod validator;
pub mod withdrawal;

// Temporary root level exports to maintain backwards compatibility for Lighthouse.
pub use attestation::*;
pub use block::*;
pub use builder::*;
pub use consolidation::*;
pub use core::{consts, *};
pub use data::*;
pub use deposit::*;
pub use execution::*;
pub use exit::*;
pub use fork::*;
pub use kzg_ext::*;
pub use light_client::*;
pub use slashing::*;
pub use state::*;
pub use sync_committee::*;
pub use validator::*;
pub use withdrawal::*;

// Temporary facade modules to maintain backwards compatibility for Lighthouse.
pub mod eth_spec {
    pub use crate::core::EthSpec;
}

<<<<<<< HEAD
pub use crate::activation_queue::ActivationQueue;
pub use crate::aggregate_and_proof::{
    AggregateAndProof, AggregateAndProofBase, AggregateAndProofElectra, AggregateAndProofRef,
};
pub use crate::attestation::{
    Attestation, AttestationBase, AttestationElectra, AttestationRef, AttestationRefMut,
    Error as AttestationError, SingleAttestation,
};
pub use crate::attestation_data::AttestationData;
pub use crate::attestation_duty::AttestationDuty;
pub use crate::attester_slashing::{
    AttesterSlashing, AttesterSlashingBase, AttesterSlashingElectra, AttesterSlashingOnDisk,
    AttesterSlashingRef, AttesterSlashingRefOnDisk,
};
pub use crate::beacon_block::{
    BeaconBlock, BeaconBlockAltair, BeaconBlockBase, BeaconBlockBellatrix, BeaconBlockCapella,
    BeaconBlockDeneb, BeaconBlockEip7805, BeaconBlockElectra, BeaconBlockFulu, BeaconBlockRef,
    BeaconBlockRefMut, BlindedBeaconBlock, BlockImportSource, EmptyBlock,
};
pub use crate::beacon_block_body::{
    BeaconBlockBody, BeaconBlockBodyAltair, BeaconBlockBodyBase, BeaconBlockBodyBellatrix,
    BeaconBlockBodyCapella, BeaconBlockBodyDeneb, BeaconBlockBodyEip7805, BeaconBlockBodyElectra,
    BeaconBlockBodyFulu, BeaconBlockBodyRef, BeaconBlockBodyRefMut,
};
pub use crate::beacon_block_header::BeaconBlockHeader;
pub use crate::beacon_committee::{BeaconCommittee, OwnedBeaconCommittee};
pub use crate::beacon_response::{
    BeaconResponse, ForkVersionDecode, ForkVersionedResponse, UnversionedResponse,
};
pub use crate::beacon_state::{Error as BeaconStateError, *};
pub use crate::blob_sidecar::{BlobIdentifier, BlobSidecar, BlobSidecarList, BlobsList};
pub use crate::bls_to_execution_change::BlsToExecutionChange;
pub use crate::chain_spec::{ChainSpec, Config, Domain};
pub use crate::checkpoint::Checkpoint;
pub use crate::config_and_preset::{
    ConfigAndPreset, ConfigAndPresetDeneb, ConfigAndPresetElectra, ConfigAndPresetFulu,
};
pub use crate::consolidation_request::ConsolidationRequest;
pub use crate::contribution_and_proof::ContributionAndProof;
pub use crate::data_column_sidecar::{
    ColumnIndex, DataColumnSidecar, DataColumnSidecarList, DataColumnsByRootIdentifier,
};
pub use crate::data_column_subnet_id::DataColumnSubnetId;
pub use crate::deposit::{Deposit, DEPOSIT_TREE_DEPTH};
pub use crate::deposit_data::DepositData;
pub use crate::deposit_message::DepositMessage;
pub use crate::deposit_request::DepositRequest;
pub use crate::deposit_tree_snapshot::{DepositTreeSnapshot, FinalizedExecutionBlock};
pub use crate::enr_fork_id::EnrForkId;
pub use crate::epoch_cache::{EpochCache, EpochCacheError, EpochCacheKey};
pub use crate::eth1_data::Eth1Data;
pub use crate::eth_spec::EthSpecId;
pub use crate::execution_block_hash::ExecutionBlockHash;
pub use crate::execution_block_header::{EncodableExecutionBlockHeader, ExecutionBlockHeader};
pub use crate::execution_payload::{
    ExecutionPayload, ExecutionPayloadBellatrix, ExecutionPayloadCapella, ExecutionPayloadDeneb,
    ExecutionPayloadEip7805, ExecutionPayloadElectra, ExecutionPayloadFulu, ExecutionPayloadRef,
    Transaction, Transactions, Withdrawals,
};
pub use crate::execution_payload_header::{
    ExecutionPayloadHeader, ExecutionPayloadHeaderBellatrix, ExecutionPayloadHeaderCapella,
    ExecutionPayloadHeaderDeneb, ExecutionPayloadHeaderEip7805, ExecutionPayloadHeaderElectra,
    ExecutionPayloadHeaderFulu, ExecutionPayloadHeaderRef, ExecutionPayloadHeaderRefMut,
};
pub use crate::execution_requests::{ExecutionRequests, RequestType};
pub use crate::fork::Fork;
pub use crate::fork_context::ForkContext;
pub use crate::fork_data::ForkData;
pub use crate::fork_name::{ForkName, InconsistentFork};
pub use crate::graffiti::{Graffiti, GRAFFITI_BYTES_LEN};
pub use crate::historical_batch::HistoricalBatch;
pub use crate::inclusion_list::{InclusionList, SignedInclusionList};
pub use crate::inclusion_list_committee::InclusionListCommittee;
pub use crate::inclusion_list_duty::InclusionListDuty;
pub use crate::indexed_attestation::{
    IndexedAttestation, IndexedAttestationBase, IndexedAttestationElectra, IndexedAttestationRef,
};
pub use crate::light_client_bootstrap::{
    LightClientBootstrap, LightClientBootstrapAltair, LightClientBootstrapCapella,
    LightClientBootstrapDeneb, LightClientBootstrapElectra, LightClientBootstrapFulu,
};
pub use crate::light_client_finality_update::{
    LightClientFinalityUpdate, LightClientFinalityUpdateAltair, LightClientFinalityUpdateCapella,
    LightClientFinalityUpdateDeneb, LightClientFinalityUpdateElectra,
    LightClientFinalityUpdateFulu,
};
pub use crate::light_client_header::{
    LightClientHeader, LightClientHeaderAltair, LightClientHeaderCapella, LightClientHeaderDeneb,
    LightClientHeaderElectra, LightClientHeaderFulu,
};
pub use crate::light_client_optimistic_update::{
    LightClientOptimisticUpdate, LightClientOptimisticUpdateAltair,
    LightClientOptimisticUpdateCapella, LightClientOptimisticUpdateDeneb,
    LightClientOptimisticUpdateElectra, LightClientOptimisticUpdateFulu,
};
pub use crate::light_client_update::{
    Error as LightClientUpdateError, LightClientUpdate, LightClientUpdateAltair,
    LightClientUpdateCapella, LightClientUpdateDeneb, LightClientUpdateElectra,
    LightClientUpdateFulu, MerkleProof,
};
pub use crate::participation_flags::ParticipationFlags;
pub use crate::payload::{
    AbstractExecPayload, BlindedPayload, BlindedPayloadBellatrix, BlindedPayloadCapella,
    BlindedPayloadDeneb, BlindedPayloadEip7805, BlindedPayloadElectra, BlindedPayloadFulu,
    BlindedPayloadRef, BlockType, ExecPayload, FullPayload, FullPayloadBellatrix,
    FullPayloadCapella, FullPayloadDeneb, FullPayloadEip7805, FullPayloadElectra, FullPayloadFulu,
    FullPayloadRef, OwnedExecPayload,
};
pub use crate::pending_attestation::PendingAttestation;
pub use crate::pending_consolidation::PendingConsolidation;
pub use crate::pending_deposit::PendingDeposit;
pub use crate::pending_partial_withdrawal::PendingPartialWithdrawal;
pub use crate::preset::{
    AltairPreset, BasePreset, BellatrixPreset, CapellaPreset, DenebPreset, Eip7805Preset,
    ElectraPreset, FuluPreset,
};
pub use crate::proposer_preparation_data::ProposerPreparationData;
pub use crate::proposer_slashing::ProposerSlashing;
pub use crate::relative_epoch::{Error as RelativeEpochError, RelativeEpoch};
pub use crate::runtime_fixed_vector::RuntimeFixedVector;
pub use crate::runtime_var_list::RuntimeVariableList;
pub use crate::selection_proof::SelectionProof;
pub use crate::shuffling_id::AttestationShufflingId;
pub use crate::signed_aggregate_and_proof::{
    SignedAggregateAndProof, SignedAggregateAndProofBase, SignedAggregateAndProofElectra,
};
pub use crate::signed_beacon_block::{
    ssz_tagged_signed_beacon_block, ssz_tagged_signed_beacon_block_arc, SignedBeaconBlock,
    SignedBeaconBlockAltair, SignedBeaconBlockBase, SignedBeaconBlockBellatrix,
    SignedBeaconBlockCapella, SignedBeaconBlockDeneb, SignedBeaconBlockEip7805,
    SignedBeaconBlockElectra, SignedBeaconBlockFulu, SignedBeaconBlockHash,
    SignedBlindedBeaconBlock,
};
pub use crate::signed_beacon_block_header::SignedBeaconBlockHeader;
pub use crate::signed_bls_to_execution_change::SignedBlsToExecutionChange;
pub use crate::signed_contribution_and_proof::SignedContributionAndProof;
pub use crate::signed_voluntary_exit::SignedVoluntaryExit;
pub use crate::signing_data::{SignedRoot, SigningData};
pub use crate::slot_epoch::{Epoch, Slot};
pub use crate::subnet_id::SubnetId;
pub use crate::sync_aggregate::SyncAggregate;
pub use crate::sync_aggregator_selection_data::SyncAggregatorSelectionData;
pub use crate::sync_committee::SyncCommittee;
pub use crate::sync_committee_contribution::{SyncCommitteeContribution, SyncContributionData};
pub use crate::sync_committee_message::SyncCommitteeMessage;
pub use crate::sync_committee_subscription::SyncCommitteeSubscription;
pub use crate::sync_duty::SyncDuty;
pub use crate::sync_selection_proof::SyncSelectionProof;
pub use crate::sync_subnet_id::SyncSubnetId;
pub use crate::validator::Validator;
pub use crate::validator_registration_data::*;
pub use crate::validator_subscription::ValidatorSubscription;
pub use crate::voluntary_exit::VoluntaryExit;
pub use crate::withdrawal::Withdrawal;
pub use crate::withdrawal_credentials::WithdrawalCredentials;
pub use crate::withdrawal_request::WithdrawalRequest;
pub use fixed_bytes::FixedBytesExtended;
=======
pub mod chain_spec {
    pub use crate::core::ChainSpec;
}
>>>>>>> 2ce6b51269708a1c28c69a3241028522ebc153df

pub mod beacon_block {
    pub use crate::block::{BlindedBeaconBlock, BlockImportSource};
}

pub mod beacon_block_body {
    pub use crate::kzg_ext::{KzgCommitments, format_kzg_commitments};
}

pub mod beacon_state {
    pub use crate::state::{
        BeaconState, BeaconStateBase, CommitteeCache, compute_committee_index_in_epoch,
        compute_committee_range_in_epoch, epoch_committee_count,
    };
}

pub mod graffiti {
    pub use crate::core::GraffitiString;
}

pub mod indexed_attestation {
    pub use crate::attestation::{IndexedAttestationBase, IndexedAttestationElectra};
}

pub mod historical_summary {
    pub use crate::state::HistoricalSummary;
}

pub mod participation_flags {
    pub use crate::attestation::ParticipationFlags;
}

pub mod epoch_cache {
    pub use crate::state::{EpochCache, EpochCacheError, EpochCacheKey};
}

pub mod non_zero_usize {
    pub use crate::core::new_non_zero_usize;
}

pub mod data_column_sidecar {
    pub use crate::data::{
        Cell, ColumnIndex, DataColumn, DataColumnSidecar, DataColumnSidecarError,
        DataColumnSidecarList,
    };
}

pub mod builder_bid {
    pub use crate::builder::*;
}

pub mod blob_sidecar {
    pub use crate::data::{
        BlobIdentifier, BlobSidecar, BlobSidecarError, BlobsList, FixedBlobSidecarList,
    };
}

pub mod payload {
    pub use crate::execution::BlockProductionVersion;
}

pub mod execution_requests {
    pub use crate::execution::{
        ConsolidationRequests, DepositRequests, ExecutionRequests, RequestType, WithdrawalRequests,
    };
}

pub mod execution_payload_envelope {
    pub use crate::execution::{ExecutionPayloadEnvelope, SignedExecutionPayloadEnvelope};
}

pub mod data_column_custody_group {
    pub use crate::data::{
        CustodyIndex, compute_columns_for_custody_group, compute_ordered_custody_column_indices,
        compute_subnets_for_node, compute_subnets_from_custody_group, get_custody_groups,
    };
}

pub mod sync_aggregate {
    pub use crate::sync_committee::SyncAggregateError as Error;
}

pub mod light_client_update {
    pub use crate::light_client::consts::{
        CURRENT_SYNC_COMMITTEE_INDEX, CURRENT_SYNC_COMMITTEE_INDEX_ELECTRA, FINALIZED_ROOT_INDEX,
        FINALIZED_ROOT_INDEX_ELECTRA, MAX_REQUEST_LIGHT_CLIENT_UPDATES, NEXT_SYNC_COMMITTEE_INDEX,
        NEXT_SYNC_COMMITTEE_INDEX_ELECTRA,
    };
}

pub mod sync_committee_contribution {
    pub use crate::sync_committee::{
        SyncCommitteeContributionError as Error, SyncContributionData,
    };
}

pub mod slot_data {
    pub use crate::core::SlotData;
}

pub mod signed_aggregate_and_proof {
    pub use crate::attestation::SignedAggregateAndProofRefMut;
}

pub mod payload_attestation {
    pub use crate::attestation::{
        PayloadAttestation, PayloadAttestationData, PayloadAttestationMessage,
    };
}

pub mod application_domain {
    pub use crate::core::ApplicationDomain;
}

// Temporary re-exports to maintain backwards compatibility for Lighthouse.
pub use crate::kzg_ext::consts::VERSIONED_HASH_VERSION_KZG;
pub use crate::light_client::LightClientError as LightClientUpdateError;
pub use crate::state::BeaconStateError as Error;
