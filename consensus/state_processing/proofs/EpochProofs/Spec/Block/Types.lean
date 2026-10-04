import EpochProofs.Spec.Types

/-!
# Gloas block, envelope and signing containers

Transcribed from the consensus specs (v1.7.0-beta.2). Names and field order match the spec.
Each field has the SSZ default as its default, except that a vector field defaults to the
empty list. Python `BitList` and `BitVector` are `List Bool`. Byte lists are `List UInt8`.
-/

namespace EpochProofs.Spec

abbrev CommitteeIndex := Nat

structure SigningData where
  object_root : Root := Bytes32.zero
  domain : Domain := Bytes32.zero
  deriving DecidableEq, Repr

structure ForkData where
  current_version : Version := Version.zero
  genesis_validators_root : Root := Bytes32.zero
  deriving DecidableEq, Repr

structure SignedBeaconBlockHeader where
  message : BeaconBlockHeader := {}
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

structure ProposerSlashing where
  signed_header_1 : SignedBeaconBlockHeader := {}
  signed_header_2 : SignedBeaconBlockHeader := {}
  deriving DecidableEq, Repr

structure AttestationData where
  slot : Slot := 0
  index : CommitteeIndex := 0
  beacon_block_root : Root := Bytes32.zero
  source : Checkpoint := {}
  target : Checkpoint := {}
  deriving DecidableEq, Repr

structure IndexedAttestation where
  attesting_indices : List ValidatorIndex := []
  data : AttestationData := {}
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

structure AttesterSlashing where
  attestation_1 : IndexedAttestation := {}
  attestation_2 : IndexedAttestation := {}
  deriving DecidableEq, Repr

/-- `committee_bits` is a `BitVector[MAX_COMMITTEES_PER_SLOT]`. -/
structure Attestation where
  aggregation_bits : List Bool := []
  data : AttestationData := {}
  signature : BLSSignature := BLSSignature.zero
  committee_bits : List Bool := []
  deriving DecidableEq, Repr

structure DepositData where
  pubkey : BLSPubkey := BLSPubkey.zero
  withdrawal_credentials : Bytes32 := Bytes32.zero
  amount : Gwei := 0
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

structure DepositMessage where
  pubkey : BLSPubkey := BLSPubkey.zero
  withdrawal_credentials : Bytes32 := Bytes32.zero
  amount : Gwei := 0
  deriving DecidableEq, Repr

/-- `proof` is a `Vector[Bytes32, DEPOSIT_CONTRACT_TREE_DEPTH + 1]`. -/
structure Deposit where
  proof : List Bytes32 := []
  data : DepositData := {}
  deriving DecidableEq, Repr

structure VoluntaryExit where
  epoch : Epoch := 0
  validator_index : ValidatorIndex := 0
  deriving DecidableEq, Repr

structure SignedVoluntaryExit where
  message : VoluntaryExit := {}
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

/-- `sync_committee_bits` is a `BitVector[SYNC_COMMITTEE_SIZE]`. -/
structure SyncAggregate where
  sync_committee_bits : List Bool := []
  sync_committee_signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

structure BLSToExecutionChange where
  validator_index : ValidatorIndex := 0
  from_bls_pubkey : BLSPubkey := BLSPubkey.zero
  to_execution_address : ExecutionAddress := ExecutionAddress.zero
  deriving DecidableEq, Repr

structure SignedBLSToExecutionChange where
  message : BLSToExecutionChange := {}
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

structure SignedExecutionPayloadBid where
  message : ExecutionPayloadBid := {}
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

structure PayloadAttestationData where
  beacon_block_root : Root := Bytes32.zero
  slot : Slot := 0
  payload_present : Bool := false
  blob_data_available : Bool := false
  deriving DecidableEq, Repr

/-- `aggregation_bits` is a `BitVector[PTC_SIZE]`. -/
structure PayloadAttestation where
  aggregation_bits : List Bool := []
  data : PayloadAttestationData := {}
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

structure IndexedPayloadAttestation where
  attesting_indices : List ValidatorIndex := []
  data : PayloadAttestationData := {}
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

structure DepositRequest where
  pubkey : BLSPubkey := BLSPubkey.zero
  withdrawal_credentials : Bytes32 := Bytes32.zero
  amount : Gwei := 0
  signature : BLSSignature := BLSSignature.zero
  index : Uint64 := 0
  deriving DecidableEq, Repr

structure WithdrawalRequest where
  source_address : ExecutionAddress := ExecutionAddress.zero
  validator_pubkey : BLSPubkey := BLSPubkey.zero
  amount : Gwei := 0
  deriving DecidableEq, Repr

structure ConsolidationRequest where
  source_address : ExecutionAddress := ExecutionAddress.zero
  source_pubkey : BLSPubkey := BLSPubkey.zero
  target_pubkey : BLSPubkey := BLSPubkey.zero
  deriving DecidableEq, Repr

structure BuilderDepositRequest where
  pubkey : BLSPubkey := BLSPubkey.zero
  withdrawal_credentials : Bytes32 := Bytes32.zero
  amount : Gwei := 0
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

structure BuilderExitRequest where
  source_address : ExecutionAddress := ExecutionAddress.zero
  pubkey : BLSPubkey := BLSPubkey.zero
  deriving DecidableEq, Repr

structure ExecutionRequests where
  deposits : List DepositRequest := []
  withdrawals : List WithdrawalRequest := []
  consolidations : List ConsolidationRequest := []
  builder_deposits : List BuilderDepositRequest := []
  builder_exits : List BuilderExitRequest := []
  deriving DecidableEq, Repr

structure BeaconBlockBody where
  randao_reveal : BLSSignature := BLSSignature.zero
  eth1_data : Eth1Data := {}
  graffiti : Bytes32 := Bytes32.zero
  proposer_slashings : List ProposerSlashing := []
  attester_slashings : List AttesterSlashing := []
  attestations : List Attestation := []
  deposits : List Deposit := []
  voluntary_exits : List SignedVoluntaryExit := []
  sync_aggregate : SyncAggregate := {}
  bls_to_execution_changes : List SignedBLSToExecutionChange := []
  signed_execution_payload_bid : SignedExecutionPayloadBid := {}
  payload_attestations : List PayloadAttestation := []
  parent_execution_requests : ExecutionRequests := {}
  deriving DecidableEq, Repr

structure BeaconBlock where
  slot : Slot := 0
  proposer_index : ValidatorIndex := 0
  parent_root : Root := Bytes32.zero
  state_root : Root := Bytes32.zero
  body : BeaconBlockBody := {}
  deriving DecidableEq, Repr

structure SignedBeaconBlock where
  message : BeaconBlock := {}
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

/-- `logs_bloom` is a `ByteVector[BYTES_PER_LOGS_BLOOM]`. `base_fee_per_gas` is a `Uint256`. -/
structure ExecutionPayload where
  parent_hash : Hash32 := Bytes32.zero
  fee_recipient : ExecutionAddress := ExecutionAddress.zero
  state_root : Bytes32 := Bytes32.zero
  receipts_root : Bytes32 := Bytes32.zero
  logs_bloom : List UInt8 := []
  prev_randao : Bytes32 := Bytes32.zero
  block_number : Uint64 := 0
  gas_limit : Uint64 := 0
  gas_used : Uint64 := 0
  timestamp : Uint64 := 0
  extra_data : List UInt8 := []
  base_fee_per_gas : Nat := 0
  block_hash : Hash32 := Bytes32.zero
  transactions : List (List UInt8) := []
  withdrawals : List Withdrawal := []
  blob_gas_used : Uint64 := 0
  excess_blob_gas : Uint64 := 0
  block_access_list : List UInt8 := []
  slot_number : Uint64 := 0
  deriving DecidableEq, Repr

structure ExecutionPayloadEnvelope where
  payload : ExecutionPayload := {}
  execution_requests : ExecutionRequests := {}
  builder_index : BuilderIndex := 0
  beacon_block_root : Root := Bytes32.zero
  parent_beacon_block_root : Root := Bytes32.zero
  deriving DecidableEq, Repr

structure SignedExecutionPayloadEnvelope where
  message : ExecutionPayloadEnvelope := {}
  signature : BLSSignature := BLSSignature.zero
  deriving DecidableEq, Repr

end EpochProofs.Spec
