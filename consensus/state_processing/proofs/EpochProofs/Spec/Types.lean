/-!
# Gloas types and `Uint64` arithmetic

Transcribed from the consensus specs (v1.7.0-beta.2). Names match the spec.

pyspec raises on `Uint64` overflow and on division by zero. Here a raise is `Except.error`.

Write this file from the spec only. Do not read Lighthouse.
-/

namespace EpochProofs.Spec

abbrev Gwei := Nat
abbrev Uint64 := Nat
abbrev ValidatorIndex := Nat
abbrev BuilderIndex := Nat
abbrev Epoch := Nat
abbrev Slot := Nat
abbrev ParticipationFlags := UInt8
abbrev ExecutionAddress := Vector UInt8 20

inductive SpecError where
  | overflow
  | divisionByZero
  | indexOutOfRange
  | assertionFailed
  deriving DecidableEq, Repr

abbrev SpecM := Except SpecError

def UINT64_SIZE : Nat := 2 ^ 64

def uint64Add (a b : Uint64) : SpecM Uint64 :=
  if a + b < UINT64_SIZE then pure (a + b) else throw .overflow

def uint64Sub (a b : Uint64) : SpecM Uint64 :=
  if b ≤ a then pure (a - b) else throw .overflow

def uint64Mod (a b : Uint64) : SpecM Uint64 :=
  if b = 0 then throw .divisionByZero else pure (a % b)

def uint64Mul (a b : Uint64) : SpecM Uint64 :=
  if a * b < UINT64_SIZE then pure (a * b) else throw .overflow

def uint64Div (a b : Uint64) : SpecM Uint64 :=
  if b = 0 then throw .divisionByZero else pure (a / b)

/-- `l[i]` in Python. -/
def listGet {α : Type} (l : List α) (i : Nat) : SpecM α :=
  match l[i]? with
  | some a => pure a
  | none => throw .indexOutOfRange

/-- `l[i] = a` in Python. -/
def listSet {α : Type} (l : List α) (i : Nat) (a : α) : SpecM (List α) :=
  if i < l.length then pure (l.set i a) else throw .indexOutOfRange

/-- Preset and config values that the reference reads. The defaults are the mainnet values. -/
structure Preset where
  SLOTS_PER_EPOCH : Nat
  EFFECTIVE_BALANCE_INCREMENT : Gwei := 1000000000
  HYSTERESIS_QUOTIENT : Uint64 := 4
  HYSTERESIS_DOWNWARD_MULTIPLIER : Uint64 := 1
  HYSTERESIS_UPWARD_MULTIPLIER : Uint64 := 5
  MIN_ACTIVATION_BALANCE : Gwei := 32000000000
  MAX_EFFECTIVE_BALANCE_ELECTRA : Gwei := 2048000000000
  MIN_EPOCHS_TO_INACTIVITY_PENALTY : Uint64 := 4
  INACTIVITY_SCORE_BIAS : Uint64 := 4
  INACTIVITY_SCORE_RECOVERY_RATE : Uint64 := 16
  EPOCHS_PER_SLASHINGS_VECTOR : Uint64 := 8192
  PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX : Uint64 := 3
  BASE_REWARD_FACTOR : Uint64 := 64
  INACTIVITY_PENALTY_QUOTIENT_BELLATRIX : Uint64 := 16777216
  MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA : Gwei := 128000000000
  CHURN_LIMIT_QUOTIENT_GLOAS : Uint64 := 32768
  EJECTION_BALANCE : Gwei := 16000000000
  MAX_SEED_LOOKAHEAD : Uint64 := 4
  MIN_VALIDATOR_WITHDRAWABILITY_DELAY : Uint64 := 256
  MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT_GLOAS : Gwei := 256000000000
  MAX_PENDING_DEPOSITS_PER_EPOCH : Uint64 := 16
  SLOTS_PER_HISTORICAL_ROOT : Uint64 := 8192
  EPOCHS_PER_HISTORICAL_VECTOR : Uint64 := 65536
  EPOCHS_PER_ETH1_VOTING_PERIOD : Uint64 := 64
  MIN_SEED_LOOKAHEAD : Uint64 := 1
  SYNC_COMMITTEE_SIZE : Uint64 := 512
  EPOCHS_PER_SYNC_COMMITTEE_PERIOD : Uint64 := 256
  PTC_SIZE : Uint64 := 512
  GENESIS_FORK_VERSION : Vector UInt8 4 := #v[0, 0, 0, 0]
  MAX_WITHDRAWALS_PER_PAYLOAD : Uint64 := 16
  MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP : Uint64 := 16384
  MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP : Uint64 := 8
  MAX_BUILDERS_PER_WITHDRAWALS_SWEEP : Uint64 := 16384
  CAPELLA_FORK_VERSION : Vector UInt8 4 := #v[3, 0, 0, 0]
  SHUFFLE_ROUND_COUNT : Uint64 := 90
  MAX_COMMITTEES_PER_SLOT : Uint64 := 64
  TARGET_COMMITTEE_SIZE : Uint64 := 128
  MAX_VALIDATORS_PER_COMMITTEE : Uint64 := 2048
  MIN_ATTESTATION_INCLUSION_DELAY : Uint64 := 1
  SHARD_COMMITTEE_PERIOD : Uint64 := 256
  MIN_SLASHING_PENALTY_QUOTIENT_ELECTRA : Uint64 := 4096
  WHISTLEBLOWER_REWARD_QUOTIENT_ELECTRA : Uint64 := 4096

def Preset.mainnet : Preset := { SLOTS_PER_EPOCH := 32 }
def Preset.minimal : Preset :=
  { SLOTS_PER_EPOCH := 8, EPOCHS_PER_SLASHINGS_VECTOR := 64,
    MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA := 64000000000, CHURN_LIMIT_QUOTIENT_GLOAS := 16,
    MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT_GLOAS := 128000000000,
    SLOTS_PER_HISTORICAL_ROOT := 64, EPOCHS_PER_HISTORICAL_VECTOR := 64,
    EPOCHS_PER_ETH1_VOTING_PERIOD := 4, SYNC_COMMITTEE_SIZE := 32, PTC_SIZE := 16,
    EPOCHS_PER_SYNC_COMMITTEE_PERIOD := 8,
    GENESIS_FORK_VERSION := #v[0, 0, 0, 1], CAPELLA_FORK_VERSION := #v[3, 0, 0, 1],
    SHUFFLE_ROUND_COUNT := 10, MAX_COMMITTEES_PER_SLOT := 4, TARGET_COMMITTEE_SIZE := 4,
    SHARD_COMMITTEE_PERIOD := 64, MAX_WITHDRAWALS_PER_PAYLOAD := 4,
    MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP := 16, MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP := 2,
    MAX_BUILDERS_PER_WITHDRAWALS_SWEEP := 16 }

def BUILDER_PAYMENT_THRESHOLD_NUMERATOR : Uint64 := 6
def BUILDER_PAYMENT_THRESHOLD_DENOMINATOR : Uint64 := 10

def COMPOUNDING_WITHDRAWAL_PREFIX : UInt8 := 0x02
def BLS_WITHDRAWAL_PREFIX : UInt8 := 0x00
def ETH1_ADDRESS_WITHDRAWAL_PREFIX : UInt8 := 0x01
def PROPOSER_WEIGHT : Uint64 := 8
def SYNC_REWARD_WEIGHT : Uint64 := 2

def GENESIS_EPOCH : Epoch := 0

def FAR_FUTURE_EPOCH : Epoch := 2 ^ 64 - 1

def TIMELY_SOURCE_FLAG_INDEX : Nat := 0
def TIMELY_TARGET_FLAG_INDEX : Nat := 1
def TIMELY_HEAD_FLAG_INDEX : Nat := 2

def TIMELY_SOURCE_WEIGHT : Uint64 := 14
def TIMELY_TARGET_WEIGHT : Uint64 := 26
def TIMELY_HEAD_WEIGHT : Uint64 := 14
def WEIGHT_DENOMINATOR : Uint64 := 64
def PARTICIPATION_FLAG_WEIGHTS : List Uint64 :=
  [TIMELY_SOURCE_WEIGHT, TIMELY_TARGET_WEIGHT, TIMELY_HEAD_WEIGHT]

def UINT64_MAX : Uint64 := 2 ^ 64 - 1
def UINT64_MAX_SQRT : Uint64 := 4294967295

def JUSTIFICATION_BITS_LENGTH : Uint64 := 4

def BUILDER_INDEX_FLAG : Uint64 := 2 ^ 40

abbrev Bytes32 := Vector UInt8 32
abbrev BLSPubkey := Vector UInt8 48
abbrev BLSSignature := Vector UInt8 96
abbrev Root := Bytes32
abbrev Hash32 := Bytes32
abbrev Version := Vector UInt8 4
abbrev DomainType := Vector UInt8 4
abbrev Domain := Bytes32
abbrev KZGCommitment := Vector UInt8 48
abbrev WithdrawalIndex := Nat

/-- `Bytes32()` and `Root()` in Python: all bytes zero. -/
def Bytes32.zero : Bytes32 := Vector.replicate 32 0

def ExecutionAddress.zero : ExecutionAddress := Vector.replicate 20 0
def BLSPubkey.zero : BLSPubkey := Vector.replicate 48 0
def BLSSignature.zero : BLSSignature := Vector.replicate 96 0
def Version.zero : Version := Vector.replicate 4 0

def DOMAIN_BEACON_PROPOSER : DomainType := #v[0, 0, 0, 0]
def DOMAIN_BEACON_ATTESTER : DomainType := #v[1, 0, 0, 0]
def DOMAIN_RANDAO : DomainType := #v[2, 0, 0, 0]
def DOMAIN_DEPOSIT : DomainType := #v[3, 0, 0, 0]
def DOMAIN_VOLUNTARY_EXIT : DomainType := #v[4, 0, 0, 0]
def DOMAIN_SYNC_COMMITTEE : DomainType := #v[7, 0, 0, 0]
def DOMAIN_BLS_TO_EXECUTION_CHANGE : DomainType := #v[10, 0, 0, 0]
def DOMAIN_BEACON_BUILDER : DomainType := #v[11, 0, 0, 0]
def DOMAIN_PTC_ATTESTER : DomainType := #v[12, 0, 0, 0]
def DOMAIN_BUILDER_DEPOSIT : DomainType := #v[14, 0, 0, 0]

/-- Only the fields that the reference reads or writes. -/
structure Validator where
  pubkey : BLSPubkey
  withdrawal_credentials : Bytes32
  effective_balance : Gwei
  slashed : Bool
  activation_eligibility_epoch : Epoch
  activation_epoch : Epoch
  exit_epoch : Epoch
  withdrawable_epoch : Epoch
  deriving DecidableEq, Repr

structure PendingDeposit where
  pubkey : BLSPubkey
  withdrawal_credentials : Bytes32
  amount : Gwei
  signature : BLSSignature
  slot : Slot
  deriving DecidableEq, Repr

structure PendingConsolidation where
  source_index : ValidatorIndex
  target_index : ValidatorIndex
  deriving DecidableEq, Repr

structure Checkpoint where
  epoch : Epoch := 0
  root : Root := Bytes32.zero
  deriving DecidableEq, Repr

structure Fork where
  previous_version : Version := Version.zero
  current_version : Version := Version.zero
  epoch : Epoch := 0
  deriving DecidableEq, Repr

structure BeaconBlockHeader where
  slot : Slot := 0
  proposer_index : ValidatorIndex := 0
  parent_root : Root := Bytes32.zero
  state_root : Root := Bytes32.zero
  body_root : Root := Bytes32.zero
  deriving DecidableEq, Repr

structure Eth1Data where
  deposit_root : Root := Bytes32.zero
  deposit_count : Uint64 := 0
  block_hash : Hash32 := Bytes32.zero
  deriving DecidableEq, Repr

/-- `pubkeys` is a `Vector[BLSPubkey, SYNC_COMMITTEE_SIZE]`. -/
structure SyncCommittee where
  pubkeys : List BLSPubkey := []
  aggregate_pubkey : BLSPubkey := BLSPubkey.zero
  deriving DecidableEq, Repr

structure HistoricalSummary where
  block_summary_root : Root := Bytes32.zero
  state_summary_root : Root := Bytes32.zero
  deriving DecidableEq, Repr

structure PendingPartialWithdrawal where
  validator_index : ValidatorIndex := 0
  amount : Gwei := 0
  withdrawable_epoch : Epoch := 0
  deriving DecidableEq, Repr

structure Builder where
  pubkey : BLSPubkey := BLSPubkey.zero
  version : UInt8 := 0
  execution_address : ExecutionAddress := ExecutionAddress.zero
  balance : Gwei := 0
  deposit_epoch : Epoch := 0
  withdrawable_epoch : Epoch := 0
  deriving DecidableEq, Repr

structure Withdrawal where
  index : WithdrawalIndex := 0
  validator_index : ValidatorIndex := 0
  address : ExecutionAddress := ExecutionAddress.zero
  amount : Gwei := 0
  deriving DecidableEq, Repr

structure ExecutionPayloadBid where
  parent_block_hash : Hash32 := Bytes32.zero
  parent_block_root : Root := Bytes32.zero
  block_hash : Hash32 := Bytes32.zero
  prev_randao : Bytes32 := Bytes32.zero
  fee_recipient : ExecutionAddress := ExecutionAddress.zero
  gas_limit : Uint64 := 0
  builder_index : BuilderIndex := 0
  slot : Slot := 0
  value : Gwei := 0
  execution_payment : Gwei := 0
  blob_kzg_commitments : List KZGCommitment := []
  execution_requests_root : Root := Bytes32.zero
  deriving DecidableEq, Repr

structure BuilderPendingWithdrawal where
  fee_recipient : ExecutionAddress
  amount : Gwei
  builder_index : BuilderIndex
  deriving DecidableEq, Repr

def BuilderPendingWithdrawal.empty : BuilderPendingWithdrawal :=
  { fee_recipient := Vector.replicate 20 0, amount := 0, builder_index := 0 }

structure BuilderPendingPayment where
  weight : Gwei
  withdrawal : BuilderPendingWithdrawal
  proposer_index : ValidatorIndex
  deriving DecidableEq, Repr

def BuilderPendingPayment.empty : BuilderPendingPayment :=
  { weight := 0, withdrawal := .empty, proposer_index := 0 }

/-- All Gloas `BeaconState` fields, in spec order.

Python `Vector`, `List`, `ProgressiveList` and `BitVector` fields are a `List` here. A `List`
has no fixed length, so `WellFormed` and `VectorLengths` give the vector lengths.

Each field except the builder payment queues has a default. The default of a vector field is
the empty list, not the SSZ default. -/
structure BeaconState where
  genesis_time : Uint64 := 0
  genesis_validators_root : Root := Bytes32.zero
  slot : Slot := 0
  fork : Fork := {}
  latest_block_header : BeaconBlockHeader := {}
  block_roots : List Root := []
  state_roots : List Root := []
  historical_roots : List Root := []
  eth1_data : Eth1Data := {}
  eth1_data_votes : List Eth1Data := []
  eth1_deposit_index : Uint64 := 0
  validators : List Validator := []
  balances : List Gwei := []
  randao_mixes : List Bytes32 := []
  slashings : List Gwei := []
  previous_epoch_participation : List ParticipationFlags := []
  current_epoch_participation : List ParticipationFlags := []
  justification_bits : List Bool := []
  previous_justified_checkpoint : Checkpoint := {}
  current_justified_checkpoint : Checkpoint := {}
  finalized_checkpoint : Checkpoint := {}
  inactivity_scores : List Uint64 := []
  current_sync_committee : SyncCommittee := {}
  next_sync_committee : SyncCommittee := {}
  latest_block_hash : Hash32 := Bytes32.zero
  next_withdrawal_index : WithdrawalIndex := 0
  next_withdrawal_validator_index : ValidatorIndex := 0
  historical_summaries : List HistoricalSummary := []
  deposit_requests_start_index : Uint64 := 0
  deposit_balance_to_consume : Gwei := 0
  exit_balance_to_consume : Gwei := 0
  earliest_exit_epoch : Epoch := 0
  consolidation_balance_to_consume : Gwei := 0
  earliest_consolidation_epoch : Epoch := 0
  pending_deposits : List PendingDeposit := []
  pending_partial_withdrawals : List PendingPartialWithdrawal := []
  pending_consolidations : List PendingConsolidation := []
  proposer_lookahead : List ValidatorIndex := []
  builders : List Builder := []
  next_withdrawal_builder_index : BuilderIndex := 0
  execution_payload_availability : List Bool := []
  builder_pending_payments : List BuilderPendingPayment
  builder_pending_withdrawals : List BuilderPendingWithdrawal
  latest_execution_payload_bid : ExecutionPayloadBid := {}
  payload_expected_withdrawals : List Withdrawal := []
  ptc_window : List (List ValidatorIndex) := []
  deriving DecidableEq, Repr

def BeaconState.WellFormed (p : Preset) (state : BeaconState) : Prop :=
  state.builder_pending_payments.length = 2 * p.SLOTS_PER_EPOCH

/-- The lengths of the Python `Vector` and `BitVector` fields. -/
def BeaconState.VectorLengths (p : Preset) (state : BeaconState) : Prop :=
  state.block_roots.length = p.SLOTS_PER_HISTORICAL_ROOT ∧
  state.state_roots.length = p.SLOTS_PER_HISTORICAL_ROOT ∧
  state.randao_mixes.length = p.EPOCHS_PER_HISTORICAL_VECTOR ∧
  state.slashings.length = p.EPOCHS_PER_SLASHINGS_VECTOR ∧
  state.justification_bits.length = JUSTIFICATION_BITS_LENGTH ∧
  state.current_sync_committee.pubkeys.length = p.SYNC_COMMITTEE_SIZE ∧
  state.next_sync_committee.pubkeys.length = p.SYNC_COMMITTEE_SIZE ∧
  state.proposer_lookahead.length = (p.MIN_SEED_LOOKAHEAD + 1) * p.SLOTS_PER_EPOCH ∧
  state.execution_payload_availability.length = p.SLOTS_PER_HISTORICAL_ROOT ∧
  state.builder_pending_payments.length = 2 * p.SLOTS_PER_EPOCH ∧
  state.ptc_window.length = (p.MIN_SEED_LOOKAHEAD + 2) * p.SLOTS_PER_EPOCH ∧
  ∀ ptc ∈ state.ptc_window, ptc.length = p.PTC_SIZE

end EpochProofs.Spec
