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

def Preset.mainnet : Preset := { SLOTS_PER_EPOCH := 32 }
def Preset.minimal : Preset :=
  { SLOTS_PER_EPOCH := 8, EPOCHS_PER_SLASHINGS_VECTOR := 64,
    MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA := 64000000000, CHURN_LIMIT_QUOTIENT_GLOAS := 16 }

def BUILDER_PAYMENT_THRESHOLD_NUMERATOR : Uint64 := 6
def BUILDER_PAYMENT_THRESHOLD_DENOMINATOR : Uint64 := 10

def COMPOUNDING_WITHDRAWAL_PREFIX : UInt8 := 0x02

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

abbrev Bytes32 := Vector UInt8 32

/-- Only the fields that the reference reads or writes. -/
structure Validator where
  withdrawal_credentials : Bytes32
  effective_balance : Gwei
  slashed : Bool
  activation_eligibility_epoch : Epoch
  activation_epoch : Epoch
  exit_epoch : Epoch
  withdrawable_epoch : Epoch
  deriving DecidableEq, Repr

structure Checkpoint where
  epoch : Epoch
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

/-- Only the fields that the reference reads or writes.

`builder_pending_withdrawals` is a `ProgressiveList`. It has no limit.
A `List` has no fixed length, so `WellFormed` gives the vector length. -/
structure BeaconState where
  slot : Slot := 0
  validators : List Validator := []
  balances : List Gwei := []
  finalized_checkpoint : Checkpoint := ⟨0⟩
  previous_epoch_participation : List ParticipationFlags := []
  current_epoch_participation : List ParticipationFlags := []
  inactivity_scores : List Uint64 := []
  slashings : List Gwei := []
  earliest_exit_epoch : Epoch := 0
  exit_balance_to_consume : Gwei := 0
  builder_pending_payments : List BuilderPendingPayment
  builder_pending_withdrawals : List BuilderPendingWithdrawal
  deriving DecidableEq, Repr

def BeaconState.WellFormed (p : Preset) (state : BeaconState) : Prop :=
  state.builder_pending_payments.length = 2 * p.SLOTS_PER_EPOCH

end EpochProofs.Spec
