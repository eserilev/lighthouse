/-!
# Gloas types and `Uint64` arithmetic

Transcribed from `specs/gloas/beacon-chain.md` (v1.7.0-beta.0). Names match the spec.

pyspec raises on `Uint64` overflow and on division by zero. Here a raise is `Except.error`.

Write this file from the spec only. Do not read Lighthouse.
-/

namespace EpochProofs.Spec

abbrev Gwei := Nat
abbrev Uint64 := Nat
abbrev ValidatorIndex := Nat
abbrev BuilderIndex := Nat
abbrev ExecutionAddress := Vector UInt8 20

inductive SpecError where
  | overflow
  | divisionByZero
  deriving DecidableEq, Repr

abbrev SpecM := Except SpecError

def UINT64_SIZE : Nat := 2 ^ 64

def uint64Mul (a b : Uint64) : SpecM Uint64 :=
  if a * b < UINT64_SIZE then pure (a * b) else throw .overflow

def uint64Div (a b : Uint64) : SpecM Uint64 :=
  if b = 0 then throw .divisionByZero else pure (a / b)

/-- Preset values that the reference reads. -/
structure Preset where
  SLOTS_PER_EPOCH : Nat

def Preset.mainnet : Preset := { SLOTS_PER_EPOCH := 32 }
def Preset.minimal : Preset := { SLOTS_PER_EPOCH := 8 }

def BUILDER_PAYMENT_THRESHOLD_NUMERATOR : Uint64 := 6
def BUILDER_PAYMENT_THRESHOLD_DENOMINATOR : Uint64 := 10

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
  builder_pending_payments : List BuilderPendingPayment
  builder_pending_withdrawals : List BuilderPendingWithdrawal
  deriving DecidableEq, Repr

def BeaconState.WellFormed (p : Preset) (state : BeaconState) : Prop :=
  state.builder_pending_payments.length = 2 * p.SLOTS_PER_EPOCH

end EpochProofs.Spec
