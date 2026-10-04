import EpochProofs.Spec.Block.Types

/-!
# Oracle: the functions the reference takes as parameters

The reference does not compute SHA-256, SSZ `hash_tree_root` or BLS. It takes them as fields of
an `Oracle` value. A field is a parameter, not an axiom. A theorem that holds for every
`Oracle` does not depend on the hash or signature values.
-/

namespace EpochProofs.Spec

/-- The hash, SSZ and BLS functions of the spec.

`hash` is `sha256`. Shuffling, seeds, committee selection and Merkle branches use only `hash`
and integer arithmetic, so the reference computes them from `hash`.

Each `hash_tree_root_T` is `hash_tree_root` on the SSZ type `T`. Add a field when a new
transcription hashes a new type. A new field does not break code that takes an `Oracle`.

`bls_*` are the IETF BLS functions that the spec calls as `bls.*`. They return `Bool`, so a
failed check is an `assert` in the reference. -/
structure Oracle where
  hash : List UInt8 → Bytes32
  hash_tree_root_BeaconState : BeaconState → Root
  hash_tree_root_BeaconBlockHeader : BeaconBlockHeader → Root
  hash_tree_root_BeaconBlock : BeaconBlock → Root
  hash_tree_root_BeaconBlockBody : BeaconBlockBody → Root
  /-- `hash_tree_root` on `BlockRoots`, a `Vector[Root, SLOTS_PER_HISTORICAL_ROOT]`. -/
  hash_tree_root_BlockRoots : List Root → Root
  /-- `hash_tree_root` on `StateRoots`, a `Vector[Root, SLOTS_PER_HISTORICAL_ROOT]`. -/
  hash_tree_root_StateRoots : List Root → Root
  hash_tree_root_Epoch : Epoch → Root
  hash_tree_root_SigningData : SigningData → Root
  hash_tree_root_ForkData : ForkData → Root
  hash_tree_root_AttestationData : AttestationData → Root
  hash_tree_root_VoluntaryExit : VoluntaryExit → Root
  hash_tree_root_DepositMessage : DepositMessage → Root
  hash_tree_root_BLSToExecutionChange : BLSToExecutionChange → Root
  hash_tree_root_ExecutionPayloadBid : ExecutionPayloadBid → Root
  hash_tree_root_ExecutionPayloadEnvelope : ExecutionPayloadEnvelope → Root
  hash_tree_root_PayloadAttestationData : PayloadAttestationData → Root
  hash_tree_root_ExecutionRequests : ExecutionRequests → Root
  /-- `hash_tree_root` on `Withdrawals`, a `ProgressiveList[Withdrawal]`. -/
  hash_tree_root_Withdrawals : List Withdrawal → Root
  /-- `bls.Verify(pubkey, message, signature)` -/
  bls_Verify : BLSPubkey → Root → BLSSignature → Bool
  /-- `bls.FastAggregateVerify(pubkeys, message, signature)` -/
  bls_FastAggregateVerify : List BLSPubkey → Root → BLSSignature → Bool
  /-- `bls.AggregateVerify(pubkeys, messages, signature)` -/
  bls_AggregateVerify : List BLSPubkey → List Root → BLSSignature → Bool
  /-- `bls.AggregatePKs(pubkeys)` -/
  bls_AggregatePKs : List BLSPubkey → BLSPubkey

end EpochProofs.Spec
