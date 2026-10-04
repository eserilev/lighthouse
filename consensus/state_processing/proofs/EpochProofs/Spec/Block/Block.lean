import EpochProofs.Spec.Epoch
import EpochProofs.Spec.Slots
import EpochProofs.Spec.Block.Header
import EpochProofs.Spec.Block.Randao
import EpochProofs.Spec.Block.Eth1Data
import EpochProofs.Spec.Block.Withdrawals
import EpochProofs.Spec.Block.ParentPayload
import EpochProofs.Spec.Block.Slashings
import EpochProofs.Spec.Block.Exits
import EpochProofs.Spec.Block.Attestations
import EpochProofs.Spec.Block.SyncAggregate

/-!
# Reference: `process_operations`, `process_block` and `state_transition`

Transcribed from `specs/phase0` and `specs/gloas/beacon-chain.md` (v1.7.0-beta.2). Each
definition quotes the pyspec of its latest fork. Two values are parameters:
`max_blobs_per_block epoch` is `get_blob_parameters(epoch).max_blobs_per_block`, and
`GLOAS_FORK_EPOCH` is the Gloas fork epoch of the config.
-/

namespace EpochProofs.Spec

/-- Phase0 preset, mainnet value. -/
def MAX_PROPOSER_SLASHINGS : Uint64 := 16
/-- Electra preset, mainnet value. -/
def MAX_ATTESTER_SLASHINGS_ELECTRA : Uint64 := 1
/-- Electra preset, mainnet value. -/
def MAX_ATTESTATIONS_ELECTRA : Uint64 := 8
/-- Phase0 preset, mainnet value. -/
def MAX_VOLUNTARY_EXITS : Uint64 := 16
/-- Capella preset, mainnet value. -/
def MAX_BLS_TO_EXECUTION_CHANGES : Uint64 := 16
/-- Gloas preset, mainnet value. -/
def MAX_PAYLOAD_ATTESTATIONS : Uint64 := 4

/-- ```python
def process_operations(
    state: BeaconState,
    body: BeaconBlockBody,
    # [New in Gloas:EIP7732]
    parent_slot: Slot,
) -> None:
    assert len(body.deposits) == 0

    # [Modified in Gloas:EIP7732]
    def for_ops(operations: Sequence[Any], fn: Callable[..., None], *args: Any) -> None:
        for operation in operations:
            fn(state, operation, *args)

    # [New in Gloas:EIP7688]
    assert len(body.proposer_slashings) <= MAX_PROPOSER_SLASHINGS
    assert len(body.attester_slashings) <= MAX_ATTESTER_SLASHINGS_ELECTRA
    assert len(body.attestations) <= MAX_ATTESTATIONS_ELECTRA
    assert len(body.voluntary_exits) <= MAX_VOLUNTARY_EXITS
    assert len(body.bls_to_execution_changes) <= MAX_BLS_TO_EXECUTION_CHANGES
    assert len(body.payload_attestations) <= MAX_PAYLOAD_ATTESTATIONS

    # [Modified in Gloas:EIP7732]
    for_ops(body.proposer_slashings, process_proposer_slashing)
    for_ops(body.attester_slashings, process_attester_slashing)
    # [Modified in Gloas:EIP7732]
    for_ops(body.attestations, process_attestation, parent_slot)
    for_ops(body.voluntary_exits, process_voluntary_exit)
    for_ops(body.bls_to_execution_changes, process_bls_to_execution_change)
    # [Modified in Gloas:EIP7732]
    # Removed `process_deposit_request`
    # [Modified in Gloas:EIP7732]
    # Removed `process_withdrawal_request`
    # [Modified in Gloas:EIP7732]
    # Removed `process_consolidation_request`
    # [New in Gloas:EIP7732]
    for_ops(body.payload_attestations, process_payload_attestation)
```

Each `for_ops` is a `List.foldlM`. The operation limits are constants with the mainnet
values. -/
def process_operations (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (state : BeaconState) (body : BeaconBlockBody) (parent_slot : Slot) :
    SpecM BeaconState := do
  if ¬ body.deposits.length = 0 then throw .assertionFailed

  if ¬ body.proposer_slashings.length ≤ MAX_PROPOSER_SLASHINGS then throw .assertionFailed
  if ¬ body.attester_slashings.length ≤ MAX_ATTESTER_SLASHINGS_ELECTRA then
    throw .assertionFailed
  if ¬ body.attestations.length ≤ MAX_ATTESTATIONS_ELECTRA then throw .assertionFailed
  if ¬ body.voluntary_exits.length ≤ MAX_VOLUNTARY_EXITS then throw .assertionFailed
  if ¬ body.bls_to_execution_changes.length ≤ MAX_BLS_TO_EXECUTION_CHANGES then
    throw .assertionFailed
  if ¬ body.payload_attestations.length ≤ MAX_PAYLOAD_ATTESTATIONS then throw .assertionFailed

  let state ← body.proposer_slashings.foldlM (process_proposer_slashing p o) state
  let state ← body.attester_slashings.foldlM (process_attester_slashing p o) state
  let state ← body.attestations.foldlM
    (fun state attestation => process_attestation p o state attestation parent_slot) state
  let state ← body.voluntary_exits.foldlM (process_voluntary_exit p o) state
  let state ← body.bls_to_execution_changes.foldlM (process_bls_to_execution_change p o) state
  body.payload_attestations.foldlM (process_payload_attestation p o GLOAS_FORK_EPOCH) state

/-- ```python
def process_block(state: BeaconState, block: BeaconBlock) -> None:
    # [New in Gloas:EIP7732]
    parent_slot = state.latest_block_header.slot

    # [New in Gloas:EIP7732]
    process_parent_execution_payload(state, block)
    process_block_header(state, block)
    # [Modified in Gloas:EIP7732]
    process_withdrawals(state)
    # [Modified in Gloas:EIP7732]
    # Removed `process_execution_payload`
    # [New in Gloas:EIP7732]
    process_execution_payload_bid(state, block.body.signed_execution_payload_bid)
    process_randao(state, block.body)
    process_eth1_data(state, block.body)
    # [Modified in Gloas:EIP7732]
    process_operations(state, block.body, parent_slot)
    process_sync_aggregate(state, block.body.sync_aggregate)
``` -/
def process_block (p : Preset) (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (state : BeaconState) (block : BeaconBlock) :
    SpecM BeaconState := do
  let parent_slot := state.latest_block_header.slot

  let state ← process_parent_execution_payload p o state block
  let state ← process_block_header p o state block
  let state ← process_withdrawals p state
  let state ← process_execution_payload_bid p o max_blobs_per_block state
    block.body.signed_execution_payload_bid
  let state ← process_randao p o state block.body
  let state ← process_eth1_data p state block.body
  let state ← process_operations p o GLOAS_FORK_EPOCH state block.body parent_slot
  process_sync_aggregate p o state block.body.sync_aggregate

/-- ```python
def verify_block_signature(state: BeaconState, signed_block: SignedBeaconBlock) -> bool:
    proposer = state.validators[signed_block.message.proposer_index]
    signing_root = compute_signing_root(
        signed_block.message, get_domain(state, DOMAIN_BEACON_PROPOSER)
    )
    return bls.Verify(proposer.pubkey, signing_root, signed_block.signature)
``` -/
def verify_block_signature (p : Preset) (o : Oracle) (state : BeaconState)
    (signed_block : SignedBeaconBlock) : SpecM Bool := do
  let proposer ← listGet state.validators signed_block.message.proposer_index
  let signing_root := compute_signing_root o
    (o.hash_tree_root_BeaconBlock signed_block.message)
    (← get_domain p o state DOMAIN_BEACON_PROPOSER)
  pure (o.bls_Verify proposer.pubkey signing_root signed_block.signature)

/-- ```python
def state_transition(
    state: BeaconState, signed_block: SignedBeaconBlock, validate_result: bool = True
) -> None:
    block = signed_block.message
    # Process slots (including those with no blocks) since block
    process_slots(state, block.slot)
    # Verify signature
    if validate_result:
        assert verify_block_signature(state, signed_block)
    # Process block
    process_block(state, block)
    # Verify state root
    if validate_result:
        assert block.state_root == hash_tree_root(state)
```

`process_slots` runs `process_epoch` at the end of each epoch. -/
def state_transition (p : Preset) (o : Oracle) (max_blobs_per_block : Epoch → Uint64)
    (GLOAS_FORK_EPOCH : Epoch) (state : BeaconState) (signed_block : SignedBeaconBlock)
    (validate_result : Bool := true) : SpecM BeaconState := do
  let block := signed_block.message
  let state ← process_slots p o (process_epoch p o) state block.slot
  if validate_result then
    if ¬ (← verify_block_signature p o state signed_block) then throw .assertionFailed
  let state ← process_block p o max_blobs_per_block GLOAS_FORK_EPOCH state block
  if validate_result then
    if ¬ block.state_root = o.hash_tree_root_BeaconState state then throw .assertionFailed
  pure state

end EpochProofs.Spec
