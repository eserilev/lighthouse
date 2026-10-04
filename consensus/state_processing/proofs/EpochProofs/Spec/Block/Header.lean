import EpochProofs.Spec.Helpers

/-!
# Reference: `process_block_header`

Transcribed from `specs/phase0/beacon-chain.md` (v1.7.0-beta.2). Later forks do not change it.
Hashes come from the `Oracle`.
-/

namespace EpochProofs.Spec

/-- ```python
def process_block_header(state: BeaconState, block: BeaconBlock) -> None:
    # Verify that the slots match
    assert block.slot == state.slot
    # Verify that the block is newer than latest block header
    assert block.slot > state.latest_block_header.slot
    # Verify that proposer index is the correct index
    assert block.proposer_index == get_beacon_proposer_index(state)
    # Verify that the parent matches
    assert block.parent_root == hash_tree_root(state.latest_block_header)
    # Cache current block as the new latest block
    state.latest_block_header = BeaconBlockHeader(
        slot=block.slot,
        proposer_index=block.proposer_index,
        parent_root=block.parent_root,
        state_root=Root(),  # Overwritten in the next process_slot call
        body_root=hash_tree_root(block.body),
    )

    # Verify proposer is not slashed
    proposer = state.validators[block.proposer_index]
    assert not proposer.slashed
``` -/
def process_block_header (p : Preset) (o : Oracle) (state : BeaconState) (block : BeaconBlock) :
    SpecM BeaconState := do
  if ¬ block.slot = state.slot then throw .assertionFailed
  if ¬ block.slot > state.latest_block_header.slot then throw .assertionFailed
  if ¬ block.proposer_index = (← get_beacon_proposer_index p state) then throw .assertionFailed
  if ¬ block.parent_root = o.hash_tree_root_BeaconBlockHeader state.latest_block_header then
    throw .assertionFailed
  let state := { state with
    latest_block_header := {
      slot := block.slot
      proposer_index := block.proposer_index
      parent_root := block.parent_root
      state_root := Bytes32.zero
      body_root := o.hash_tree_root_BeaconBlockBody block.body } }

  let proposer ← listGet state.validators block.proposer_index
  if proposer.slashed then throw .assertionFailed
  pure state

end EpochProofs.Spec
