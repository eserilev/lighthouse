import EpochProofs.Spec.Helpers

/-!
# Reference: `process_slot` and `process_slots`

Transcribed from `specs/phase0/beacon-chain.md` and `specs/gloas/beacon-chain.md`
(v1.7.0-beta.2). Each definition quotes its pyspec.

`process_epoch` is a parameter `processEpoch`. The real one plugs in later.
-/

namespace EpochProofs.Spec

/-- ```python
def process_slot(state: BeaconState) -> None:
    slot_index = state.slot % SLOTS_PER_HISTORICAL_ROOT
    # Cache state root
    previous_state_root = hash_tree_root(state)
    state.state_roots[slot_index] = previous_state_root
    # Cache latest block header state root
    if state.latest_block_header.state_root == Bytes32():
        state.latest_block_header.state_root = previous_state_root
    # Cache block root
    previous_block_root = hash_tree_root(state.latest_block_header)
    state.block_roots[slot_index] = previous_block_root
    # [New in Gloas:EIP7732]
    # Unset the next payload availability
    next_slot_index = (state.slot + 1) % SLOTS_PER_HISTORICAL_ROOT
    state.execution_payload_availability[next_slot_index] = Boolean(False)
``` -/
def process_slot (p : Preset) (o : Oracle) (state : BeaconState) : SpecM BeaconState := do
  let slot_index ← uint64Mod state.slot p.SLOTS_PER_HISTORICAL_ROOT
  let previous_state_root := o.hash_tree_root_BeaconState state
  let state := { state with
    state_roots := ← listSet state.state_roots slot_index previous_state_root }
  let state :=
    if state.latest_block_header.state_root == Bytes32.zero then
      { state with
        latest_block_header := { state.latest_block_header with
          state_root := previous_state_root } }
    else state
  let previous_block_root := o.hash_tree_root_BeaconBlockHeader state.latest_block_header
  let state := { state with
    block_roots := ← listSet state.block_roots slot_index previous_block_root }
  let next_slot_index ← uint64Mod (← uint64Add state.slot 1) p.SLOTS_PER_HISTORICAL_ROOT
  pure { state with
    execution_payload_availability :=
      ← listSet state.execution_payload_availability next_slot_index false }

/-- The `while` loop of `process_slots`, with at most `fuel` iterations.

Each iteration tests `state.slot < slot` first, as the `while` does. If `processEpoch` keeps
`state.slot`, each iteration adds one to `state.slot`. Then `slot - state.slot` iterations
reach the end of the `while`, and the loop equals pyspec. `process_slots_slot` uses this. -/
def process_slots_loop (p : Preset) (o : Oracle)
    (processEpoch : BeaconState → SpecM BeaconState) (slot : Slot) :
    Nat → BeaconState → SpecM BeaconState
  | 0, state => pure state
  | fuel + 1, state => do
    if state.slot < slot then
      let state ← process_slot p o state
      let state ←
        if (← uint64Mod (← uint64Add state.slot 1) p.SLOTS_PER_EPOCH) == 0 then
          processEpoch state
        else pure state
      let state := { state with slot := ← uint64Add state.slot 1 }
      process_slots_loop p o processEpoch slot fuel state
    else
      pure state

/-- ```python
def process_slots(state: BeaconState, slot: Slot) -> None:
    assert state.slot < slot
    while state.slot < slot:
        process_slot(state)
        # Process epoch on the start slot of the next epoch
        if (state.slot + 1) % SLOTS_PER_EPOCH == 0:
            process_epoch(state)
        state.slot = state.slot + 1
```

The `while` is `process_slots_loop` with `slot - state.slot` iterations. -/
def process_slots (p : Preset) (o : Oracle) (processEpoch : BeaconState → SpecM BeaconState)
    (state : BeaconState) (slot : Slot) : SpecM BeaconState := do
  if ¬ state.slot < slot then throw .assertionFailed
  process_slots_loop p o processEpoch slot (slot - state.slot) state

end EpochProofs.Spec
