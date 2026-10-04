import EpochProofs.Spec.TotalActiveBalance
import EpochProofs.Spec.Helpers

/-!
# Reference: `process_justification_and_finalization`

Transcribed from `specs/phase0/beacon-chain.md` and `specs/altair/beacon-chain.md`
(v1.7.0-beta.2). Later forks do not change these functions. Each definition quotes its pyspec.
`get_block_root` is in `Spec/Helpers.lean`.
-/

namespace EpochProofs.Spec

/-- ```python
def weigh_justification_and_finalization(
    state: BeaconState,
    total_active_balance: Gwei,
    previous_epoch_target_balance: Gwei,
    current_epoch_target_balance: Gwei,
) -> None:
    previous_epoch = get_previous_epoch(state)
    current_epoch = get_current_epoch(state)
    old_previous_justified_checkpoint = state.previous_justified_checkpoint
    old_current_justified_checkpoint = state.current_justified_checkpoint

    # Process justifications
    state.previous_justified_checkpoint = state.current_justified_checkpoint
    state.justification_bits[1:] = state.justification_bits[: JUSTIFICATION_BITS_LENGTH - 1]
    state.justification_bits[0] = Boolean(False)
    if previous_epoch_target_balance * 3 >= total_active_balance * 2:
        state.current_justified_checkpoint = Checkpoint(
            epoch=previous_epoch, root=get_block_root(state, previous_epoch)
        )
        state.justification_bits[1] = Boolean(True)
    if current_epoch_target_balance * 3 >= total_active_balance * 2:
        state.current_justified_checkpoint = Checkpoint(
            epoch=current_epoch, root=get_block_root(state, current_epoch)
        )
        state.justification_bits[0] = Boolean(True)

    # Process finalizations
    bits = state.justification_bits
    # The 2nd/3rd/4th most recent epochs are justified, the 2nd using the 4th as source
    if all(bits[1:4]) and old_previous_justified_checkpoint.epoch + 3 == current_epoch:
        state.finalized_checkpoint = old_previous_justified_checkpoint
    # The 2nd/3rd most recent epochs are justified, the 2nd using the 3rd as source
    if all(bits[1:3]) and old_previous_justified_checkpoint.epoch + 2 == current_epoch:
        state.finalized_checkpoint = old_previous_justified_checkpoint
    # The 1st/2nd/3rd most recent epochs are justified, the 1st using the 3rd as source
    if all(bits[0:3]) and old_current_justified_checkpoint.epoch + 2 == current_epoch:
        state.finalized_checkpoint = old_current_justified_checkpoint
    # The 1st/2nd most recent epochs are justified, the 1st using the 2nd as source
    if all(bits[0:2]) and old_current_justified_checkpoint.epoch + 1 == current_epoch:
        state.finalized_checkpoint = old_current_justified_checkpoint
```

`justification_bits` is a `BitVector[4]`. The slice write `bits[1:] = bits[:3]` is
`bits.take 1 ++ bits.take 3` here. That is exact for a list of length 4. Python `and` computes
the epoch sum only when the bits are all set. So does this transcription. Each Python `if`
is one `let state ← ...` step, so the proofs take one step at a time. -/
def weigh_justification_and_finalization (p : Preset) (state : BeaconState)
    (total_active_balance previous_epoch_target_balance current_epoch_target_balance : Gwei) :
    SpecM BeaconState := do
  let previous_epoch ← get_previous_epoch p state
  let current_epoch ← get_current_epoch p state
  let old_previous_justified_checkpoint := state.previous_justified_checkpoint
  let old_current_justified_checkpoint := state.current_justified_checkpoint

  -- Process justifications
  let state := { state with
    previous_justified_checkpoint := state.current_justified_checkpoint }
  let state := { state with
    justification_bits :=
      state.justification_bits.take 1 ++
        state.justification_bits.take (JUSTIFICATION_BITS_LENGTH - 1) }
  let state := { state with justification_bits := ← listSet state.justification_bits 0 false }
  let state ← (do
    if (← uint64Mul previous_epoch_target_balance 3) ≥ (← uint64Mul total_active_balance 2) then
      let state := { state with
        current_justified_checkpoint :=
          { epoch := previous_epoch, root := ← get_block_root p state previous_epoch } }
      pure { state with justification_bits := ← listSet state.justification_bits 1 true }
    else pure state)
  let state ← (do
    if (← uint64Mul current_epoch_target_balance 3) ≥ (← uint64Mul total_active_balance 2) then
      let state := { state with
        current_justified_checkpoint :=
          { epoch := current_epoch, root := ← get_block_root p state current_epoch } }
      pure { state with justification_bits := ← listSet state.justification_bits 0 true }
    else pure state)

  -- Process finalizations
  let bits := state.justification_bits
  let state ← (do
    if (bits.extract 1 4).all id then do
      if (← uint64Add old_previous_justified_checkpoint.epoch 3) == current_epoch then
        pure { state with finalized_checkpoint := old_previous_justified_checkpoint }
      else pure state
    else pure state)
  let state ← (do
    if (bits.extract 1 3).all id then do
      if (← uint64Add old_previous_justified_checkpoint.epoch 2) == current_epoch then
        pure { state with finalized_checkpoint := old_previous_justified_checkpoint }
      else pure state
    else pure state)
  let state ← (do
    if (bits.extract 0 3).all id then do
      if (← uint64Add old_current_justified_checkpoint.epoch 2) == current_epoch then
        pure { state with finalized_checkpoint := old_current_justified_checkpoint }
      else pure state
    else pure state)
  if (bits.extract 0 2).all id then do
    if (← uint64Add old_current_justified_checkpoint.epoch 1) == current_epoch then
      pure { state with finalized_checkpoint := old_current_justified_checkpoint }
    else pure state
  else pure state

/-- ```python
def process_justification_and_finalization(state: BeaconState) -> None:
    # Initial FFG checkpoint values have a `0x00` stub for `root`.
    # Skip FFG updates in the first two epochs to avoid corner cases that might result in modifying this stub.
    if get_current_epoch(state) <= GENESIS_EPOCH + 1:
        return
    previous_indices = get_unslashed_participating_indices(
        state, TIMELY_TARGET_FLAG_INDEX, get_previous_epoch(state)
    )
    current_indices = get_unslashed_participating_indices(
        state, TIMELY_TARGET_FLAG_INDEX, get_current_epoch(state)
    )
    total_active_balance = get_total_active_balance(state)
    previous_target_balance = get_total_balance(state, previous_indices)
    current_target_balance = get_total_balance(state, current_indices)
    weigh_justification_and_finalization(
        state, total_active_balance, previous_target_balance, current_target_balance
    )
``` -/
def process_justification_and_finalization (p : Preset) (state : BeaconState) :
    SpecM BeaconState := do
  if (← get_current_epoch p state) ≤ GENESIS_EPOCH + 1 then
    return state
  let previous_indices ← get_unslashed_participating_indices p state TIMELY_TARGET_FLAG_INDEX
    (← get_previous_epoch p state)
  let current_indices ← get_unslashed_participating_indices p state TIMELY_TARGET_FLAG_INDEX
    (← get_current_epoch p state)
  let total_active_balance ← get_total_active_balance p state
  let previous_target_balance ← get_total_balance p state previous_indices
  let current_target_balance ← get_total_balance p state current_indices
  weigh_justification_and_finalization p state total_active_balance previous_target_balance
    current_target_balance

end EpochProofs.Spec
