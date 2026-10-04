import EpochProofs.Spec.Committees
import EpochProofs.Spec.Block.SyncAggregate

/-!
# Reference: sync committee, proposer lookahead and PTC window updates

Transcribed from `specs/altair`, `specs/fulu` and `specs/gloas/beacon-chain.md`
(v1.7.0-beta.2). Each definition quotes the pyspec of its latest fork. `sha256` and BLS come
from the `Oracle`.
-/

namespace EpochProofs.Spec

/-- ```python
def get_next_sync_committee(state: BeaconState) -> SyncCommittee:
    indices = get_next_sync_committee_indices(state)
    pubkeys = SyncCommitteePubkeys(data=[state.validators[index].pubkey for index in indices])
    aggregate_pubkey = eth_aggregate_pubkeys(pubkeys)
    return SyncCommittee(pubkeys=pubkeys, aggregate_pubkey=aggregate_pubkey)
``` -/
def get_next_sync_committee (p : Preset) (o : Oracle) (state : BeaconState) :
    SpecM SyncCommittee := do
  let indices ← get_next_sync_committee_indices p o state
  let pubkeys ← indices.mapM fun index => do pure (← listGet state.validators index).pubkey
  let aggregate_pubkey ← eth_aggregate_pubkeys o pubkeys
  pure { pubkeys, aggregate_pubkey }

/-- ```python
def process_sync_committee_updates(state: BeaconState) -> None:
    next_epoch = get_current_epoch(state) + 1
    if next_epoch % EPOCHS_PER_SYNC_COMMITTEE_PERIOD == 0:
        state.current_sync_committee = state.next_sync_committee
        state.next_sync_committee = get_next_sync_committee(state)
``` -/
def process_sync_committee_updates (p : Preset) (o : Oracle) (state : BeaconState) :
    SpecM BeaconState := do
  let next_epoch ← uint64Add (← get_current_epoch p state) 1
  if (← uint64Mod next_epoch p.EPOCHS_PER_SYNC_COMMITTEE_PERIOD) == 0 then
    let state := { state with current_sync_committee := state.next_sync_committee }
    pure { state with next_sync_committee := ← get_next_sync_committee p o state }
  else
    pure state

/-- ```python
def process_proposer_lookahead(state: BeaconState) -> None:
    last_epoch_start = len(state.proposer_lookahead) - SLOTS_PER_EPOCH
    # Shift out proposers in the first epoch
    state.proposer_lookahead[:last_epoch_start] = state.proposer_lookahead[SLOTS_PER_EPOCH:]
    # Fill in the last epoch with new proposer indices
    last_epoch_proposers = get_beacon_proposer_indices(
        state, get_current_epoch(state) + MIN_SEED_LOOKAHEAD + 1
    )
    state.proposer_lookahead[last_epoch_start:] = last_epoch_proposers
```

`len(...) - SLOTS_PER_EPOCH` is a `Uint64`, so it raises when the vector is shorter than one
epoch. A slice assignment replaces the slice with the new items. -/
def process_proposer_lookahead (p : Preset) (o : Oracle) (state : BeaconState) :
    SpecM BeaconState := do
  let last_epoch_start ← uint64Sub state.proposer_lookahead.length p.SLOTS_PER_EPOCH
  let state := { state with
    proposer_lookahead := state.proposer_lookahead.drop p.SLOTS_PER_EPOCH ++
      state.proposer_lookahead.drop last_epoch_start }
  let last_epoch_proposers ← get_beacon_proposer_indices p o state
    (← uint64Add (← uint64Add (← get_current_epoch p state) p.MIN_SEED_LOOKAHEAD) 1)
  pure { state with
    proposer_lookahead := state.proposer_lookahead.take last_epoch_start ++ last_epoch_proposers }

/-- ```python
def process_ptc_window(state: BeaconState) -> None:
    # Shift all epochs forward by one
    state.ptc_window[: len(state.ptc_window) - SLOTS_PER_EPOCH] = state.ptc_window[SLOTS_PER_EPOCH:]
    # Fill in the last epoch
    next_epoch = get_current_epoch(state) + MIN_SEED_LOOKAHEAD + 1
    start_slot = compute_start_slot_at_epoch(next_epoch)
    state.ptc_window[len(state.ptc_window) - SLOTS_PER_EPOCH :] = [
        compute_ptc(state, Slot(slot)) for slot in range(start_slot, start_slot + SLOTS_PER_EPOCH)
    ]
```

`len(...) - SLOTS_PER_EPOCH` is a `Uint64`, so it raises when the vector is shorter than one
epoch. A slice assignment replaces the slice with the new items. -/
def process_ptc_window (p : Preset) (o : Oracle) (state : BeaconState) :
    SpecM BeaconState := do
  let shift_end ← uint64Sub state.ptc_window.length p.SLOTS_PER_EPOCH
  let state := { state with
    ptc_window := state.ptc_window.drop p.SLOTS_PER_EPOCH ++ state.ptc_window.drop shift_end }
  let next_epoch ← uint64Add (← uint64Add (← get_current_epoch p state) p.MIN_SEED_LOOKAHEAD) 1
  let start_slot ← compute_start_slot_at_epoch p next_epoch
  let end_slot ← uint64Add start_slot p.SLOTS_PER_EPOCH
  let ptcs ← (List.range' start_slot (end_slot - start_slot)).mapM fun slot =>
    compute_ptc p o state slot
  let fill_start ← uint64Sub state.ptc_window.length p.SLOTS_PER_EPOCH
  pure { state with ptc_window := state.ptc_window.take fill_start ++ ptcs }

end EpochProofs.Spec
