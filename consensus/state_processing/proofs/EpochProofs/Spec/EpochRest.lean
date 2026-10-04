import EpochProofs.Spec.Helpers

/-!
# Reference: the reset and rotation steps of `process_epoch`

Transcribed from `specs/phase0/beacon-chain.md`, `specs/altair/beacon-chain.md` and
`specs/capella/beacon-chain.md` (v1.7.0-beta.2). Later forks do not change these functions.
Each definition quotes its pyspec.

Python evaluates the right side of `a[i] = b` before `i`. The reference keeps that order.
-/

namespace EpochProofs.Spec

/-- ```python
def process_eth1_data_reset(state: BeaconState) -> None:
    next_epoch = get_current_epoch(state) + 1
    # Reset eth1 data votes
    if next_epoch % EPOCHS_PER_ETH1_VOTING_PERIOD == 0:
        state.eth1_data_votes = Eth1DataVotes()
``` -/
def process_eth1_data_reset (p : Preset) (state : BeaconState) : SpecM BeaconState := do
  let next_epoch ← uint64Add (← get_current_epoch p state) 1
  if (← uint64Mod next_epoch p.EPOCHS_PER_ETH1_VOTING_PERIOD) == 0 then
    pure { state with eth1_data_votes := [] }
  else
    pure state

/-- ```python
def process_slashings_reset(state: BeaconState) -> None:
    next_epoch = get_current_epoch(state) + 1
    # Reset slashings
    state.slashings[next_epoch % EPOCHS_PER_SLASHINGS_VECTOR] = Gwei(0)
``` -/
def process_slashings_reset (p : Preset) (state : BeaconState) : SpecM BeaconState := do
  let next_epoch ← uint64Add (← get_current_epoch p state) 1
  pure { state with
    slashings :=
      ← listSet state.slashings (← uint64Mod next_epoch p.EPOCHS_PER_SLASHINGS_VECTOR) 0 }

/-- ```python
def process_randao_mixes_reset(state: BeaconState) -> None:
    current_epoch = get_current_epoch(state)
    next_epoch = current_epoch + 1
    # Set randao mix
    state.randao_mixes[next_epoch % EPOCHS_PER_HISTORICAL_VECTOR] = get_randao_mix(
        state, current_epoch
    )
``` -/
def process_randao_mixes_reset (p : Preset) (state : BeaconState) : SpecM BeaconState := do
  let current_epoch ← get_current_epoch p state
  let next_epoch ← uint64Add current_epoch 1
  let mix ← get_randao_mix p state current_epoch
  pure { state with
    randao_mixes :=
      ← listSet state.randao_mixes (← uint64Mod next_epoch p.EPOCHS_PER_HISTORICAL_VECTOR) mix }

/-- ```python
def process_historical_summaries_update(state: BeaconState) -> None:
    # Set historical block root accumulator.
    next_epoch = get_current_epoch(state) + 1
    if next_epoch % Uint64(SLOTS_PER_HISTORICAL_ROOT // SLOTS_PER_EPOCH) == 0:
        historical_summary = HistoricalSummary(
            block_summary_root=hash_tree_root(state.block_roots),
            state_summary_root=hash_tree_root(state.state_roots),
        )
        state.historical_summaries.append(historical_summary)
``` -/
def process_historical_summaries_update (p : Preset) (o : Oracle) (state : BeaconState) :
    SpecM BeaconState := do
  let next_epoch ← uint64Add (← get_current_epoch p state) 1
  if (← uint64Mod next_epoch (← uint64Div p.SLOTS_PER_HISTORICAL_ROOT p.SLOTS_PER_EPOCH)) == 0
  then
    let historical_summary : HistoricalSummary := {
      block_summary_root := o.hash_tree_root_BlockRoots state.block_roots
      state_summary_root := o.hash_tree_root_StateRoots state.state_roots }
    pure { state with historical_summaries := state.historical_summaries ++ [historical_summary] }
  else
    pure state

/-- ```python
def process_participation_flag_updates(state: BeaconState) -> None:
    state.previous_epoch_participation = state.current_epoch_participation
    state.current_epoch_participation = EpochParticipation(
        data=[ParticipationFlags(0b0000_0000) for _ in range(len(state.validators))]
    )
``` -/
def process_participation_flag_updates (state : BeaconState) : BeaconState :=
  let state := { state with previous_epoch_participation := state.current_epoch_participation }
  { state with current_epoch_participation := List.replicate state.validators.length 0 }

end EpochProofs.Spec
