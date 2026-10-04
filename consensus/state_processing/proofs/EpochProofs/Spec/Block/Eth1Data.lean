import EpochProofs.Spec.Helpers

/-!
# Reference: `process_eth1_data`

Transcribed from `specs/phase0/beacon-chain.md` (v1.7.0-beta.2). Later forks do not change it.
-/

namespace EpochProofs.Spec

/-- ```python
def process_eth1_data(state: BeaconState, body: BeaconBlockBody) -> None:
    state.eth1_data_votes.append(body.eth1_data)
    if (
        state.eth1_data_votes.count(body.eth1_data) * 2
        > Uint64(EPOCHS_PER_ETH1_VOTING_PERIOD) * SLOTS_PER_EPOCH
    ):
        state.eth1_data = body.eth1_data
```

`eth1_data_votes` has limit `EPOCHS_PER_ETH1_VOTING_PERIOD * SLOTS_PER_EPOCH`. An append past
the limit raises.

`count` returns a Python `int`, so `count * 2` does not overflow. -/
def process_eth1_data (p : Preset) (state : BeaconState) (body : BeaconBlockBody) :
    SpecM BeaconState := do
  let limit ← uint64Mul p.EPOCHS_PER_ETH1_VOTING_PERIOD p.SLOTS_PER_EPOCH
  if ¬ state.eth1_data_votes.length < limit then throw .indexOutOfRange
  let state := { state with eth1_data_votes := state.eth1_data_votes ++ [body.eth1_data] }
  if state.eth1_data_votes.count body.eth1_data * 2
      > (← uint64Mul p.EPOCHS_PER_ETH1_VOTING_PERIOD p.SLOTS_PER_EPOCH) then
    pure { state with eth1_data := body.eth1_data }
  else
    pure state

end EpochProofs.Spec
