import EpochProofs.Spec.RewardsAndPenalties

/-!
# Reference: `get_total_active_balance`

Transcribed from `specs/phase0/beacon-chain.md` (v1.7.0-beta.2). The epoch references take the
total active balance as a parameter. Block operations compute it with this function.
-/

namespace EpochProofs.Spec

/-- ```python
def get_total_active_balance(state: BeaconState) -> Gwei:
    return get_total_balance(
        state, set(get_active_validator_indices(state, get_current_epoch(state)))
    )
``` -/
def get_total_active_balance (p : Preset) (state : BeaconState) : SpecM Gwei := do
  get_total_balance p state (get_active_validator_indices state (← get_current_epoch p state))

end EpochProofs.Spec
