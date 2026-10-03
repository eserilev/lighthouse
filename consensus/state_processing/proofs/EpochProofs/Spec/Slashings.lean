import EpochProofs.Spec.InactivityUpdates

/-!
# Reference: `process_slashings`

Transcribed line by line from `specs/electra/beacon-chain.md` (v1.7.0-beta.2). Gloas does not
change it. Each definition quotes its pyspec.

`get_total_active_balance(state)` is a parameter. The reference does not model it yet.
-/

namespace EpochProofs.Spec

/-- `Gwei(sum(values))` in Python. Each `uint64` addition raises on overflow. All values are
non-negative, so this raises exactly when the total does not fit. -/
def uint64Sum (values : List Uint64) : SpecM Uint64 :=
  if values.sum < UINT64_SIZE then pure values.sum else throw .overflow

/-- ```python
def decrease_balance(state: BeaconState, index: ValidatorIndex, delta: Gwei) -> None:
    state.balances[index] = saturating_sub(state.balances[index], delta)
``` -/
def decrease_balance (balances : List Gwei) (index : ValidatorIndex) (delta : Gwei) :
    SpecM (List Gwei) := do
  listSet balances index (saturating_sub (← listGet balances index) delta)

/-- ```python
def process_slashings(state: BeaconState) -> None:
    epoch = get_current_epoch(state)
    total_balance = get_total_active_balance(state)
    adjusted_total_slashing_balance = min(
        Gwei(sum(state.slashings)) * PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX, total_balance
    )
    increment = (
        EFFECTIVE_BALANCE_INCREMENT  # Factored out from total balance to avoid Uint64 overflow
    )
    penalty_per_effective_balance_increment = adjusted_total_slashing_balance // (
        total_balance // increment
    )
    for index, validator in enumerate(state.validators):
        if (
            validator.slashed
            and epoch + EPOCHS_PER_SLASHINGS_VECTOR // 2 == validator.withdrawable_epoch
        ):
            effective_balance_increments = validator.effective_balance // increment
            # [Modified in Electra:EIP7251]
            penalty = penalty_per_effective_balance_increment * effective_balance_increments
            decrease_balance(state, ValidatorIndex(index), penalty)
```

Python `and` does not evaluate its right side for a validator that is not slashed. -/
def process_slashings (p : Preset) (total_active_balance : Gwei) (state : BeaconState) :
    SpecM BeaconState := do
  let epoch ← get_current_epoch p state
  let total_balance := total_active_balance
  let adjusted_total_slashing_balance :=
    min (← uint64Mul (← uint64Sum state.slashings) p.PROPORTIONAL_SLASHING_MULTIPLIER_BELLATRIX)
      total_balance
  let increment := p.EFFECTIVE_BALANCE_INCREMENT
  let penalty_per_effective_balance_increment ←
    uint64Div adjusted_total_slashing_balance (← uint64Div total_balance increment)
  let mut balances := state.balances
  for (validator, index) in state.validators.zipIdx do
    if validator.slashed then
      let target ← uint64Add epoch (← uint64Div p.EPOCHS_PER_SLASHINGS_VECTOR 2)
      if target == validator.withdrawable_epoch then
        let effective_balance_increments ← uint64Div validator.effective_balance increment
        let penalty ←
          uint64Mul penalty_per_effective_balance_increment effective_balance_increments
        balances ← decrease_balance balances index penalty
  pure { state with balances }

end EpochProofs.Spec
