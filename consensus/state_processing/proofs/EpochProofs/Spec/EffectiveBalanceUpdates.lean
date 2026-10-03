import EpochProofs.Spec.Types

/-!
# Reference: `process_effective_balance_updates`

Transcribed line by line from `specs/electra/beacon-chain.md` (v1.7.0-beta.2). Gloas does not
change it. Each definition quotes its pyspec.
-/

namespace EpochProofs.Spec

/-- ```python
def is_compounding_withdrawal_credential(withdrawal_credentials: Bytes32) -> bool:
    return withdrawal_credentials[:1] == COMPOUNDING_WITHDRAWAL_PREFIX
``` -/
def is_compounding_withdrawal_credential (withdrawal_credentials : Bytes32) : Bool :=
  withdrawal_credentials.toList.take 1 == [COMPOUNDING_WITHDRAWAL_PREFIX]

/-- ```python
def has_compounding_withdrawal_credential(validator: Validator) -> bool:
    return is_compounding_withdrawal_credential(validator.withdrawal_credentials)
``` -/
def has_compounding_withdrawal_credential (validator : Validator) : Bool :=
  is_compounding_withdrawal_credential validator.withdrawal_credentials

/-- ```python
def get_max_effective_balance(validator: Validator) -> Gwei:
    if has_compounding_withdrawal_credential(validator):
        return MAX_EFFECTIVE_BALANCE_ELECTRA
    else:
        return MIN_ACTIVATION_BALANCE
``` -/
def get_max_effective_balance (p : Preset) (validator : Validator) : Gwei :=
  if has_compounding_withdrawal_credential validator then p.MAX_EFFECTIVE_BALANCE_ELECTRA
  else p.MIN_ACTIVATION_BALANCE

/-- ```python
def process_effective_balance_updates(state: BeaconState) -> None:
    # Update effective balances with hysteresis
    for index, validator in enumerate(state.validators):
        balance = state.balances[index]
        HYSTERESIS_INCREMENT = Uint64(EFFECTIVE_BALANCE_INCREMENT // HYSTERESIS_QUOTIENT)
        DOWNWARD_THRESHOLD = HYSTERESIS_INCREMENT * HYSTERESIS_DOWNWARD_MULTIPLIER
        UPWARD_THRESHOLD = HYSTERESIS_INCREMENT * HYSTERESIS_UPWARD_MULTIPLIER
        # [Modified in Electra:EIP7251]
        max_effective_balance = get_max_effective_balance(validator)

        if (
            balance + DOWNWARD_THRESHOLD < validator.effective_balance
            or validator.effective_balance + UPWARD_THRESHOLD < balance
        ):
            validator.effective_balance = min(
                balance - balance % EFFECTIVE_BALANCE_INCREMENT, max_effective_balance
            )
```

Python mutates each validator in place. Here the loop builds the new list in order. Python
`or` does not evaluate its right side when the left side is true, so the second addition is in
the `else` branch. -/
def process_effective_balance_updates (p : Preset) (state : BeaconState) :
    SpecM BeaconState := do
  let mut validators := []
  for (validator, index) in state.validators.zipIdx do
    let balance ← match state.balances[index]? with
      | some balance => pure balance
      | none => throw .indexOutOfRange
    let HYSTERESIS_INCREMENT ← uint64Div p.EFFECTIVE_BALANCE_INCREMENT p.HYSTERESIS_QUOTIENT
    let DOWNWARD_THRESHOLD ← uint64Mul HYSTERESIS_INCREMENT p.HYSTERESIS_DOWNWARD_MULTIPLIER
    let UPWARD_THRESHOLD ← uint64Mul HYSTERESIS_INCREMENT p.HYSTERESIS_UPWARD_MULTIPLIER
    let max_effective_balance := get_max_effective_balance p validator

    let mut validator := validator
    let below ← uint64Add balance DOWNWARD_THRESHOLD
    let update ← if below < validator.effective_balance then pure true
      else do
        let above ← uint64Add validator.effective_balance UPWARD_THRESHOLD
        pure (decide (above < balance))
    if update then
      let remainder ← uint64Mod balance p.EFFECTIVE_BALANCE_INCREMENT
      let rounded ← uint64Sub balance remainder
      validator := { validator with effective_balance := min rounded max_effective_balance }
    validators := validators ++ [validator]
  pure { state with validators }

end EpochProofs.Spec
