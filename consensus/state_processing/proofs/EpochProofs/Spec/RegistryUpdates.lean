import EpochProofs.Spec.RewardsAndPenalties

/-!
# Reference: `process_registry_updates`

Transcribed line by line from `specs/phase0/beacon-chain.md`, `specs/electra/beacon-chain.md`
and `specs/gloas/beacon-chain.md` (v1.7.0-beta.2). Each definition quotes its pyspec.

`get_total_active_balance(state)` is a parameter. The reference does not model it yet.
-/

namespace EpochProofs.Spec

/-- ```python
def compute_activation_exit_epoch(epoch: Epoch) -> Epoch:
    return epoch + 1 + MAX_SEED_LOOKAHEAD
``` -/
def compute_activation_exit_epoch (p : Preset) (epoch : Epoch) : SpecM Epoch := do
  uint64Add (← uint64Add epoch 1) p.MAX_SEED_LOOKAHEAD

/-- ```python
def get_exit_churn_limit(state: BeaconState) -> Gwei:
    churn = max(
        MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA,
        get_total_active_balance(state) // CHURN_LIMIT_QUOTIENT_GLOAS,
    )
    return churn - churn % EFFECTIVE_BALANCE_INCREMENT
``` -/
def get_exit_churn_limit (p : Preset) (total_active_balance : Gwei) : SpecM Gwei := do
  let churn := max p.MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA
    (← uint64Div total_active_balance p.CHURN_LIMIT_QUOTIENT_GLOAS)
  uint64Sub churn (← uint64Mod churn p.EFFECTIVE_BALANCE_INCREMENT)

/-- ```python
def compute_exit_epoch_and_update_churn(state: BeaconState, exit_balance: Gwei) -> Epoch:
    earliest_exit_epoch = max(
        state.earliest_exit_epoch, compute_activation_exit_epoch(get_current_epoch(state))
    )
    # [Modified in Gloas:EIP8061]
    per_epoch_churn = get_exit_churn_limit(state)
    # New epoch for exits.
    if state.earliest_exit_epoch < earliest_exit_epoch:
        exit_balance_to_consume = per_epoch_churn
    else:
        exit_balance_to_consume = state.exit_balance_to_consume

    # Exit doesn't fit in the current earliest epoch.
    if exit_balance > exit_balance_to_consume:
        balance_to_process = exit_balance - exit_balance_to_consume
        additional_epochs = (balance_to_process - 1) // per_epoch_churn + 1
        earliest_exit_epoch += Epoch(additional_epochs)
        exit_balance_to_consume += additional_epochs * per_epoch_churn

    # Consume the balance and update state variables.
    state.exit_balance_to_consume = exit_balance_to_consume - exit_balance
    state.earliest_exit_epoch = earliest_exit_epoch

    return state.earliest_exit_epoch
``` -/
def compute_exit_epoch_and_update_churn (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (exit_balance : Gwei) : SpecM (Epoch × BeaconState) := do
  let mut earliest_exit_epoch := max state.earliest_exit_epoch
    (← compute_activation_exit_epoch p (← get_current_epoch p state))
  let per_epoch_churn ← get_exit_churn_limit p total_active_balance
  let mut exit_balance_to_consume :=
    if state.earliest_exit_epoch < earliest_exit_epoch then per_epoch_churn
    else state.exit_balance_to_consume

  if exit_balance > exit_balance_to_consume then
    let balance_to_process ← uint64Sub exit_balance exit_balance_to_consume
    let additional_epochs ←
      uint64Add (← uint64Div (← uint64Sub balance_to_process 1) per_epoch_churn) 1
    earliest_exit_epoch ← uint64Add earliest_exit_epoch additional_epochs
    exit_balance_to_consume ←
      uint64Add exit_balance_to_consume (← uint64Mul additional_epochs per_epoch_churn)

  let state := { state with
    exit_balance_to_consume := ← uint64Sub exit_balance_to_consume exit_balance
    earliest_exit_epoch }
  pure (state.earliest_exit_epoch, state)

/-- ```python
def initiate_validator_exit(state: BeaconState, index: ValidatorIndex) -> None:
    # Return if validator already initiated exit
    validator = state.validators[index]
    if validator.exit_epoch != FAR_FUTURE_EPOCH:
        return

    # Compute exit queue epoch [Modified in Electra:EIP7251]
    exit_queue_epoch = compute_exit_epoch_and_update_churn(state, validator.effective_balance)

    # Set validator exit epoch and withdrawable epoch
    validator.exit_epoch = exit_queue_epoch
    validator.withdrawable_epoch = validator.exit_epoch + MIN_VALIDATOR_WITHDRAWABILITY_DELAY
``` -/
def initiate_validator_exit (p : Preset) (total_active_balance : Gwei) (state : BeaconState)
    (index : ValidatorIndex) : SpecM BeaconState := do
  let validator ← listGet state.validators index
  if validator.exit_epoch != FAR_FUTURE_EPOCH then
    return state

  let (exit_queue_epoch, state) ←
    compute_exit_epoch_and_update_churn p total_active_balance state validator.effective_balance

  let validator := { validator with exit_epoch := exit_queue_epoch }
  let validator := { validator with
    withdrawable_epoch := ← uint64Add validator.exit_epoch p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY }
  pure { state with validators := ← listSet state.validators index validator }

/-- ```python
def is_eligible_for_activation_queue(validator: Validator) -> bool:
    return (
        validator.activation_eligibility_epoch == FAR_FUTURE_EPOCH
        # [Modified in Electra:EIP7251]
        and validator.effective_balance >= MIN_ACTIVATION_BALANCE
    )
``` -/
def is_eligible_for_activation_queue (p : Preset) (validator : Validator) : Bool :=
  validator.activation_eligibility_epoch == FAR_FUTURE_EPOCH
    && validator.effective_balance ≥ p.MIN_ACTIVATION_BALANCE

/-- ```python
def is_eligible_for_activation(state: BeaconState, validator: Validator) -> bool:
    return (
        # Placement in queue is finalized
        validator.activation_eligibility_epoch <= state.finalized_checkpoint.epoch
        # Has not yet been activated
        and validator.activation_epoch == FAR_FUTURE_EPOCH
    )
``` -/
def is_eligible_for_activation (state : BeaconState) (validator : Validator) : Bool :=
  validator.activation_eligibility_epoch ≤ state.finalized_checkpoint.epoch
    && validator.activation_epoch == FAR_FUTURE_EPOCH

/-- ```python
def process_registry_updates(state: BeaconState) -> None:
    current_epoch = get_current_epoch(state)
    activation_epoch = compute_activation_exit_epoch(current_epoch)

    # Process activation eligibility, ejections, and activations
    for index, validator in enumerate(state.validators):
        # [Modified in Electra:EIP7251]
        if is_eligible_for_activation_queue(validator):
            validator.activation_eligibility_epoch = current_epoch + 1
        elif (
            is_active_validator(validator, current_epoch)
            and validator.effective_balance <= EJECTION_BALANCE
        ):
            # [Modified in Electra:EIP7251]
            initiate_validator_exit(state, ValidatorIndex(index))
        elif is_eligible_for_activation(state, validator):
            validator.activation_epoch = activation_epoch
```

Each iteration only changes the validator at `index`, so reading `state.validators[index]`
gives the same validator as `enumerate`. -/
def process_registry_updates (p : Preset) (total_active_balance : Gwei) (state : BeaconState) :
    SpecM BeaconState := do
  let current_epoch ← get_current_epoch p state
  let activation_epoch ← compute_activation_exit_epoch p current_epoch

  let mut state := state
  for index in List.range state.validators.length do
    let validator ← listGet state.validators index
    if is_eligible_for_activation_queue p validator then
      let validator := { validator with
        activation_eligibility_epoch := ← uint64Add current_epoch 1 }
      state := { state with validators := ← listSet state.validators index validator }
    else if is_active_validator validator current_epoch
        && validator.effective_balance ≤ p.EJECTION_BALANCE then
      state ← initiate_validator_exit p total_active_balance state index
    else if is_eligible_for_activation state validator then
      let validator := { validator with activation_epoch }
      state := { state with validators := ← listSet state.validators index validator }
  pure state

end EpochProofs.Spec
