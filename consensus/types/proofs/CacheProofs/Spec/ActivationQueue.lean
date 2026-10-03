import CacheProofs.Spec.Types

/-!
# Activation queue (phase0 to Deneb, Electra)

Transcribed from the consensus specs. The local checkout is v1.7.0-beta.0-17-g593604b8f. These
functions are the same in v1.7.0-beta.2.

`Validator` holds only the fields that these functions read.

`process_registry_updates` also calls `initiate_validator_exit` for ejections. That function
writes only `exit_epoch` and `withdrawable_epoch`, and no function here reads them, except
`is_active_validator`. So the reference models the activation part and returns the ejection
decision as a flag.
-/

namespace CacheProofs.Spec.ActivationQueue

open CacheProofs.Spec

structure Validator where
  effective_balance : Gwei
  activation_eligibility_epoch : Epoch
  activation_epoch : Epoch
  exit_epoch : Epoch
  deriving DecidableEq, Repr

/-!
```python
def is_active_validator(validator: Validator, epoch: Epoch) -> bool:
    return validator.activation_epoch <= epoch < validator.exit_epoch
```
-/
def is_active_validator (validator : Validator) (epoch : Epoch) : Bool :=
  decide (validator.activation_epoch ≤ epoch) && decide (epoch < validator.exit_epoch)

/-!
phase0:

```python
def is_eligible_for_activation_queue(validator: Validator) -> bool:
    return (
        validator.activation_eligibility_epoch == FAR_FUTURE_EPOCH
        and validator.effective_balance == MAX_EFFECTIVE_BALANCE
    )
```
-/
def is_eligible_for_activation_queue (MAX_EFFECTIVE_BALANCE : Gwei) (validator : Validator) :
    Bool :=
  decide (validator.activation_eligibility_epoch = FAR_FUTURE_EPOCH)
    && decide (validator.effective_balance = MAX_EFFECTIVE_BALANCE)

/-!
Electra:

```python
def is_eligible_for_activation_queue(validator: Validator) -> bool:
    return (
        validator.activation_eligibility_epoch == FAR_FUTURE_EPOCH
        # [Modified in Electra:EIP7251]
        and validator.effective_balance >= MIN_ACTIVATION_BALANCE
    )
```
-/
def is_eligible_for_activation_queue_electra (MIN_ACTIVATION_BALANCE : Gwei)
    (validator : Validator) : Bool :=
  decide (validator.activation_eligibility_epoch = FAR_FUTURE_EPOCH)
    && decide (validator.effective_balance ≥ MIN_ACTIVATION_BALANCE)

/-!
```python
def is_eligible_for_activation(state: BeaconState, validator: Validator) -> bool:
    return (
        # Placement in queue is finalized
        validator.activation_eligibility_epoch <= state.finalized_checkpoint.epoch
        # Has not yet been activated
        and validator.activation_epoch == FAR_FUTURE_EPOCH
    )
```

`finalized_epoch` is `state.finalized_checkpoint.epoch`.
-/
def is_eligible_for_activation (finalized_epoch : Epoch) (validator : Validator) : Bool :=
  decide (validator.activation_eligibility_epoch ≤ finalized_epoch)
    && decide (validator.activation_epoch = FAR_FUTURE_EPOCH)

/-!
## phase0 to Deneb `process_registry_updates`

```python
def process_registry_updates(state: BeaconState) -> None:
    # Process activation eligibility and ejections
    for index, validator in enumerate(state.validators):
        if is_eligible_for_activation_queue(validator):
            validator.activation_eligibility_epoch = get_current_epoch(state) + 1

        if (
            is_active_validator(validator, get_current_epoch(state))
            and validator.effective_balance <= EJECTION_BALANCE
        ):
            initiate_validator_exit(state, ValidatorIndex(index))

    # Queue validators eligible for activation and not yet dequeued for activation
    activation_queue = sorted(
        [
            index
            for index, validator in enumerate(state.validators)
            if is_eligible_for_activation(state, validator)
        ],
        # Order by the sequence of activation_eligibility_epoch setting and then index
        key=lambda index: (state.validators[index].activation_eligibility_epoch, index),
    )
    # Dequeued validators for activation up to activation churn limit
    # [Modified in Deneb:EIP7514]
    for index in activation_queue[: get_validator_activation_churn_limit(state)]:
        validator = state.validators[index]
        validator.activation_epoch = compute_activation_exit_epoch(get_current_epoch(state))
```

phase0 to Capella use `get_validator_churn_limit(state)` in place of
`get_validator_activation_churn_limit(state)`. `churn_limit` is a parameter.
-/

/-- Python tuple order `<=` on `(Epoch, ValidatorIndex)`. -/
def tupleLe (a b : Nat × Nat) : Bool :=
  decide (a.1 < b.1) || (decide (a.1 = b.1) && decide (a.2 ≤ b.2))

/-- The first loop: the activation eligibility part. -/
def process_activation_eligibility (MAX_EFFECTIVE_BALANCE : Gwei) (current_epoch : Epoch)
    (validators : List Validator) : SpecM (List Validator) :=
  validators.mapM fun validator =>
    if is_eligible_for_activation_queue MAX_EFFECTIVE_BALANCE validator then do
      let epoch ← uint64Add current_epoch 1
      pure { validator with activation_eligibility_epoch := epoch }
    else pure validator

/-- `activation_queue`, on the validators after the first loop. -/
def activation_queue (finalized_epoch : Epoch) (validators : List Validator) : List Nat :=
  let eligible := validators.zipIdx.filter fun (validator, _) =>
    is_eligible_for_activation finalized_epoch validator
  (eligible.mergeSort fun a b =>
    tupleLe (a.1.activation_eligibility_epoch, a.2) (b.1.activation_eligibility_epoch, b.2)).map
    (·.2)

/-- The indices that `process_registry_updates` activates. -/
def dequeued (finalized_epoch : Epoch) (churn_limit : Nat) (validators : List Validator) :
    List Nat :=
  (activation_queue finalized_epoch validators).take churn_limit

/-!
## Electra `process_registry_updates`

```python
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

One loop iteration. It returns the new validator and `true` if it calls
`initiate_validator_exit`. `activation_epoch` is a parameter.
-/
def process_registry_update_electra (MIN_ACTIVATION_BALANCE EJECTION_BALANCE : Gwei)
    (current_epoch activation_epoch finalized_epoch : Epoch) (validator : Validator) :
    SpecM (Validator × Bool) :=
  if is_eligible_for_activation_queue_electra MIN_ACTIVATION_BALANCE validator then do
    let epoch ← uint64Add current_epoch 1
    pure ({ validator with activation_eligibility_epoch := epoch }, false)
  else if is_active_validator validator current_epoch
      && decide (validator.effective_balance ≤ EJECTION_BALANCE) then
    pure (validator, true)
  else if is_eligible_for_activation finalized_epoch validator then
    pure ({ validator with activation_epoch := activation_epoch }, false)
  else pure (validator, false)

/-!
## Finalization in `weigh_justification_and_finalization`

```python
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

Only the epochs of the checkpoints are modelled. `bits` is the new `justification_bits`.
-/

/-- One `if` of the finalization rules. -/
def finalization_rule (all_bits : Bool) (source_epoch distance current_epoch : Epoch)
    (finalized_epoch : Epoch) : SpecM Epoch :=
  if all_bits then do
    let epoch ← uint64Add source_epoch distance
    pure (if epoch = current_epoch then source_epoch else finalized_epoch)
  else pure finalized_epoch

/-- The new `state.finalized_checkpoint.epoch`. -/
def process_finalizations (bits : List Bool)
    (old_previous_justified_epoch old_current_justified_epoch current_epoch : Epoch)
    (finalized_epoch : Epoch) : SpecM Epoch := do
  let f ← finalization_rule (((bits.drop 1).take 3).all id) old_previous_justified_epoch 3
    current_epoch finalized_epoch
  let f ← finalization_rule (((bits.drop 1).take 2).all id) old_previous_justified_epoch 2
    current_epoch f
  let f ← finalization_rule ((bits.take 3).all id) old_current_justified_epoch 2 current_epoch f
  finalization_rule ((bits.take 2).all id) old_current_justified_epoch 1 current_epoch f

end CacheProofs.Spec.ActivationQueue
