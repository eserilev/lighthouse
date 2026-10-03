import CacheProofs.Spec.Types

/-!
# Exit queue (phase0)

```python
def initiate_validator_exit(state: BeaconState, index: ValidatorIndex) -> None:
    # Return if validator already initiated exit
    validator = state.validators[index]
    if validator.exit_epoch != FAR_FUTURE_EPOCH:
        return

    # Compute exit queue epoch
    exit_epochs = [v.exit_epoch for v in state.validators if v.exit_epoch != FAR_FUTURE_EPOCH]
    exit_queue_epoch = max(exit_epochs + [compute_activation_exit_epoch(get_current_epoch(state))])
    exit_queue_churn = len([v for v in state.validators if v.exit_epoch == exit_queue_epoch])
    if exit_queue_churn >= get_validator_churn_limit(state):
        exit_queue_epoch += Epoch(1)

    # Set validator exit epoch and withdrawable epoch
    validator.exit_epoch = exit_queue_epoch
    validator.withdrawable_epoch = Epoch(validator.exit_epoch + config.MIN_VALIDATOR_WITHDRAWABILITY_DELAY)
```

`exitQueueEpoch` is the exit queue epoch computation. `exitEpochs` is the `exit_epoch` of
each validator, in order. `delayedEpoch` is `compute_activation_exit_epoch(get_current_epoch(state))`
and `churnLimit` is `get_validator_churn_limit(state)`.
-/

namespace CacheProofs.Spec

def exitQueueEpoch (exitEpochs : List Epoch) (delayedEpoch churnLimit : Nat) : SpecM Epoch :=
  let exit_epochs := exitEpochs.filter (· ≠ FAR_FUTURE_EPOCH)
  let exit_queue_epoch := (exit_epochs ++ [delayedEpoch]).foldl max 0
  let exit_queue_churn := (exitEpochs.filter (· = exit_queue_epoch)).length
  if exit_queue_churn ≥ churnLimit then uint64Add exit_queue_epoch 1
  else pure exit_queue_epoch

end CacheProofs.Spec
