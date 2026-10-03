import EpochProofs.Spec.RegistryUpdates

/-!
# Reference: `process_pending_deposits`

Transcribed line by line from `specs/phase0/beacon-chain.md`, `specs/electra/beacon-chain.md`
and `specs/gloas/beacon-chain.md` (v1.7.0-beta.2). Each definition quotes its pyspec.

`get_total_active_balance(state)` is a parameter. `apply_pending_deposit` is also a parameter:
it verifies a BLS signature and builds a new validator, which this reference does not model.
-/

namespace EpochProofs.Spec

/-- ```python
def compute_start_slot_at_epoch(epoch: Epoch) -> Slot:
    return Slot(epoch) * SLOTS_PER_EPOCH
``` -/
def compute_start_slot_at_epoch (p : Preset) (epoch : Epoch) : SpecM Slot :=
  uint64Mul epoch p.SLOTS_PER_EPOCH

/-- ```python
def get_activation_churn_limit(state: BeaconState) -> Gwei:
    churn = max(
        MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA,
        get_total_active_balance(state) // CHURN_LIMIT_QUOTIENT_GLOAS,
    )
    churn = churn - churn % EFFECTIVE_BALANCE_INCREMENT
    return min(MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT_GLOAS, churn)
``` -/
def get_activation_churn_limit (p : Preset) (total_active_balance : Gwei) : SpecM Gwei := do
  let churn := max p.MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA
    (← uint64Div total_active_balance p.CHURN_LIMIT_QUOTIENT_GLOAS)
  let churn ← uint64Sub churn (← uint64Mod churn p.EFFECTIVE_BALANCE_INCREMENT)
  pure (min p.MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT_GLOAS churn)

/-- ```python
def process_pending_deposits(state: BeaconState) -> None:
    next_epoch = get_current_epoch(state) + 1
    # [Modified in Gloas:EIP8061]
    # Deposits still consume the activation-only churn budget in Gloas.
    available_for_processing = state.deposit_balance_to_consume + get_activation_churn_limit(state)
    processed_amount = 0
    next_deposit_index = 0
    deposits_to_postpone = []
    is_churn_limit_reached = False
    finalized_slot = compute_start_slot_at_epoch(state.finalized_checkpoint.epoch)

    for deposit in state.pending_deposits:
        # Check if deposit has been finalized, otherwise, stop processing.
        if deposit.slot > finalized_slot:
            break

        # Check if number of processed deposits has not reached the limit, otherwise, stop processing.
        if next_deposit_index >= MAX_PENDING_DEPOSITS_PER_EPOCH:
            break

        # Read validator state
        is_validator_exited = False
        is_validator_withdrawn = False
        validator_pubkeys = [v.pubkey for v in state.validators]
        if deposit.pubkey in validator_pubkeys:
            validator = state.validators[ValidatorIndex(validator_pubkeys.index(deposit.pubkey))]
            is_validator_exited = validator.exit_epoch < FAR_FUTURE_EPOCH
            is_validator_withdrawn = validator.withdrawable_epoch < next_epoch

        if is_validator_withdrawn:
            # Deposited balance will never become active. Increase balance but do not consume churn
            apply_pending_deposit(state, deposit)
        elif is_validator_exited:
            # Validator is exiting, postpone the deposit until after withdrawable epoch
            deposits_to_postpone.append(deposit)
        else:
            # Check if deposit fits in the churn, otherwise, do no more deposit processing in this epoch.
            is_churn_limit_reached = processed_amount + deposit.amount > available_for_processing
            if is_churn_limit_reached:
                break

            # Consume churn and apply deposit.
            processed_amount += deposit.amount
            apply_pending_deposit(state, deposit)

        # Regardless of how the deposit was handled, we move on in the queue.
        next_deposit_index += 1

    state.pending_deposits = state.pending_deposits[next_deposit_index:] + deposits_to_postpone

    # Accumulate churn only if the churn limit has been hit.
    if is_churn_limit_reached:
        state.deposit_balance_to_consume = available_for_processing - processed_amount
    else:
        state.deposit_balance_to_consume = Gwei(0)
``` -/
def process_pending_deposits (p : Preset) (total_active_balance : Gwei)
    (apply_pending_deposit : BeaconState → PendingDeposit → SpecM BeaconState)
    (state : BeaconState) : SpecM BeaconState := do
  let next_epoch ← uint64Add (← get_current_epoch p state) 1
  let available_for_processing ←
    uint64Add state.deposit_balance_to_consume (← get_activation_churn_limit p total_active_balance)
  let mut processed_amount := 0
  let mut next_deposit_index := 0
  let mut deposits_to_postpone := []
  let mut is_churn_limit_reached := false
  let finalized_slot ← compute_start_slot_at_epoch p state.finalized_checkpoint.epoch

  let mut state := state
  for deposit in state.pending_deposits do
    if deposit.slot > finalized_slot then
      break

    if next_deposit_index ≥ p.MAX_PENDING_DEPOSITS_PER_EPOCH then
      break

    let mut is_validator_exited := false
    let mut is_validator_withdrawn := false
    let validator_pubkeys := state.validators.map (·.pubkey)
    if deposit.pubkey ∈ validator_pubkeys then
      let validator ← listGet state.validators (validator_pubkeys.idxOf deposit.pubkey)
      is_validator_exited := validator.exit_epoch < FAR_FUTURE_EPOCH
      is_validator_withdrawn := validator.withdrawable_epoch < next_epoch

    if is_validator_withdrawn then
      state ← apply_pending_deposit state deposit
    else if is_validator_exited then
      deposits_to_postpone := deposits_to_postpone ++ [deposit]
    else
      is_churn_limit_reached :=
        decide ((← uint64Add processed_amount deposit.amount) > available_for_processing)
      if is_churn_limit_reached then
        break
      processed_amount ← uint64Add processed_amount deposit.amount
      state ← apply_pending_deposit state deposit

    next_deposit_index := next_deposit_index + 1

  state := { state with
    pending_deposits := state.pending_deposits.drop next_deposit_index ++ deposits_to_postpone }

  if is_churn_limit_reached then
    state := { state with
      deposit_balance_to_consume := ← uint64Sub available_for_processing processed_amount }
  else
    state := { state with deposit_balance_to_consume := 0 }
  pure state

end EpochProofs.Spec
