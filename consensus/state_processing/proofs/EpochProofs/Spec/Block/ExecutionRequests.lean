import EpochProofs.Spec.Helpers
import EpochProofs.Spec.TotalActiveBalance
import EpochProofs.Spec.RegistryUpdates
import EpochProofs.Spec.EffectiveBalanceUpdates
import EpochProofs.Spec.Block.Types
import EpochProofs.Spec.Block.Withdrawals

/-!
# Reference: execution requests and builder registry helpers

Transcribed from `specs/phase0`, `altair`, `capella`, `electra`, `fulu` and `gloas`
`beacon-chain.md` (v1.7.0-beta.2). Each definition quotes the pyspec of its latest fork.
Constants that are not in `Preset` are top-level values with the mainnet value.
`has_execution_withdrawal_credential` and `withdrawalAddress` come from `Block/Withdrawals`.
-/

namespace EpochProofs.Spec

def GENESIS_SLOT : Slot := 0
def MIN_DEPOSIT_AMOUNT : Gwei := 1000000000
def FULL_EXIT_REQUEST_AMOUNT : Gwei := 0
def PENDING_PARTIAL_WITHDRAWALS_LIMIT : Uint64 := 134217728
def PENDING_CONSOLIDATIONS_LIMIT : Uint64 := 262144
def MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD : Uint64 := 16
def MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD : Uint64 := 2
def MAX_BUILDER_DEPOSIT_REQUESTS_PER_PAYLOAD : Uint64 := 64
def MAX_BUILDER_EXIT_REQUESTS_PER_PAYLOAD : Uint64 := 16
def CONSOLIDATION_CHURN_LIMIT_QUOTIENT : Uint64 := 65536
def MIN_BUILDER_WITHDRAWABILITY_DELAY : Epoch := 64
def BUILDER_INDEX_SELF_BUILD : BuilderIndex := UINT64_MAX
def BUILDER_WITHDRAWAL_PREFIX : UInt8 := 0xB0
def PAYLOAD_BUILDER_VERSION : UInt8 := 0

/-- `bls.G2_POINT_AT_INFINITY`: `0xc0` and then 95 zero bytes. -/
def G2_POINT_AT_INFINITY : BLSSignature :=
  (#v[(0xc0 : UInt8)] ++ Vector.replicate 95 (0 : UInt8)).cast (by decide)

/-- ```python
def set_or_append_list(
    list: List | ProgressiveList,
    index: Uint64,
    value: SSZObject,
) -> None:
    if index == len(list):
        list.append(value)
    else:
        list[index] = value
``` -/
def set_or_append_list {α : Type} (list : List α) (index : Uint64) (value : α) :
    SpecM (List α) :=
  if index == list.length then pure (list ++ [value])
  else listSet list index value

/-- ```python
def is_builder_withdrawal_credential(withdrawal_credentials: Bytes32) -> bool:
    return withdrawal_credentials[:1] == BUILDER_WITHDRAWAL_PREFIX
``` -/
def is_builder_withdrawal_credential (withdrawal_credentials : Bytes32) : Bool :=
  withdrawal_credentials.toList.take 1 == [BUILDER_WITHDRAWAL_PREFIX]

/-- ```python
def get_pending_balance_to_withdraw(state: BeaconState, validator_index: ValidatorIndex) -> Gwei:
    balance = Gwei(0)
    for withdrawal in state.pending_partial_withdrawals:
        if withdrawal.validator_index == validator_index:
            balance += withdrawal.amount
    return balance
``` -/
def get_pending_balance_to_withdraw (state : BeaconState) (validator_index : ValidatorIndex) :
    SpecM Gwei := do
  let mut balance := 0
  for withdrawal in state.pending_partial_withdrawals do
    if withdrawal.validator_index == validator_index then
      balance ← uint64Add balance withdrawal.amount
  pure balance

/-- ```python
def get_consolidation_churn_limit(state: BeaconState) -> Gwei:
    churn = get_total_active_balance(state) // CONSOLIDATION_CHURN_LIMIT_QUOTIENT
    return churn - churn % EFFECTIVE_BALANCE_INCREMENT
``` -/
def get_consolidation_churn_limit (p : Preset) (total_active_balance : Gwei) : SpecM Gwei := do
  let churn ← uint64Div total_active_balance CONSOLIDATION_CHURN_LIMIT_QUOTIENT
  uint64Sub churn (← uint64Mod churn p.EFFECTIVE_BALANCE_INCREMENT)

/-- ```python
def compute_consolidation_epoch_and_update_churn(
    state: BeaconState, consolidation_balance: Gwei
) -> Epoch:
    earliest_consolidation_epoch = max(
        state.earliest_consolidation_epoch, compute_activation_exit_epoch(get_current_epoch(state))
    )
    per_epoch_consolidation_churn = get_consolidation_churn_limit(state)
    # New epoch for consolidations.
    if state.earliest_consolidation_epoch < earliest_consolidation_epoch:
        consolidation_balance_to_consume = per_epoch_consolidation_churn
    else:
        consolidation_balance_to_consume = state.consolidation_balance_to_consume

    # Consolidation doesn't fit in the current earliest epoch.
    if consolidation_balance > consolidation_balance_to_consume:
        balance_to_process = consolidation_balance - consolidation_balance_to_consume
        additional_epochs = (balance_to_process - 1) // per_epoch_consolidation_churn + 1
        earliest_consolidation_epoch += Epoch(additional_epochs)
        consolidation_balance_to_consume += additional_epochs * per_epoch_consolidation_churn

    # Consume the balance and update state variables.
    state.consolidation_balance_to_consume = (
        consolidation_balance_to_consume - consolidation_balance
    )
    state.earliest_consolidation_epoch = earliest_consolidation_epoch

    return state.earliest_consolidation_epoch
``` -/
def compute_consolidation_epoch_and_update_churn (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) (consolidation_balance : Gwei) : SpecM (Epoch × BeaconState) := do
  let mut earliest_consolidation_epoch := max state.earliest_consolidation_epoch
    (← compute_activation_exit_epoch p (← get_current_epoch p state))
  let per_epoch_consolidation_churn ← get_consolidation_churn_limit p total_active_balance
  let mut consolidation_balance_to_consume :=
    if state.earliest_consolidation_epoch < earliest_consolidation_epoch then
      per_epoch_consolidation_churn
    else state.consolidation_balance_to_consume

  if consolidation_balance > consolidation_balance_to_consume then
    let balance_to_process ← uint64Sub consolidation_balance consolidation_balance_to_consume
    let additional_epochs ← uint64Add
      (← uint64Div (← uint64Sub balance_to_process 1) per_epoch_consolidation_churn) 1
    earliest_consolidation_epoch ← uint64Add earliest_consolidation_epoch additional_epochs
    consolidation_balance_to_consume ← uint64Add consolidation_balance_to_consume
      (← uint64Mul additional_epochs per_epoch_consolidation_churn)

  let state := { state with
    consolidation_balance_to_consume :=
      ← uint64Sub consolidation_balance_to_consume consolidation_balance
    earliest_consolidation_epoch }
  pure (state.earliest_consolidation_epoch, state)

/-- ```python
def queue_excess_active_balance(state: BeaconState, index: ValidatorIndex) -> None:
    balance = state.balances[index]
    if balance > MIN_ACTIVATION_BALANCE:
        excess_balance = balance - MIN_ACTIVATION_BALANCE
        state.balances[index] = MIN_ACTIVATION_BALANCE
        validator = state.validators[index]
        # Use G2_POINT_AT_INFINITY as a signature field placeholder
        # and GENESIS_SLOT to distinguish from a pending deposit request
        state.pending_deposits.append(
            PendingDeposit(
                pubkey=validator.pubkey,
                withdrawal_credentials=validator.withdrawal_credentials,
                amount=excess_balance,
                signature=G2_POINT_AT_INFINITY,
                slot=GENESIS_SLOT,
            )
        )
``` -/
def queue_excess_active_balance (p : Preset) (state : BeaconState) (index : ValidatorIndex) :
    SpecM BeaconState := do
  let balance ← listGet state.balances index
  if balance > p.MIN_ACTIVATION_BALANCE then
    let excess_balance ← uint64Sub balance p.MIN_ACTIVATION_BALANCE
    let state := { state with
      balances := ← listSet state.balances index p.MIN_ACTIVATION_BALANCE }
    let validator ← listGet state.validators index
    pure { state with
      pending_deposits := state.pending_deposits ++ [{
        pubkey := validator.pubkey
        withdrawal_credentials := validator.withdrawal_credentials
        amount := excess_balance
        signature := G2_POINT_AT_INFINITY
        slot := GENESIS_SLOT }] }
  else
    pure state

/-- ```python
def switch_to_compounding_validator(state: BeaconState, index: ValidatorIndex) -> None:
    validator = state.validators[index]
    validator.withdrawal_credentials = Bytes32(
        COMPOUNDING_WITHDRAWAL_PREFIX + validator.withdrawal_credentials[1:]
    )
    queue_excess_active_balance(state, index)
``` -/
def switch_to_compounding_validator (p : Preset) (state : BeaconState) (index : ValidatorIndex) :
    SpecM BeaconState := do
  let validator ← listGet state.validators index
  let validator := { validator with
    withdrawal_credentials :=
      (#v[COMPOUNDING_WITHDRAWAL_PREFIX] ++ validator.withdrawal_credentials.extract 1 32).cast
        (by decide) }
  let state := { state with validators := ← listSet state.validators index validator }
  queue_excess_active_balance p state index

/-- ```python
def process_deposit_request(state: BeaconState, deposit_request: DepositRequest) -> None:
    state.pending_deposits.append(
        PendingDeposit(
            pubkey=deposit_request.pubkey,
            withdrawal_credentials=deposit_request.withdrawal_credentials,
            amount=deposit_request.amount,
            signature=deposit_request.signature,
            slot=state.slot,
        )
    )
```

This is the Fulu version. Gloas does not change it. -/
def process_deposit_request (state : BeaconState) (deposit_request : DepositRequest) :
    SpecM BeaconState :=
  pure { state with
    pending_deposits := state.pending_deposits ++ [{
      pubkey := deposit_request.pubkey
      withdrawal_credentials := deposit_request.withdrawal_credentials
      amount := deposit_request.amount
      signature := deposit_request.signature
      slot := state.slot }] }

/-- ```python
def process_withdrawal_request(state: BeaconState, withdrawal_request: WithdrawalRequest) -> None:
    amount = withdrawal_request.amount
    is_full_exit_request = amount == FULL_EXIT_REQUEST_AMOUNT

    # If partial withdrawal queue is full, only full exits are processed
    if (
        len(state.pending_partial_withdrawals) == PENDING_PARTIAL_WITHDRAWALS_LIMIT
        and not is_full_exit_request
    ):
        return

    validator_pubkeys = [v.pubkey for v in state.validators]
    # Verify pubkey exists
    request_pubkey = withdrawal_request.validator_pubkey
    if request_pubkey not in validator_pubkeys:
        return
    index = ValidatorIndex(validator_pubkeys.index(request_pubkey))
    validator = state.validators[index]

    # Verify withdrawal credentials
    has_correct_credential = has_execution_withdrawal_credential(validator)
    is_correct_source_address = (
        validator.withdrawal_credentials[12:] == withdrawal_request.source_address
    )
    if not (has_correct_credential and is_correct_source_address):
        return
    # Verify the validator is active
    if not is_active_validator(validator, get_current_epoch(state)):
        return
    # Verify exit has not been initiated
    if validator.exit_epoch != FAR_FUTURE_EPOCH:
        return
    # Verify the validator has been active long enough
    if get_current_epoch(state) < validator.activation_epoch + SHARD_COMMITTEE_PERIOD:
        return

    pending_balance_to_withdraw = get_pending_balance_to_withdraw(state, index)

    if is_full_exit_request:
        # Only exit validator if it has no pending withdrawals in the queue
        if pending_balance_to_withdraw == 0:
            initiate_validator_exit(state, index)
        return

    has_sufficient_effective_balance = validator.effective_balance >= MIN_ACTIVATION_BALANCE
    has_excess_balance = (
        state.balances[index] > MIN_ACTIVATION_BALANCE + pending_balance_to_withdraw
    )

    # Only allow partial withdrawals with compounding withdrawal credentials
    if (
        has_compounding_withdrawal_credential(validator)
        and has_sufficient_effective_balance
        and has_excess_balance
    ):
        to_withdraw = min(
            state.balances[index] - MIN_ACTIVATION_BALANCE - pending_balance_to_withdraw, amount
        )
        exit_queue_epoch = compute_exit_epoch_and_update_churn(state, to_withdraw)
        withdrawable_epoch = exit_queue_epoch + MIN_VALIDATOR_WITHDRAWABILITY_DELAY
        state.pending_partial_withdrawals.append(
            PendingPartialWithdrawal(
                validator_index=index,
                amount=to_withdraw,
                withdrawable_epoch=withdrawable_epoch,
            )
        )
``` -/
def process_withdrawal_request (p : Preset) (state : BeaconState)
    (withdrawal_request : WithdrawalRequest) : SpecM BeaconState := do
  let amount := withdrawal_request.amount
  let is_full_exit_request := amount == FULL_EXIT_REQUEST_AMOUNT

  if state.pending_partial_withdrawals.length == PENDING_PARTIAL_WITHDRAWALS_LIMIT
      && !is_full_exit_request then
    return state

  let validator_pubkeys := state.validators.map (·.pubkey)
  let request_pubkey := withdrawal_request.validator_pubkey
  if request_pubkey ∉ validator_pubkeys then
    return state
  let index := validator_pubkeys.idxOf request_pubkey
  let validator ← listGet state.validators index

  let has_correct_credential := has_execution_withdrawal_credential validator
  let is_correct_source_address :=
    withdrawalAddress validator.withdrawal_credentials
      == withdrawal_request.source_address
  if !(has_correct_credential && is_correct_source_address) then
    return state
  if !is_active_validator validator (← get_current_epoch p state) then
    return state
  if validator.exit_epoch != FAR_FUTURE_EPOCH then
    return state
  if (← get_current_epoch p state)
      < (← uint64Add validator.activation_epoch p.SHARD_COMMITTEE_PERIOD) then
    return state

  let pending_balance_to_withdraw ← get_pending_balance_to_withdraw state index

  if is_full_exit_request then
    if pending_balance_to_withdraw == 0 then
      return ← initiate_validator_exit p (← get_total_active_balance p state) state index
    return state

  let has_sufficient_effective_balance := validator.effective_balance ≥ p.MIN_ACTIVATION_BALANCE
  let has_excess_balance := (← listGet state.balances index)
    > (← uint64Add p.MIN_ACTIVATION_BALANCE pending_balance_to_withdraw)

  if has_compounding_withdrawal_credential validator && has_sufficient_effective_balance
      && has_excess_balance then
    let to_withdraw := min (← uint64Sub (← uint64Sub (← listGet state.balances index)
      p.MIN_ACTIVATION_BALANCE) pending_balance_to_withdraw) amount
    let (exit_queue_epoch, state) ← compute_exit_epoch_and_update_churn p
      (← get_total_active_balance p state) state to_withdraw
    let withdrawable_epoch ← uint64Add exit_queue_epoch p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY
    pure { state with
      pending_partial_withdrawals := state.pending_partial_withdrawals ++ [{
        validator_index := index
        amount := to_withdraw
        withdrawable_epoch }] }
  else
    pure state

/-- ```python
def is_valid_switch_to_compounding_request(
    state: BeaconState, consolidation_request: ConsolidationRequest
) -> bool:
    # Switch to compounding requires source and target be equal
    if consolidation_request.source_pubkey != consolidation_request.target_pubkey:
        return False

    # Verify pubkey exists
    source_pubkey = consolidation_request.source_pubkey
    validator_pubkeys = [v.pubkey for v in state.validators]
    if source_pubkey not in validator_pubkeys:
        return False

    source_validator = state.validators[ValidatorIndex(validator_pubkeys.index(source_pubkey))]

    # Verify request has been authorized
    if source_validator.withdrawal_credentials[12:] != consolidation_request.source_address:
        return False

    # Verify source withdrawal credentials
    if not has_eth1_withdrawal_credential(source_validator):
        return False

    # Verify the source is active
    current_epoch = get_current_epoch(state)
    if not is_active_validator(source_validator, current_epoch):
        return False

    # Verify exit for source has not been initiated
    if source_validator.exit_epoch != FAR_FUTURE_EPOCH:
        return False

    return True
``` -/
def is_valid_switch_to_compounding_request (p : Preset) (state : BeaconState)
    (consolidation_request : ConsolidationRequest) : SpecM Bool := do
  if consolidation_request.source_pubkey != consolidation_request.target_pubkey then
    return false

  let source_pubkey := consolidation_request.source_pubkey
  let validator_pubkeys := state.validators.map (·.pubkey)
  if source_pubkey ∉ validator_pubkeys then
    return false

  let source_validator ← listGet state.validators (validator_pubkeys.idxOf source_pubkey)

  if withdrawalAddress source_validator.withdrawal_credentials
      != consolidation_request.source_address then
    return false

  if !has_eth1_withdrawal_credential source_validator then
    return false

  let current_epoch ← get_current_epoch p state
  if !is_active_validator source_validator current_epoch then
    return false

  if source_validator.exit_epoch != FAR_FUTURE_EPOCH then
    return false

  return true

/-- ```python
def process_consolidation_request(
    state: BeaconState, consolidation_request: ConsolidationRequest
) -> None:
    if is_valid_switch_to_compounding_request(state, consolidation_request):
        validator_pubkeys = [v.pubkey for v in state.validators]
        request_source_pubkey = consolidation_request.source_pubkey
        source_index = ValidatorIndex(validator_pubkeys.index(request_source_pubkey))
        switch_to_compounding_validator(state, source_index)
        return

    # Verify that source != target, so a consolidation cannot be used as an exit
    if consolidation_request.source_pubkey == consolidation_request.target_pubkey:
        return
    # If the pending consolidations queue is full, consolidation requests are ignored
    if len(state.pending_consolidations) == PENDING_CONSOLIDATIONS_LIMIT:
        return
    # If there is too little available consolidation churn limit, consolidation requests are ignored
    if get_consolidation_churn_limit(state) <= MIN_ACTIVATION_BALANCE:
        return

    validator_pubkeys = [v.pubkey for v in state.validators]
    # Verify pubkeys exists
    request_source_pubkey = consolidation_request.source_pubkey
    request_target_pubkey = consolidation_request.target_pubkey
    if request_source_pubkey not in validator_pubkeys:
        return
    if request_target_pubkey not in validator_pubkeys:
        return
    source_index = ValidatorIndex(validator_pubkeys.index(request_source_pubkey))
    target_index = ValidatorIndex(validator_pubkeys.index(request_target_pubkey))
    source_validator = state.validators[source_index]
    target_validator = state.validators[target_index]

    # Verify source withdrawal credentials
    has_correct_credential = has_execution_withdrawal_credential(source_validator)
    is_correct_source_address = (
        source_validator.withdrawal_credentials[12:] == consolidation_request.source_address
    )
    if not (has_correct_credential and is_correct_source_address):
        return

    # Verify that target has compounding withdrawal credentials
    if not has_compounding_withdrawal_credential(target_validator):
        return

    # Verify the source and the target are active
    current_epoch = get_current_epoch(state)
    if not is_active_validator(source_validator, current_epoch):
        return
    if not is_active_validator(target_validator, current_epoch):
        return
    # Verify exits for source and target have not been initiated
    if source_validator.exit_epoch != FAR_FUTURE_EPOCH:
        return
    if target_validator.exit_epoch != FAR_FUTURE_EPOCH:
        return
    # Verify the source has been active long enough
    if current_epoch < source_validator.activation_epoch + SHARD_COMMITTEE_PERIOD:
        return
    # Verify the source has no pending withdrawals in the queue
    if get_pending_balance_to_withdraw(state, source_index) > 0:
        return

    # Initiate source validator exit and append pending consolidation
    source_validator.exit_epoch = compute_consolidation_epoch_and_update_churn(
        state, source_validator.effective_balance
    )
    source_validator.withdrawable_epoch = (
        source_validator.exit_epoch + MIN_VALIDATOR_WITHDRAWABILITY_DELAY
    )
    state.pending_consolidations.append(
        PendingConsolidation(source_index=source_index, target_index=target_index)
    )
```

`source_validator` is a view into `state.validators`. The two writes go to
`state.validators[source_index]`. Each early `return` is an `if ... then pure state else`, so
the rest of the function is the `else` branch. -/
def process_consolidation_request (p : Preset) (state : BeaconState)
    (consolidation_request : ConsolidationRequest) : SpecM BeaconState := do
  if ← is_valid_switch_to_compounding_request p state consolidation_request then
    let validator_pubkeys := state.validators.map (·.pubkey)
    let request_source_pubkey := consolidation_request.source_pubkey
    let source_index := validator_pubkeys.idxOf request_source_pubkey
    switch_to_compounding_validator p state source_index
  else

  if consolidation_request.source_pubkey == consolidation_request.target_pubkey then
    pure state else
  if state.pending_consolidations.length == PENDING_CONSOLIDATIONS_LIMIT then
    pure state else
  if (← get_consolidation_churn_limit p (← get_total_active_balance p state))
      ≤ p.MIN_ACTIVATION_BALANCE then
    pure state else

  let validator_pubkeys := state.validators.map (·.pubkey)
  let request_source_pubkey := consolidation_request.source_pubkey
  let request_target_pubkey := consolidation_request.target_pubkey
  if request_source_pubkey ∉ validator_pubkeys then
    pure state else
  if request_target_pubkey ∉ validator_pubkeys then
    pure state else
  let source_index := validator_pubkeys.idxOf request_source_pubkey
  let target_index := validator_pubkeys.idxOf request_target_pubkey
  let source_validator ← listGet state.validators source_index
  let target_validator ← listGet state.validators target_index

  let has_correct_credential := has_execution_withdrawal_credential source_validator
  let is_correct_source_address :=
    withdrawalAddress source_validator.withdrawal_credentials
      == consolidation_request.source_address
  if !(has_correct_credential && is_correct_source_address) then
    pure state else

  if !has_compounding_withdrawal_credential target_validator then
    pure state else

  let current_epoch ← get_current_epoch p state
  if !is_active_validator source_validator current_epoch then
    pure state else
  if !is_active_validator target_validator current_epoch then
    pure state else
  if source_validator.exit_epoch != FAR_FUTURE_EPOCH then
    pure state else
  if target_validator.exit_epoch != FAR_FUTURE_EPOCH then
    pure state else
  if current_epoch < (← uint64Add source_validator.activation_epoch p.SHARD_COMMITTEE_PERIOD) then
    pure state else
  if (← get_pending_balance_to_withdraw state source_index) > 0 then
    pure state else

  let (exit_epoch, state) ← compute_consolidation_epoch_and_update_churn p
    (← get_total_active_balance p state) state source_validator.effective_balance
  let source_validator := { source_validator with exit_epoch }
  let source_validator := { source_validator with
    withdrawable_epoch :=
      ← uint64Add source_validator.exit_epoch p.MIN_VALIDATOR_WITHDRAWABILITY_DELAY }
  let state := { state with
    validators := ← listSet state.validators source_index source_validator }
  pure { state with
    pending_consolidations := state.pending_consolidations ++ [{ source_index, target_index }] }

/-- ```python
def is_active_builder(state: BeaconState, builder_index: BuilderIndex) -> bool:
    builder = state.builders[builder_index]
    return (
        # Placement in builder list is finalized
        builder.deposit_epoch < state.finalized_checkpoint.epoch
        # Has not initiated exit
        and builder.withdrawable_epoch == FAR_FUTURE_EPOCH
    )
``` -/
def is_active_builder (state : BeaconState) (builder_index : BuilderIndex) : SpecM Bool := do
  let builder ← listGet state.builders builder_index
  pure (builder.deposit_epoch < state.finalized_checkpoint.epoch
    && builder.withdrawable_epoch == FAR_FUTURE_EPOCH)

/-- ```python
def get_pending_balance_to_withdraw_for_builder(
    state: BeaconState, builder_index: BuilderIndex
) -> Gwei:
    balance = Gwei(0)
    for withdrawal in state.builder_pending_withdrawals:
        if withdrawal.builder_index == builder_index:
            balance += withdrawal.amount
    for payment in state.builder_pending_payments:
        if payment.withdrawal.builder_index == builder_index:
            balance += payment.withdrawal.amount
    return balance
``` -/
def get_pending_balance_to_withdraw_for_builder (state : BeaconState)
    (builder_index : BuilderIndex) : SpecM Gwei := do
  let mut balance := 0
  for withdrawal in state.builder_pending_withdrawals do
    if withdrawal.builder_index == builder_index then
      balance ← uint64Add balance withdrawal.amount
  for payment in state.builder_pending_payments do
    if payment.withdrawal.builder_index == builder_index then
      balance ← uint64Add balance payment.withdrawal.amount
  pure balance

/-- ```python
def initiate_builder_exit(state: BeaconState, builder_index: BuilderIndex) -> None:
    # Set builder exit epoch
    builder = state.builders[builder_index]
    builder.withdrawable_epoch = get_current_epoch(state) + MIN_BUILDER_WITHDRAWABILITY_DELAY
``` -/
def initiate_builder_exit (p : Preset) (state : BeaconState) (builder_index : BuilderIndex) :
    SpecM BeaconState := do
  let builder ← listGet state.builders builder_index
  let builder := { builder with
    withdrawable_epoch :=
      ← uint64Add (← get_current_epoch p state) MIN_BUILDER_WITHDRAWABILITY_DELAY }
  pure { state with builders := ← listSet state.builders builder_index builder }

/-- ```python
def is_valid_builder_deposit_signature(request: BuilderDepositRequest) -> bool:
    deposit_message = DepositMessage(
        pubkey=request.pubkey,
        withdrawal_credentials=request.withdrawal_credentials,
        amount=request.amount,
    )
    domain = compute_domain(DOMAIN_BUILDER_DEPOSIT)
    signing_root = compute_signing_root(deposit_message, domain)
    return bls.Verify(request.pubkey, signing_root, request.signature)
``` -/
def is_valid_builder_deposit_signature (p : Preset) (o : Oracle)
    (request : BuilderDepositRequest) : Bool :=
  let deposit_message : DepositMessage := {
    pubkey := request.pubkey
    withdrawal_credentials := request.withdrawal_credentials
    amount := request.amount }
  let domain := compute_domain p o DOMAIN_BUILDER_DEPOSIT
  let signing_root :=
    compute_signing_root o (o.hash_tree_root_DepositMessage deposit_message) domain
  o.bls_Verify request.pubkey signing_root request.signature

/-- ```python
def get_index_for_new_builder(state: BeaconState) -> BuilderIndex:
    for index, builder in enumerate(state.builders):
        if builder.withdrawable_epoch <= get_current_epoch(state) and builder.balance == 0:
            return BuilderIndex(index)
    return BuilderIndex(len(state.builders))
``` -/
def get_index_for_new_builder (p : Preset) (state : BeaconState) : SpecM BuilderIndex := do
  for index in List.range state.builders.length do
    let builder ← listGet state.builders index
    if builder.withdrawable_epoch ≤ (← get_current_epoch p state) && builder.balance == 0 then
      return index
  return state.builders.length

/-- ```python
def add_builder_to_registry(
    state: BeaconState,
    pubkey: BLSPubkey,
    version: Uint8,
    execution_address: ExecutionAddress,
    amount: Gwei,
    slot: Slot,
) -> None:
    set_or_append_list(
        state.builders,
        get_index_for_new_builder(state),
        Builder(
            pubkey=pubkey,
            version=version,
            execution_address=execution_address,
            balance=amount,
            deposit_epoch=compute_epoch_at_slot(slot),
            withdrawable_epoch=FAR_FUTURE_EPOCH,
        ),
    )
``` -/
def add_builder_to_registry (p : Preset) (state : BeaconState) (pubkey : BLSPubkey)
    (version : UInt8) (execution_address : ExecutionAddress) (amount : Gwei) (slot : Slot) :
    SpecM BeaconState := do
  let builders ← set_or_append_list state.builders (← get_index_for_new_builder p state) {
    pubkey
    version
    execution_address
    balance := amount
    deposit_epoch := ← compute_epoch_at_slot p slot
    withdrawable_epoch := FAR_FUTURE_EPOCH }
  pure { state with builders }

/-- ```python
def process_builder_deposit_request(state: BeaconState, request: BuilderDepositRequest) -> None:
    # Ignore deposits with unexpected withdrawal credential prefixes
    if not is_builder_withdrawal_credential(request.withdrawal_credentials):
        return

    builder_pubkeys = [b.pubkey for b in state.builders]
    if request.pubkey not in builder_pubkeys:
        if is_valid_builder_deposit_signature(request):
            add_builder_to_registry(
                state,
                request.pubkey,
                PAYLOAD_BUILDER_VERSION,
                ExecutionAddress(request.withdrawal_credentials[12:]),
                request.amount,
                state.slot,
            )
    else:
        builder_index = BuilderIndex(builder_pubkeys.index(request.pubkey))
        builder = state.builders[builder_index]

        # If exited and swept, reset the withdrawable epoch
        if builder.withdrawable_epoch != FAR_FUTURE_EPOCH and builder.balance == 0:
            epoch = get_current_epoch(state)
            builder.withdrawable_epoch = epoch + MIN_BUILDER_WITHDRAWABILITY_DELAY

        # Increase balance by deposit amount
        builder.balance += request.amount
``` -/
def process_builder_deposit_request (p : Preset) (o : Oracle) (state : BeaconState)
    (request : BuilderDepositRequest) : SpecM BeaconState := do
  if !is_builder_withdrawal_credential request.withdrawal_credentials then
    return state

  let builder_pubkeys := state.builders.map (·.pubkey)
  if request.pubkey ∉ builder_pubkeys then
    if is_valid_builder_deposit_signature p o request then
      add_builder_to_registry p state request.pubkey PAYLOAD_BUILDER_VERSION
        (withdrawalAddress request.withdrawal_credentials) request.amount state.slot
    else
      pure state
  else
    let builder_index := builder_pubkeys.idxOf request.pubkey
    let mut builder ← listGet state.builders builder_index

    if builder.withdrawable_epoch != FAR_FUTURE_EPOCH && builder.balance == 0 then
      let epoch ← get_current_epoch p state
      builder := { builder with
        withdrawable_epoch := ← uint64Add epoch MIN_BUILDER_WITHDRAWABILITY_DELAY }

    builder := { builder with balance := ← uint64Add builder.balance request.amount }
    pure { state with builders := ← listSet state.builders builder_index builder }

/-- ```python
def process_builder_exit_request(state: BeaconState, request: BuilderExitRequest) -> None:
    builder_pubkeys = [b.pubkey for b in state.builders]
    if request.pubkey not in builder_pubkeys:
        return

    builder_index = BuilderIndex(builder_pubkeys.index(request.pubkey))
    builder = state.builders[builder_index]

    if not is_active_builder(state, builder_index):
        return
    if builder.execution_address != request.source_address:
        return
    if get_pending_balance_to_withdraw_for_builder(state, builder_index) != 0:
        return

    initiate_builder_exit(state, builder_index)
``` -/
def process_builder_exit_request (p : Preset) (state : BeaconState)
    (request : BuilderExitRequest) : SpecM BeaconState := do
  let builder_pubkeys := state.builders.map (·.pubkey)
  if request.pubkey ∉ builder_pubkeys then
    return state

  let builder_index := builder_pubkeys.idxOf request.pubkey
  let builder ← listGet state.builders builder_index

  if !(← is_active_builder state builder_index) then
    return state
  if builder.execution_address != request.source_address then
    return state
  if (← get_pending_balance_to_withdraw_for_builder state builder_index) != 0 then
    return state

  initiate_builder_exit p state builder_index

end EpochProofs.Spec
