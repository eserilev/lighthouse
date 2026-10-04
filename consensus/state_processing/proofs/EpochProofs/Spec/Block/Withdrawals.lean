import EpochProofs.Spec.Helpers
import EpochProofs.Spec.Slashings
import EpochProofs.Spec.EffectiveBalanceUpdates

/-!
# Reference: `process_withdrawals`

Transcribed from `specs/capella`, `specs/electra` and `specs/gloas/beacon-chain.md`
(v1.7.0-beta.2). Each function is the latest fork version and quotes its pyspec.
A `for` loop with `break` is a recursive helper on the list. A `for _ in range(n)` loop is a
recursive helper on a fuel of `n`.
-/

namespace EpochProofs.Spec

/-- ```python
def has_eth1_withdrawal_credential(validator: Validator) -> bool:
    return validator.withdrawal_credentials[:1] == ETH1_ADDRESS_WITHDRAWAL_PREFIX
``` -/
def has_eth1_withdrawal_credential (validator : Validator) : Bool :=
  validator.withdrawal_credentials.toList.take 1 == [ETH1_ADDRESS_WITHDRAWAL_PREFIX]

/-- ```python
def has_execution_withdrawal_credential(validator: Validator) -> bool:
    return (
        has_eth1_withdrawal_credential(validator)  # 0x01
        or has_compounding_withdrawal_credential(validator)  # 0x02
    )
``` -/
def has_execution_withdrawal_credential (validator : Validator) : Bool :=
  has_eth1_withdrawal_credential validator || has_compounding_withdrawal_credential validator

/-- ```python
def is_fully_withdrawable_validator(validator: Validator, balance: Gwei, epoch: Epoch) -> bool:
    return (
        has_execution_withdrawal_credential(validator)
        and validator.withdrawable_epoch <= epoch
        and balance > 0
    )
``` -/
def is_fully_withdrawable_validator (validator : Validator) (balance : Gwei) (epoch : Epoch) :
    Bool :=
  has_execution_withdrawal_credential validator && validator.withdrawable_epoch ≤ epoch &&
    balance > 0

/-- ```python
def is_partially_withdrawable_validator(validator: Validator, balance: Gwei) -> bool:
    max_effective_balance = get_max_effective_balance(validator)
    has_max_effective_balance = validator.effective_balance == max_effective_balance
    has_excess_balance = balance > max_effective_balance
    return (
        has_execution_withdrawal_credential(validator)
        and has_max_effective_balance
        and has_excess_balance
    )
``` -/
def is_partially_withdrawable_validator (p : Preset) (validator : Validator) (balance : Gwei) :
    Bool :=
  let max_effective_balance := get_max_effective_balance p validator
  let has_max_effective_balance := validator.effective_balance == max_effective_balance
  let has_excess_balance := decide (balance > max_effective_balance)
  has_execution_withdrawal_credential validator && has_max_effective_balance && has_excess_balance

/-- ```python
def is_eligible_for_partial_withdrawals(validator: Validator, balance: Gwei) -> bool:
    has_sufficient_effective_balance = validator.effective_balance >= MIN_ACTIVATION_BALANCE
    has_excess_balance = balance > MIN_ACTIVATION_BALANCE
    return (
        validator.exit_epoch == FAR_FUTURE_EPOCH
        and has_sufficient_effective_balance
        and has_excess_balance
    )
``` -/
def is_eligible_for_partial_withdrawals (p : Preset) (validator : Validator) (balance : Gwei) :
    Bool :=
  let has_sufficient_effective_balance :=
    decide (validator.effective_balance ≥ p.MIN_ACTIVATION_BALANCE)
  let has_excess_balance := decide (balance > p.MIN_ACTIVATION_BALANCE)
  validator.exit_epoch == FAR_FUTURE_EPOCH && has_sufficient_effective_balance &&
    has_excess_balance

/-- ```python
def is_builder_index(validator_index: ValidatorIndex) -> bool:
    return (validator_index & BUILDER_INDEX_FLAG) != 0
``` -/
def is_builder_index (validator_index : ValidatorIndex) : Bool :=
  validator_index &&& BUILDER_INDEX_FLAG != 0

/-- ```python
def convert_builder_index_to_validator_index(builder_index: BuilderIndex) -> ValidatorIndex:
    return ValidatorIndex(builder_index | BUILDER_INDEX_FLAG)
``` -/
def convert_builder_index_to_validator_index (builder_index : BuilderIndex) : ValidatorIndex :=
  builder_index ||| BUILDER_INDEX_FLAG

/-- ```python
def convert_validator_index_to_builder_index(validator_index: ValidatorIndex) -> BuilderIndex:
    return BuilderIndex(validator_index & ~BUILDER_INDEX_FLAG)
```

`~BUILDER_INDEX_FLAG` on a `uint64` is `UINT64_MAX ^ BUILDER_INDEX_FLAG`. -/
def convert_validator_index_to_builder_index (validator_index : ValidatorIndex) : BuilderIndex :=
  validator_index &&& (UINT64_MAX ^^^ BUILDER_INDEX_FLAG)

/-- `ExecutionAddress(withdrawal_credentials[12:])` in Python. -/
def withdrawalAddress (withdrawal_credentials : Bytes32) : ExecutionAddress :=
  (withdrawal_credentials.extract 12 32).cast (by decide)

/-- ```python
@dataclass
class ExpectedWithdrawals:
    withdrawals: Sequence[Withdrawal]
    processed_builder_withdrawals_count: Uint64
    processed_partial_withdrawals_count: Uint64
    processed_builders_sweep_count: Uint64
    processed_sweep_withdrawals_count: Uint64
``` -/
structure ExpectedWithdrawals where
  withdrawals : List Withdrawal
  processed_builder_withdrawals_count : Uint64
  processed_partial_withdrawals_count : Uint64
  processed_builders_sweep_count : Uint64
  processed_sweep_withdrawals_count : Uint64

/-- ```python
def get_balance_after_withdrawals(
    state: BeaconState,
    validator_index: ValidatorIndex,
    withdrawals: Sequence[Withdrawal],
) -> Gwei:
    withdrawn = sum(
        withdrawal.amount
        for withdrawal in withdrawals
        if withdrawal.validator_index == validator_index
    )
    return state.balances[validator_index] - withdrawn
``` -/
def get_balance_after_withdrawals (state : BeaconState) (validator_index : ValidatorIndex)
    (withdrawals : List Withdrawal) : SpecM Gwei := do
  let withdrawn ← uint64Sum
    ((withdrawals.filter fun withdrawal => withdrawal.validator_index == validator_index).map
      (·.amount))
  uint64Sub (← listGet state.balances validator_index) withdrawn

/-- The `for` loop of `get_builder_withdrawals`. It takes the rest of
`state.builder_pending_withdrawals` and the loop variables. -/
def get_builder_withdrawals_loop (prior_withdrawals : List Withdrawal)
    (withdrawals_limit : Uint64) :
    List BuilderPendingWithdrawal → WithdrawalIndex → Uint64 → List Withdrawal →
      SpecM (List Withdrawal × WithdrawalIndex × Uint64)
  | [], withdrawal_index, processed_count, withdrawals =>
    pure (withdrawals, withdrawal_index, processed_count)
  | withdrawal :: rest, withdrawal_index, processed_count, withdrawals => do
    let all_withdrawals := prior_withdrawals ++ withdrawals
    let has_reached_limit := decide (all_withdrawals.length ≥ withdrawals_limit)
    if has_reached_limit then
      return (withdrawals, withdrawal_index, processed_count)

    let builder_index := withdrawal.builder_index
    let withdrawals := withdrawals ++ [{
      index := withdrawal_index
      validator_index := convert_builder_index_to_validator_index builder_index
      address := withdrawal.fee_recipient
      amount := withdrawal.amount }]
    let withdrawal_index ← uint64Add withdrawal_index 1
    let processed_count ← uint64Add processed_count 1
    get_builder_withdrawals_loop prior_withdrawals withdrawals_limit rest withdrawal_index
      processed_count withdrawals

/-- ```python
def get_builder_withdrawals(
    state: BeaconState,
    withdrawal_index: WithdrawalIndex,
    prior_withdrawals: Sequence[Withdrawal],
) -> Tuple[Sequence[Withdrawal], WithdrawalIndex, Uint64]:
    withdrawals_limit = MAX_WITHDRAWALS_PER_PAYLOAD - 1
    assert len(prior_withdrawals) <= withdrawals_limit

    processed_count = Uint64(0)
    withdrawals: list[Withdrawal] = []
    for withdrawal in state.builder_pending_withdrawals:
        all_withdrawals = list(prior_withdrawals) + withdrawals
        has_reached_limit = len(all_withdrawals) >= withdrawals_limit
        if has_reached_limit:
            break

        builder_index = withdrawal.builder_index
        withdrawals.append(
            Withdrawal(
                index=withdrawal_index,
                validator_index=convert_builder_index_to_validator_index(builder_index),
                address=withdrawal.fee_recipient,
                amount=withdrawal.amount,
            )
        )
        withdrawal_index += 1
        processed_count += 1

    return withdrawals, withdrawal_index, processed_count
``` -/
def get_builder_withdrawals (p : Preset) (state : BeaconState)
    (withdrawal_index : WithdrawalIndex) (prior_withdrawals : List Withdrawal) :
    SpecM (List Withdrawal × WithdrawalIndex × Uint64) := do
  let withdrawals_limit ← uint64Sub p.MAX_WITHDRAWALS_PER_PAYLOAD 1
  if ¬ prior_withdrawals.length ≤ withdrawals_limit then throw .assertionFailed

  let processed_count : Uint64 := 0
  let withdrawals : List Withdrawal := []
  get_builder_withdrawals_loop prior_withdrawals withdrawals_limit
    state.builder_pending_withdrawals withdrawal_index processed_count withdrawals

/-- The `for` loop of `get_pending_partial_withdrawals`. It takes the rest of
`state.pending_partial_withdrawals` and the loop variables. -/
def get_pending_partial_withdrawals_loop (p : Preset) (state : BeaconState) (epoch : Epoch)
    (prior_withdrawals : List Withdrawal) (withdrawals_limit : Uint64) :
    List PendingPartialWithdrawal → WithdrawalIndex → Uint64 → List Withdrawal →
      SpecM (List Withdrawal × WithdrawalIndex × Uint64)
  | [], withdrawal_index, processed_count, withdrawals =>
    pure (withdrawals, withdrawal_index, processed_count)
  | withdrawal :: rest, withdrawal_index, processed_count, withdrawals => do
    let all_withdrawals := prior_withdrawals ++ withdrawals
    let is_withdrawable := decide (withdrawal.withdrawable_epoch ≤ epoch)
    let has_reached_limit := decide (all_withdrawals.length ≥ withdrawals_limit)
    if !is_withdrawable || has_reached_limit then
      return (withdrawals, withdrawal_index, processed_count)

    let validator_index := withdrawal.validator_index
    let validator ← listGet state.validators validator_index
    let balance ← get_balance_after_withdrawals state validator_index all_withdrawals
    let mut withdrawals := withdrawals
    let mut withdrawal_index := withdrawal_index
    if is_eligible_for_partial_withdrawals p validator balance then
      let withdrawal_amount :=
        min (← uint64Sub balance p.MIN_ACTIVATION_BALANCE) withdrawal.amount
      withdrawals := withdrawals ++ [{
        index := withdrawal_index
        validator_index := validator_index
        address := withdrawalAddress validator.withdrawal_credentials
        amount := withdrawal_amount }]
      withdrawal_index ← uint64Add withdrawal_index 1

    let processed_count ← uint64Add processed_count 1
    get_pending_partial_withdrawals_loop p state epoch prior_withdrawals withdrawals_limit rest
      withdrawal_index processed_count withdrawals

/-- ```python
def get_pending_partial_withdrawals(
    state: BeaconState,
    withdrawal_index: WithdrawalIndex,
    prior_withdrawals: Sequence[Withdrawal],
) -> Tuple[Sequence[Withdrawal], WithdrawalIndex, Uint64]:
    epoch = get_current_epoch(state)
    withdrawals_limit = min(
        len(prior_withdrawals) + MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP,
        MAX_WITHDRAWALS_PER_PAYLOAD - 1,
    )
    assert len(prior_withdrawals) <= withdrawals_limit

    processed_count = Uint64(0)
    withdrawals: list[Withdrawal] = []
    for withdrawal in state.pending_partial_withdrawals:
        all_withdrawals = list(prior_withdrawals) + withdrawals
        is_withdrawable = withdrawal.withdrawable_epoch <= epoch
        has_reached_limit = len(all_withdrawals) >= withdrawals_limit
        if not is_withdrawable or has_reached_limit:
            break

        validator_index = withdrawal.validator_index
        validator = state.validators[validator_index]
        balance = get_balance_after_withdrawals(state, validator_index, all_withdrawals)
        if is_eligible_for_partial_withdrawals(validator, balance):
            withdrawal_amount = min(balance - MIN_ACTIVATION_BALANCE, withdrawal.amount)
            withdrawals.append(
                Withdrawal(
                    index=withdrawal_index,
                    validator_index=validator_index,
                    address=ExecutionAddress(validator.withdrawal_credentials[12:]),
                    amount=withdrawal_amount,
                )
            )
            withdrawal_index += 1

        processed_count += 1

    return withdrawals, withdrawal_index, processed_count
``` -/
def get_pending_partial_withdrawals (p : Preset) (state : BeaconState)
    (withdrawal_index : WithdrawalIndex) (prior_withdrawals : List Withdrawal) :
    SpecM (List Withdrawal × WithdrawalIndex × Uint64) := do
  let epoch ← get_current_epoch p state
  let withdrawals_limit := min
    (← uint64Add prior_withdrawals.length p.MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP)
    (← uint64Sub p.MAX_WITHDRAWALS_PER_PAYLOAD 1)
  if ¬ prior_withdrawals.length ≤ withdrawals_limit then throw .assertionFailed

  let processed_count : Uint64 := 0
  let withdrawals : List Withdrawal := []
  get_pending_partial_withdrawals_loop p state epoch prior_withdrawals withdrawals_limit
    state.pending_partial_withdrawals withdrawal_index processed_count withdrawals

/-- The `for _ in range(builders_limit)` loop of `get_builders_sweep_withdrawals`. The fuel is
the number of iterations left. -/
def get_builders_sweep_withdrawals_loop (state : BeaconState) (epoch : Epoch)
    (prior_withdrawals : List Withdrawal) (withdrawals_limit : Uint64) :
    Nat → BuilderIndex → WithdrawalIndex → Uint64 → List Withdrawal →
      SpecM (List Withdrawal × WithdrawalIndex × Uint64)
  | 0, _, withdrawal_index, processed_count, withdrawals =>
    pure (withdrawals, withdrawal_index, processed_count)
  | fuel + 1, builder_index, withdrawal_index, processed_count, withdrawals => do
    let all_withdrawals := prior_withdrawals ++ withdrawals
    let has_reached_limit := decide (all_withdrawals.length ≥ withdrawals_limit)
    if has_reached_limit then
      return (withdrawals, withdrawal_index, processed_count)

    let builder ← listGet state.builders builder_index
    let mut withdrawals := withdrawals
    let mut withdrawal_index := withdrawal_index
    if builder.withdrawable_epoch ≤ epoch && builder.balance > 0 then
      withdrawals := withdrawals ++ [{
        index := withdrawal_index
        validator_index := convert_builder_index_to_validator_index builder_index
        address := builder.execution_address
        amount := builder.balance }]
      withdrawal_index ← uint64Add withdrawal_index 1

    let builder_index ← uint64Mod (← uint64Add builder_index 1) state.builders.length
    let processed_count ← uint64Add processed_count 1
    get_builders_sweep_withdrawals_loop state epoch prior_withdrawals withdrawals_limit fuel
      builder_index withdrawal_index processed_count withdrawals

/-- ```python
def get_builders_sweep_withdrawals(
    state: BeaconState,
    withdrawal_index: WithdrawalIndex,
    prior_withdrawals: Sequence[Withdrawal],
) -> Tuple[Sequence[Withdrawal], WithdrawalIndex, Uint64]:
    epoch = get_current_epoch(state)
    builders_limit = min(len(state.builders), MAX_BUILDERS_PER_WITHDRAWALS_SWEEP)
    withdrawals_limit = MAX_WITHDRAWALS_PER_PAYLOAD - 1
    assert len(prior_withdrawals) <= withdrawals_limit

    processed_count = Uint64(0)
    withdrawals: list[Withdrawal] = []
    builder_index = state.next_withdrawal_builder_index
    for _ in range(builders_limit):
        all_withdrawals = list(prior_withdrawals) + withdrawals
        has_reached_limit = len(all_withdrawals) >= withdrawals_limit
        if has_reached_limit:
            break

        builder = state.builders[builder_index]
        if builder.withdrawable_epoch <= epoch and builder.balance > 0:
            withdrawals.append(
                Withdrawal(
                    index=withdrawal_index,
                    validator_index=convert_builder_index_to_validator_index(builder_index),
                    address=builder.execution_address,
                    amount=builder.balance,
                )
            )
            withdrawal_index += 1

        builder_index = (builder_index + 1) % len(state.builders)
        processed_count += 1

    return withdrawals, withdrawal_index, processed_count
``` -/
def get_builders_sweep_withdrawals (p : Preset) (state : BeaconState)
    (withdrawal_index : WithdrawalIndex) (prior_withdrawals : List Withdrawal) :
    SpecM (List Withdrawal × WithdrawalIndex × Uint64) := do
  let epoch ← get_current_epoch p state
  let builders_limit := min state.builders.length p.MAX_BUILDERS_PER_WITHDRAWALS_SWEEP
  let withdrawals_limit ← uint64Sub p.MAX_WITHDRAWALS_PER_PAYLOAD 1
  if ¬ prior_withdrawals.length ≤ withdrawals_limit then throw .assertionFailed

  let processed_count : Uint64 := 0
  let withdrawals : List Withdrawal := []
  let builder_index := state.next_withdrawal_builder_index
  get_builders_sweep_withdrawals_loop state epoch prior_withdrawals withdrawals_limit
    builders_limit builder_index withdrawal_index processed_count withdrawals

/-- The `for _ in range(validators_limit)` loop of `get_validators_sweep_withdrawals`. The fuel
is the number of iterations left. -/
def get_validators_sweep_withdrawals_loop (p : Preset) (state : BeaconState) (epoch : Epoch)
    (prior_withdrawals : List Withdrawal) (withdrawals_limit : Uint64) :
    Nat → ValidatorIndex → WithdrawalIndex → Uint64 → List Withdrawal →
      SpecM (List Withdrawal × WithdrawalIndex × Uint64)
  | 0, _, withdrawal_index, processed_count, withdrawals =>
    pure (withdrawals, withdrawal_index, processed_count)
  | fuel + 1, validator_index, withdrawal_index, processed_count, withdrawals => do
    let all_withdrawals := prior_withdrawals ++ withdrawals
    let has_reached_limit := decide (all_withdrawals.length ≥ withdrawals_limit)
    if has_reached_limit then
      return (withdrawals, withdrawal_index, processed_count)

    let validator ← listGet state.validators validator_index
    let balance ← get_balance_after_withdrawals state validator_index all_withdrawals
    let mut withdrawals := withdrawals
    let mut withdrawal_index := withdrawal_index
    if is_fully_withdrawable_validator validator balance epoch then
      withdrawals := withdrawals ++ [{
        index := withdrawal_index
        validator_index := validator_index
        address := withdrawalAddress validator.withdrawal_credentials
        amount := balance }]
      withdrawal_index ← uint64Add withdrawal_index 1
    else if is_partially_withdrawable_validator p validator balance then
      withdrawals := withdrawals ++ [{
        index := withdrawal_index
        validator_index := validator_index
        address := withdrawalAddress validator.withdrawal_credentials
        amount := ← uint64Sub balance (get_max_effective_balance p validator) }]
      withdrawal_index ← uint64Add withdrawal_index 1

    let validator_index ← uint64Mod (← uint64Add validator_index 1) state.validators.length
    let processed_count ← uint64Add processed_count 1
    get_validators_sweep_withdrawals_loop p state epoch prior_withdrawals withdrawals_limit fuel
      validator_index withdrawal_index processed_count withdrawals

/-- ```python
def get_validators_sweep_withdrawals(
    state: BeaconState,
    withdrawal_index: WithdrawalIndex,
    prior_withdrawals: Sequence[Withdrawal],
) -> Tuple[Sequence[Withdrawal], WithdrawalIndex, Uint64]:
    epoch = get_current_epoch(state)
    validators_limit = min(len(state.validators), MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP)
    withdrawals_limit = MAX_WITHDRAWALS_PER_PAYLOAD
    # There must be at least one space reserved for validator sweep withdrawals
    assert len(prior_withdrawals) < withdrawals_limit

    processed_count = Uint64(0)
    withdrawals: list[Withdrawal] = []
    validator_index = state.next_withdrawal_validator_index
    for _ in range(validators_limit):
        all_withdrawals = list(prior_withdrawals) + withdrawals
        has_reached_limit = len(all_withdrawals) >= withdrawals_limit
        if has_reached_limit:
            break

        validator = state.validators[validator_index]
        balance = get_balance_after_withdrawals(state, validator_index, all_withdrawals)
        if is_fully_withdrawable_validator(validator, balance, epoch):
            withdrawals.append(
                Withdrawal(
                    index=withdrawal_index,
                    validator_index=validator_index,
                    address=ExecutionAddress(validator.withdrawal_credentials[12:]),
                    amount=balance,
                )
            )
            withdrawal_index += 1
        elif is_partially_withdrawable_validator(validator, balance):
            withdrawals.append(
                Withdrawal(
                    index=withdrawal_index,
                    validator_index=validator_index,
                    address=ExecutionAddress(validator.withdrawal_credentials[12:]),
                    # [Modified in Electra:EIP7251]
                    amount=balance - get_max_effective_balance(validator),
                )
            )
            withdrawal_index += 1

        validator_index = (validator_index + 1) % len(state.validators)
        processed_count += 1

    return withdrawals, withdrawal_index, processed_count
``` -/
def get_validators_sweep_withdrawals (p : Preset) (state : BeaconState)
    (withdrawal_index : WithdrawalIndex) (prior_withdrawals : List Withdrawal) :
    SpecM (List Withdrawal × WithdrawalIndex × Uint64) := do
  let epoch ← get_current_epoch p state
  let validators_limit := min state.validators.length p.MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP
  let withdrawals_limit := p.MAX_WITHDRAWALS_PER_PAYLOAD
  if ¬ prior_withdrawals.length < withdrawals_limit then throw .assertionFailed

  let processed_count : Uint64 := 0
  let withdrawals : List Withdrawal := []
  let validator_index := state.next_withdrawal_validator_index
  get_validators_sweep_withdrawals_loop p state epoch prior_withdrawals withdrawals_limit
    validators_limit validator_index withdrawal_index processed_count withdrawals

/-- ```python
def get_expected_withdrawals(state: BeaconState) -> ExpectedWithdrawals:
    withdrawal_index = state.next_withdrawal_index
    withdrawals: list[Withdrawal] = []

    # [New in Gloas:EIP7732]
    # Get builder withdrawals
    builder_withdrawals, withdrawal_index, processed_builder_withdrawals_count = (
        get_builder_withdrawals(state, withdrawal_index, withdrawals)
    )
    withdrawals.extend(builder_withdrawals)

    # Get partial withdrawals
    partial_withdrawals, withdrawal_index, processed_partial_withdrawals_count = (
        get_pending_partial_withdrawals(state, withdrawal_index, withdrawals)
    )
    withdrawals.extend(partial_withdrawals)

    # [New in Gloas:EIP7732]
    # Get builders sweep withdrawals
    builders_sweep_withdrawals, withdrawal_index, processed_builders_sweep_count = (
        get_builders_sweep_withdrawals(state, withdrawal_index, withdrawals)
    )
    withdrawals.extend(builders_sweep_withdrawals)

    # Get validators sweep withdrawals
    validators_sweep_withdrawals, withdrawal_index, processed_validators_sweep_count = (
        get_validators_sweep_withdrawals(state, withdrawal_index, withdrawals)
    )
    withdrawals.extend(validators_sweep_withdrawals)

    return ExpectedWithdrawals(
        withdrawals,
        # [New in Gloas:EIP7732]
        processed_builder_withdrawals_count,
        processed_partial_withdrawals_count,
        # [New in Gloas:EIP7732]
        processed_builders_sweep_count,
        processed_validators_sweep_count,
    )
``` -/
def get_expected_withdrawals (p : Preset) (state : BeaconState) :
    SpecM ExpectedWithdrawals := do
  let withdrawal_index := state.next_withdrawal_index
  let withdrawals : List Withdrawal := []

  let (builder_withdrawals, withdrawal_index, processed_builder_withdrawals_count) ←
    get_builder_withdrawals p state withdrawal_index withdrawals
  let withdrawals := withdrawals ++ builder_withdrawals

  let (partial_withdrawals, withdrawal_index, processed_partial_withdrawals_count) ←
    get_pending_partial_withdrawals p state withdrawal_index withdrawals
  let withdrawals := withdrawals ++ partial_withdrawals

  let (builders_sweep_withdrawals, withdrawal_index, processed_builders_sweep_count) ←
    get_builders_sweep_withdrawals p state withdrawal_index withdrawals
  let withdrawals := withdrawals ++ builders_sweep_withdrawals

  let (validators_sweep_withdrawals, _, processed_validators_sweep_count) ←
    get_validators_sweep_withdrawals p state withdrawal_index withdrawals
  let withdrawals := withdrawals ++ validators_sweep_withdrawals

  pure {
    withdrawals
    processed_builder_withdrawals_count
    processed_partial_withdrawals_count
    processed_builders_sweep_count
    processed_sweep_withdrawals_count := processed_validators_sweep_count }

/-- ```python
def apply_withdrawals(state: BeaconState, withdrawals: Sequence[Withdrawal]) -> None:
    for withdrawal in withdrawals:
        # [Modified in Gloas:EIP7732]
        if is_builder_index(withdrawal.validator_index):
            builder_index = convert_validator_index_to_builder_index(withdrawal.validator_index)
            state.builders[builder_index].balance = saturating_sub(
                state.builders[builder_index].balance, withdrawal.amount
            )
        else:
            decrease_balance(state, withdrawal.validator_index, withdrawal.amount)
``` -/
def apply_withdrawals (state : BeaconState) : List Withdrawal → SpecM BeaconState
  | [] => pure state
  | withdrawal :: rest => do
    let state ←
      if is_builder_index withdrawal.validator_index then do
        let builder_index := convert_validator_index_to_builder_index withdrawal.validator_index
        let builder ← listGet state.builders builder_index
        let builders ← listSet state.builders builder_index
          { builder with balance := saturating_sub builder.balance withdrawal.amount }
        pure { state with builders }
      else do
        let balances ←
          decrease_balance state.balances withdrawal.validator_index withdrawal.amount
        pure { state with balances }
    apply_withdrawals state rest

/-- ```python
def update_next_withdrawal_index(state: BeaconState, withdrawals: Sequence[Withdrawal]) -> None:
    # Update the next withdrawal index if this block contained withdrawals
    if len(withdrawals) != 0:
        latest_withdrawal = withdrawals[-1]
        state.next_withdrawal_index = latest_withdrawal.index + 1
``` -/
def update_next_withdrawal_index (state : BeaconState) (withdrawals : List Withdrawal) :
    SpecM BeaconState := do
  if withdrawals.length ≠ 0 then
    let latest_withdrawal ← listGet withdrawals (withdrawals.length - 1)
    return { state with next_withdrawal_index := ← uint64Add latest_withdrawal.index 1 }
  pure state

/-- ```python
def update_payload_expected_withdrawals(
    state: BeaconState, withdrawals: Sequence[Withdrawal]
) -> None:
    state.payload_expected_withdrawals = Withdrawals(data=withdrawals)
``` -/
def update_payload_expected_withdrawals (state : BeaconState) (withdrawals : List Withdrawal) :
    BeaconState :=
  { state with payload_expected_withdrawals := withdrawals }

/-- ```python
def update_builder_pending_withdrawals(
    state: BeaconState, processed_builder_withdrawals_count: Uint64
) -> None:
    state.builder_pending_withdrawals = state.builder_pending_withdrawals[
        processed_builder_withdrawals_count:
    ]
``` -/
def update_builder_pending_withdrawals (state : BeaconState)
    (processed_builder_withdrawals_count : Uint64) : BeaconState :=
  { state with
    builder_pending_withdrawals :=
      state.builder_pending_withdrawals.drop processed_builder_withdrawals_count }

/-- ```python
def update_pending_partial_withdrawals(
    state: BeaconState, processed_partial_withdrawals_count: Uint64
) -> None:
    state.pending_partial_withdrawals = state.pending_partial_withdrawals[
        processed_partial_withdrawals_count:
    ]
``` -/
def update_pending_partial_withdrawals (state : BeaconState)
    (processed_partial_withdrawals_count : Uint64) : BeaconState :=
  { state with
    pending_partial_withdrawals :=
      state.pending_partial_withdrawals.drop processed_partial_withdrawals_count }

/-- ```python
def update_next_withdrawal_builder_index(
    state: BeaconState, processed_builders_sweep_count: Uint64
) -> None:
    if len(state.builders) > 0:
        # Update the next builder index to start the next withdrawal sweep
        next_index = state.next_withdrawal_builder_index + processed_builders_sweep_count
        next_builder_index = next_index % len(state.builders)
        state.next_withdrawal_builder_index = next_builder_index
``` -/
def update_next_withdrawal_builder_index (state : BeaconState)
    (processed_builders_sweep_count : Uint64) : SpecM BeaconState := do
  if state.builders.length > 0 then
    let next_index ← uint64Add state.next_withdrawal_builder_index processed_builders_sweep_count
    let next_builder_index ← uint64Mod next_index state.builders.length
    return { state with next_withdrawal_builder_index := next_builder_index }
  pure state

/-- ```python
def update_next_withdrawal_validator_index(
    state: BeaconState, withdrawals: Sequence[Withdrawal]
) -> None:
    # Update the next validator index to start the next withdrawal sweep
    if len(withdrawals) == MAX_WITHDRAWALS_PER_PAYLOAD:
        # Next sweep starts after the latest withdrawal's validator index
        next_validator_index = (withdrawals[-1].validator_index + 1) % len(state.validators)
        state.next_withdrawal_validator_index = next_validator_index
    else:
        # Advance sweep by the max length of the sweep if there was not a full set of withdrawals
        next_index = state.next_withdrawal_validator_index + MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP
        next_validator_index = next_index % len(state.validators)
        state.next_withdrawal_validator_index = next_validator_index
```

`withdrawals[-1]` raises on an empty list, and so does `listGet [] 0`. -/
def update_next_withdrawal_validator_index (p : Preset) (state : BeaconState)
    (withdrawals : List Withdrawal) : SpecM BeaconState := do
  if withdrawals.length = p.MAX_WITHDRAWALS_PER_PAYLOAD then
    let latest_withdrawal ← listGet withdrawals (withdrawals.length - 1)
    let next_validator_index ←
      uint64Mod (← uint64Add latest_withdrawal.validator_index 1) state.validators.length
    pure { state with next_withdrawal_validator_index := next_validator_index }
  else
    let next_index ←
      uint64Add state.next_withdrawal_validator_index p.MAX_VALIDATORS_PER_WITHDRAWALS_SWEEP
    let next_validator_index ← uint64Mod next_index state.validators.length
    pure { state with next_withdrawal_validator_index := next_validator_index }

/-- ```python
def process_withdrawals(
    state: BeaconState,
    # [Modified in Gloas:EIP7732]
    # Removed `payload`
) -> None:
    # [New in Gloas:EIP7732]
    # Return early if the parent block is empty
    if state.latest_block_hash != state.latest_execution_payload_bid.block_hash:
        return

    # Get expected withdrawals
    expected = get_expected_withdrawals(state)

    # Apply expected withdrawals
    apply_withdrawals(state, expected.withdrawals)

    # Update withdrawals fields in the state
    update_next_withdrawal_index(state, expected.withdrawals)
    # [New in Gloas:EIP7732]
    update_payload_expected_withdrawals(state, expected.withdrawals)
    # [New in Gloas:EIP7732]
    update_builder_pending_withdrawals(state, expected.processed_builder_withdrawals_count)
    update_pending_partial_withdrawals(state, expected.processed_partial_withdrawals_count)
    # [New in Gloas:EIP7732]
    update_next_withdrawal_builder_index(state, expected.processed_builders_sweep_count)
    update_next_withdrawal_validator_index(state, expected.withdrawals)
``` -/
def process_withdrawals (p : Preset) (state : BeaconState) :
    SpecM BeaconState := do
  if state.latest_block_hash ≠ state.latest_execution_payload_bid.block_hash then
    return state

  let expected ← get_expected_withdrawals p state

  let state ← apply_withdrawals state expected.withdrawals

  let state ← update_next_withdrawal_index state expected.withdrawals
  let state := update_payload_expected_withdrawals state expected.withdrawals
  let state := update_builder_pending_withdrawals state expected.processed_builder_withdrawals_count
  let state := update_pending_partial_withdrawals state expected.processed_partial_withdrawals_count
  let state ← update_next_withdrawal_builder_index state expected.processed_builders_sweep_count
  update_next_withdrawal_validator_index p state expected.withdrawals

end EpochProofs.Spec
