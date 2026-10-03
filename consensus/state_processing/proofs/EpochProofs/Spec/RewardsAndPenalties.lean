import EpochProofs.Spec.Slashings

/-!
# Reference: `process_rewards_and_penalties`

Transcribed line by line from `specs/phase0/beacon-chain.md`, `specs/altair/beacon-chain.md`
and `specs/bellatrix/beacon-chain.md` (v1.7.0-beta.2). Later forks do not change these
functions. Each definition quotes its pyspec.

`get_total_active_balance(state)` is a parameter. The reference does not model it yet.

Python `a[i] += e` loads `a[i]` before it evaluates `e`. The transcription keeps that order.
-/

namespace EpochProofs.Spec

/-- ```python
def integer_squareroot(n: Uint64) -> Uint64:
    if n == UINT64_MAX:
        return UINT64_MAX_SQRT
    x = n
    y = (x + 1) // 2
    while y < x:
        x = y
        y = (x + n // x) // 2
    return x
``` -/
def integer_squareroot (n : Uint64) : Uint64 :=
  if n == UINT64_MAX then UINT64_MAX_SQRT
  else loop n ((n + 1) / 2)
where
  loop (x y : Nat) : Nat :=
    if y < x then loop y ((y + n / y) / 2) else x
  termination_by x

/-- ```python
def get_base_reward_per_increment(state: BeaconState) -> Gwei:
    return Gwei(
        EFFECTIVE_BALANCE_INCREMENT
        * BASE_REWARD_FACTOR
        // integer_squareroot(get_total_active_balance(state))
    )
``` -/
def get_base_reward_per_increment (p : Preset) (total_active_balance : Gwei) : SpecM Gwei := do
  uint64Div (← uint64Mul p.EFFECTIVE_BALANCE_INCREMENT p.BASE_REWARD_FACTOR)
    (integer_squareroot total_active_balance)

/-- ```python
def get_base_reward(state: BeaconState, index: ValidatorIndex) -> Gwei:
    increments = state.validators[index].effective_balance // EFFECTIVE_BALANCE_INCREMENT
    return increments * get_base_reward_per_increment(state)
``` -/
def get_base_reward (p : Preset) (total_active_balance : Gwei) (state : BeaconState)
    (index : ValidatorIndex) : SpecM Gwei := do
  let increments ←
    uint64Div (← listGet state.validators index).effective_balance p.EFFECTIVE_BALANCE_INCREMENT
  uint64Mul increments (← get_base_reward_per_increment p total_active_balance)

/-- ```python
def get_total_balance(state: BeaconState, indices: Set[ValidatorIndex]) -> Gwei:
    return Gwei(
        max(
            EFFECTIVE_BALANCE_INCREMENT,
            sum([state.validators[index].effective_balance for index in indices]),
        )
    )
``` -/
def get_total_balance (p : Preset) (state : BeaconState) (indices : List ValidatorIndex) :
    SpecM Gwei := do
  let balances ← indices.mapM fun index => do
    pure (← listGet state.validators index).effective_balance
  pure (max p.EFFECTIVE_BALANCE_INCREMENT (← uint64Sum balances))

/-- ```python
def increase_balance(state: BeaconState, index: ValidatorIndex, delta: Gwei) -> None:
    state.balances[index] += delta
``` -/
def increase_balance (balances : List Gwei) (index : ValidatorIndex) (delta : Gwei) :
    SpecM (List Gwei) := do
  listSet balances index (← uint64Add (← listGet balances index) delta)

/-- ```python
def get_flag_index_deltas(
    state: BeaconState, flag_index: int
) -> Tuple[Sequence[Gwei], Sequence[Gwei]]:
    rewards = [Gwei(0)] * len(state.validators)
    penalties = [Gwei(0)] * len(state.validators)
    previous_epoch = get_previous_epoch(state)
    unslashed_participating_indices = get_unslashed_participating_indices(
        state, flag_index, previous_epoch
    )
    weight = PARTICIPATION_FLAG_WEIGHTS[flag_index]
    unslashed_participating_balance = get_total_balance(state, unslashed_participating_indices)
    unslashed_participating_increments = (
        unslashed_participating_balance // EFFECTIVE_BALANCE_INCREMENT
    )
    active_increments = get_total_active_balance(state) // EFFECTIVE_BALANCE_INCREMENT
    for index in get_eligible_validator_indices(state):
        base_reward = get_base_reward(state, index)
        if index in unslashed_participating_indices:
            if not is_in_inactivity_leak(state):
                reward_numerator = base_reward * weight * unslashed_participating_increments
                rewards[index] += reward_numerator // (active_increments * WEIGHT_DENOMINATOR)
        elif flag_index != TIMELY_HEAD_FLAG_INDEX:
            penalties[index] += base_reward * weight // WEIGHT_DENOMINATOR
    return rewards, penalties
``` -/
def get_flag_index_deltas (p : Preset) (total_active_balance : Gwei) (state : BeaconState)
    (flag_index : Nat) : SpecM (List Gwei × List Gwei) := do
  let mut rewards := List.replicate state.validators.length 0
  let mut penalties := List.replicate state.validators.length 0
  let previous_epoch ← get_previous_epoch p state
  let unslashed_participating_indices ←
    get_unslashed_participating_indices p state flag_index previous_epoch
  let weight ← listGet PARTICIPATION_FLAG_WEIGHTS flag_index
  let unslashed_participating_balance ← get_total_balance p state unslashed_participating_indices
  let unslashed_participating_increments ←
    uint64Div unslashed_participating_balance p.EFFECTIVE_BALANCE_INCREMENT
  let active_increments ← uint64Div total_active_balance p.EFFECTIVE_BALANCE_INCREMENT
  for index in (← get_eligible_validator_indices p state) do
    let base_reward ← get_base_reward p total_active_balance state index
    if index ∈ unslashed_participating_indices then
      if !(← is_in_inactivity_leak p state) then
        let reward_numerator ←
          uint64Mul (← uint64Mul base_reward weight) unslashed_participating_increments
        let current ← listGet rewards index
        let reward ← uint64Div reward_numerator (← uint64Mul active_increments WEIGHT_DENOMINATOR)
        rewards ← listSet rewards index (← uint64Add current reward)
    else if flag_index != TIMELY_HEAD_FLAG_INDEX then
      let current ← listGet penalties index
      let penalty ← uint64Div (← uint64Mul base_reward weight) WEIGHT_DENOMINATOR
      penalties ← listSet penalties index (← uint64Add current penalty)
  pure (rewards, penalties)

/-- ```python
def get_inactivity_penalty_deltas(state: BeaconState) -> Tuple[Sequence[Gwei], Sequence[Gwei]]:
    rewards = [Gwei(0)] * len(state.validators)
    penalties = [Gwei(0)] * len(state.validators)
    previous_epoch = get_previous_epoch(state)
    matching_target_indices = get_unslashed_participating_indices(
        state, TIMELY_TARGET_FLAG_INDEX, previous_epoch
    )
    for index in get_eligible_validator_indices(state):
        if index not in matching_target_indices:
            penalty_numerator = (
                state.validators[index].effective_balance * state.inactivity_scores[index]
            )
            # [Modified in Bellatrix]
            penalty_denominator = INACTIVITY_SCORE_BIAS * INACTIVITY_PENALTY_QUOTIENT_BELLATRIX
            penalties[index] += penalty_numerator // penalty_denominator
    return rewards, penalties
``` -/
def get_inactivity_penalty_deltas (p : Preset) (state : BeaconState) :
    SpecM (List Gwei × List Gwei) := do
  let rewards := List.replicate state.validators.length 0
  let mut penalties := List.replicate state.validators.length 0
  let previous_epoch ← get_previous_epoch p state
  let matching_target_indices ←
    get_unslashed_participating_indices p state TIMELY_TARGET_FLAG_INDEX previous_epoch
  for index in (← get_eligible_validator_indices p state) do
    if index ∉ matching_target_indices then
      let penalty_numerator ← uint64Mul (← listGet state.validators index).effective_balance
        (← listGet state.inactivity_scores index)
      let penalty_denominator ←
        uint64Mul p.INACTIVITY_SCORE_BIAS p.INACTIVITY_PENALTY_QUOTIENT_BELLATRIX
      let current ← listGet penalties index
      penalties ← listSet penalties index
        (← uint64Add current (← uint64Div penalty_numerator penalty_denominator))
  pure (rewards, penalties)

/-- ```python
def process_rewards_and_penalties(state: BeaconState) -> None:
    # No rewards are applied at the end of `GENESIS_EPOCH` because rewards are for work done in the previous epoch
    if get_current_epoch(state) == GENESIS_EPOCH:
        return

    flag_deltas = [
        get_flag_index_deltas(state, flag_index)
        for flag_index in range(len(PARTICIPATION_FLAG_WEIGHTS))
    ]
    deltas = flag_deltas + [get_inactivity_penalty_deltas(state)]
    for rewards, penalties in deltas:
        for index in range(len(state.validators)):
            increase_balance(state, ValidatorIndex(index), rewards[index])
            decrease_balance(state, ValidatorIndex(index), penalties[index])
``` -/
def process_rewards_and_penalties (p : Preset) (total_active_balance : Gwei)
    (state : BeaconState) : SpecM BeaconState := do
  if (← get_current_epoch p state) == GENESIS_EPOCH then
    return state

  let flag_deltas ← (List.range PARTICIPATION_FLAG_WEIGHTS.length).mapM fun flag_index =>
    get_flag_index_deltas p total_active_balance state flag_index
  let deltas := flag_deltas ++ [← get_inactivity_penalty_deltas p state]
  let mut balances := state.balances
  for (rewards, penalties) in deltas do
    for index in List.range state.validators.length do
      balances ← increase_balance balances index (← listGet rewards index)
      balances ← decrease_balance balances index (← listGet penalties index)
  pure { state with balances }

end EpochProofs.Spec
