import CacheProofs.Spec.Types

/-!
# Total active balance and base rewards (phase0, altair)

Transcribed from the consensus specs. The local checkout is v1.7.0-beta.0-17-g593604b8f. These
functions are the same in v1.7.0-beta.2.

The state is reduced to the fields that these functions read: the validators (effective
balance, activation epoch, exit epoch) and the current epoch. `Config` holds the constants.

```python
def integer_squareroot(n: Uint64) -> Uint64:
    """
    Return the largest integer ``x`` such that ``x**2 <= n``.
    """
    if n == UINT64_MAX:
        return UINT64_MAX_SQRT
    x = n
    y = (x + 1) // 2
    while y < x:
        x = y
        y = (x + n // x) // 2
    return x

def is_active_validator(validator: Validator, epoch: Epoch) -> bool:
    return validator.activation_epoch <= epoch < validator.exit_epoch

def get_active_validator_indices(state: BeaconState, epoch: Epoch) -> Sequence[ValidatorIndex]:
    return [ValidatorIndex(i) for i, v in enumerate(state.validators) if is_active_validator(v, epoch)]

def get_total_balance(state: BeaconState, indices: Set[ValidatorIndex]) -> Gwei:
    return Gwei(
        max(
            EFFECTIVE_BALANCE_INCREMENT,
            sum([state.validators[index].effective_balance for index in indices]),
        )
    )

def get_total_active_balance(state: BeaconState) -> Gwei:
    return get_total_balance(
        state, set(get_active_validator_indices(state, get_current_epoch(state)))
    )

# phase0
def get_base_reward(state: BeaconState, index: ValidatorIndex) -> Gwei:
    total_balance = get_total_active_balance(state)
    effective_balance = state.validators[index].effective_balance
    return Gwei(
        effective_balance
        * BASE_REWARD_FACTOR
        // integer_squareroot(total_balance)
        // BASE_REWARDS_PER_EPOCH
    )

# altair
def get_base_reward_per_increment(state: BeaconState) -> Gwei:
    return Gwei(
        EFFECTIVE_BALANCE_INCREMENT
        * BASE_REWARD_FACTOR
        // integer_squareroot(get_total_active_balance(state))
    )

def get_base_reward(state: BeaconState, index: ValidatorIndex) -> Gwei:
    increments = state.validators[index].effective_balance // EFFECTIVE_BALANCE_INCREMENT
    return increments * get_base_reward_per_increment(state)
```

`get_active_validator_indices` has no duplicates, so the `set` keeps the list. The sum of
non-negative `Uint64` values overflows in one order only if it overflows in every order. So the
list order is fine.
-/

namespace CacheProofs.Spec.EpochCache

open CacheProofs.Spec

def UINT64_MAX : Uint64 := 2 ^ 64 - 1

def UINT64_MAX_SQRT : Uint64 := 4294967295

structure Config where
  EFFECTIVE_BALANCE_INCREMENT : Gwei
  BASE_REWARD_FACTOR : Uint64
  BASE_REWARDS_PER_EPOCH : Uint64

structure Validator where
  effective_balance : Gwei
  activation_epoch : Epoch
  exit_epoch : Epoch

structure BeaconState where
  validators : List Validator
  current_epoch : Epoch

def uint64Mul (a b : Uint64) : SpecM Uint64 :=
  if a * b < UINT64_SIZE then pure (a * b) else throw .overflow

def uint64Div (a b : Uint64) : SpecM Uint64 :=
  if b = 0 then throw .divisionByZero else pure (a / b)

def getValidator (state : BeaconState) (index : Nat) : SpecM Validator :=
  match state.validators[index]? with
  | some v => pure v
  | none => throw .indexOutOfRange

/-- The `while` loop of `integer_squareroot`, from the state `(x, y)`. -/
def integer_squareroot_loop (n x y : Uint64) : SpecM Uint64 :=
  if h : y < x then do
    let x := y
    let q ← uint64Div n x
    let s ← uint64Add x q
    integer_squareroot_loop n x (s / 2)
  else pure x
termination_by x

def integer_squareroot (n : Uint64) : SpecM Uint64 :=
  if n = UINT64_MAX then pure UINT64_MAX_SQRT
  else do
    let x := n
    let s ← uint64Add x 1
    integer_squareroot_loop n x (s / 2)

def is_active_validator (validator : Validator) (epoch : Epoch) : Bool :=
  decide (validator.activation_epoch ≤ epoch) && decide (epoch < validator.exit_epoch)

def get_active_validator_indices (state : BeaconState) (epoch : Epoch) : List Nat :=
  state.validators.zipIdx.filterMap fun (v, i) =>
    if is_active_validator v epoch then some i else none

def get_current_epoch (state : BeaconState) : Epoch := state.current_epoch

def sumUint64 (xs : List Uint64) : SpecM Uint64 := xs.foldlM uint64Add 0

def get_total_balance (cfg : Config) (state : BeaconState) (indices : List Nat) : SpecM Gwei := do
  let balances ← indices.mapM fun index => do
    let v ← getValidator state index
    pure v.effective_balance
  let total ← sumUint64 balances
  pure (max cfg.EFFECTIVE_BALANCE_INCREMENT total)

def get_total_active_balance (cfg : Config) (state : BeaconState) : SpecM Gwei :=
  get_total_balance cfg state (get_active_validator_indices state (get_current_epoch state))

/-- phase0 `get_base_reward`. -/
def get_base_reward_phase0 (cfg : Config) (state : BeaconState) (index : Nat) : SpecM Gwei := do
  let total_balance ← get_total_active_balance cfg state
  let v ← getValidator state index
  let effective_balance := v.effective_balance
  let a ← uint64Mul effective_balance cfg.BASE_REWARD_FACTOR
  let s ← integer_squareroot total_balance
  let b ← uint64Div a s
  uint64Div b cfg.BASE_REWARDS_PER_EPOCH

def get_base_reward_per_increment (cfg : Config) (state : BeaconState) : SpecM Gwei := do
  let a ← uint64Mul cfg.EFFECTIVE_BALANCE_INCREMENT cfg.BASE_REWARD_FACTOR
  let total ← get_total_active_balance cfg state
  let s ← integer_squareroot total
  uint64Div a s

/-- altair `get_base_reward`. -/
def get_base_reward_altair (cfg : Config) (state : BeaconState) (index : Nat) : SpecM Gwei := do
  let v ← getValidator state index
  let increments ← uint64Div v.effective_balance cfg.EFFECTIVE_BALANCE_INCREMENT
  let per_increment ← get_base_reward_per_increment cfg state
  uint64Mul increments per_increment

end CacheProofs.Spec.EpochCache
