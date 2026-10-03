import CacheProofs.Spec.Types

/-!
# Unslashed participating balances (Altair and later)

Transcribed from `specs/phase0/beacon-chain.md` and `specs/altair/beacon-chain.md`. The local
checkout is `v1.7.0-beta.0-17-g593604b8f`. These functions are the same in v1.7.0-beta.2.

`BeaconState` and `Validator` keep only the fields that these functions read.
`current_epoch` is `get_current_epoch(state)`. `EFFECTIVE_BALANCE_INCREMENT` is a parameter.

```python
def is_active_validator(validator: Validator, epoch: Epoch) -> bool:
    return validator.activation_epoch <= epoch < validator.exit_epoch

def get_previous_epoch(state: BeaconState) -> Epoch:
    current_epoch = get_current_epoch(state)
    return GENESIS_EPOCH if current_epoch == GENESIS_EPOCH else current_epoch - 1

def get_active_validator_indices(state: BeaconState, epoch: Epoch) -> Sequence[ValidatorIndex]:
    return [
        ValidatorIndex(i) for i, v in enumerate(state.validators) if is_active_validator(v, epoch)
    ]

def get_total_balance(state: BeaconState, indices: Set[ValidatorIndex]) -> Gwei:
    return Gwei(
        max(
            EFFECTIVE_BALANCE_INCREMENT,
            sum([state.validators[index].effective_balance for index in indices]),
        )
    )

def add_flag(flags: ParticipationFlags, flag_index: int) -> ParticipationFlags:
    flag = ParticipationFlags(2**flag_index)
    return flags | flag

def has_flag(flags: ParticipationFlags, flag_index: int) -> bool:
    flag = ParticipationFlags(2**flag_index)
    return flags & flag == flag

def get_unslashed_participating_indices(
    state: BeaconState, flag_index: int, epoch: Epoch
) -> Set[ValidatorIndex]:
    assert epoch in (get_previous_epoch(state), get_current_epoch(state))
    if epoch == get_current_epoch(state):
        epoch_participation = state.current_epoch_participation
    else:
        epoch_participation = state.previous_epoch_participation
    active_validator_indices = get_active_validator_indices(state, epoch)
    participating_indices = [
        i for i in active_validator_indices if has_flag(epoch_participation[i], flag_index)
    ]
    return set(filter(lambda index: not state.validators[index].slashed, participating_indices))

def set_participation_flag (state : BeaconState) (target_epoch : Epoch) (index : ValidatorIndex)
    (flag_index : Nat) : SpecM BeaconState := do
  if target_epoch = get_current_epoch state then
    let epoch_participation := state.current_epoch_participation
    let flags ← getAt epoch_participation index
    if !has_flag flags flag_index then
      pure { state with
        current_epoch_participation := epoch_participation.set index (add_flag flags flag_index) }
    else pure state
  else
    let epoch_participation := state.previous_epoch_participation
    let flags ← getAt epoch_participation index
    if !has_flag flags flag_index then
      pure { state with
        previous_epoch_participation := epoch_participation.set index (add_flag flags flag_index) }
    else pure state

def set_slashed (state : BeaconState) (slashed_index : ValidatorIndex) : SpecM BeaconState := do
  let validator ← getAt state.validators slashed_index
  pure { state with
    validators := state.validators.set slashed_index { validator with slashed := true } }

def set_effective_balance (state : BeaconState) (index : ValidatorIndex)
    (effective_balance : Gwei) : SpecM BeaconState := do
  let validator ← getAt state.validators index
  pure { state with
    validators := state.validators.set index { validator with effective_balance } }

def process_participation_flag_updates(state: BeaconState) -> None:
    state.previous_epoch_participation = state.current_epoch_participation
    state.current_epoch_participation = EpochParticipation(
        data=[ParticipationFlags(0b0000_0000) for _ in range(len(state.validators))]
    )
```

## State changes

Lighthouse updates the cache when one of these lines runs.

`process_attestation`, for one attester `index` and one flag:

```python
    if data.target.epoch == get_current_epoch(state):
        epoch_participation = state.current_epoch_participation
    else:
        epoch_participation = state.previous_epoch_participation
    ...
            if flag_index in participation_flag_indices and not has_flag(
                epoch_participation[index], flag_index
            ):
                epoch_participation[index] = add_flag(epoch_participation[index], flag_index)
```

`slash_validator`:

```python
    validator = state.validators[slashed_index]
    validator.slashed = True
```

`process_effective_balance_updates`, for one validator:

```python
            validator.effective_balance = min(
                balance - balance % EFFECTIVE_BALANCE_INCREMENT, max_effective_balance
            )
```

`process_participation_flag_updates` (above), then `state.slot += 1` in `process_slots`, which
moves `get_current_epoch(state)` to the next epoch.

The indices come from `enumerate`, so they are distinct. The `set` is a list here.
`get_total_balance` sums Gwei values, which are not negative. So no partial sum is larger than
the full sum, and the `Gwei(...)` cast raises exactly when the sum does not fit in a `Uint64`.
-/

namespace CacheProofs.Spec

abbrev ParticipationFlags := Nat
abbrev ValidatorIndex := Nat

def GENESIS_EPOCH : Epoch := 0

def TIMELY_SOURCE_FLAG_INDEX : Nat := 0
def TIMELY_TARGET_FLAG_INDEX : Nat := 1
def TIMELY_HEAD_FLAG_INDEX : Nat := 2

structure Validator where
  effective_balance : Gwei
  slashed : Bool
  activation_epoch : Epoch
  exit_epoch : Epoch

structure BeaconState where
  current_epoch : Epoch
  validators : List Validator
  previous_epoch_participation : List ParticipationFlags
  current_epoch_participation : List ParticipationFlags

/-- `sequence[index]`. Python raises `IndexError` out of range. -/
def getAt {α : Type} (l : List α) (i : Nat) : SpecM α :=
  match l[i]? with
  | some a => pure a
  | none => throw .indexOutOfRange

def get_current_epoch (state : BeaconState) : Epoch := state.current_epoch

def is_active_validator (validator : Validator) (epoch : Epoch) : Bool :=
  validator.activation_epoch ≤ epoch && epoch < validator.exit_epoch

def get_previous_epoch (state : BeaconState) : Epoch :=
  let current_epoch := get_current_epoch state
  if current_epoch = GENESIS_EPOCH then GENESIS_EPOCH else current_epoch - 1

def get_active_validator_indices (state : BeaconState) (epoch : Epoch) : List ValidatorIndex :=
  state.validators.zipIdx.filterMap fun (v, i) =>
    if is_active_validator v epoch then some i else none

def get_total_balance (EFFECTIVE_BALANCE_INCREMENT : Gwei) (state : BeaconState)
    (indices : List ValidatorIndex) : SpecM Gwei := do
  let balances ← indices.mapM fun index => do
    let v ← getAt state.validators index
    pure v.effective_balance
  let total := max EFFECTIVE_BALANCE_INCREMENT balances.sum
  if total < UINT64_SIZE then pure total else throw .overflow

def add_flag (flags : ParticipationFlags) (flag_index : Nat) : ParticipationFlags :=
  let flag := 2 ^ flag_index
  flags ||| flag

def has_flag (flags : ParticipationFlags) (flag_index : Nat) : Bool :=
  let flag := 2 ^ flag_index
  flags &&& flag == flag

def get_unslashed_participating_indices (state : BeaconState) (flag_index : Nat)
    (epoch : Epoch) : SpecM (List ValidatorIndex) := do
  if !(epoch = get_previous_epoch state || epoch = get_current_epoch state) then
    throw .assertionFailed
  let epoch_participation :=
    if epoch = get_current_epoch state then state.current_epoch_participation
    else state.previous_epoch_participation
  let active_validator_indices := get_active_validator_indices state epoch
  let participating_indices ← active_validator_indices.filterMapM fun i => do
    let flags ← getAt epoch_participation i
    pure (if has_flag flags flag_index then some i else none)
  participating_indices.filterMapM fun index => do
    let v ← getAt state.validators index
    pure (if !v.slashed then some index else none)

def set_participation_flag (state : BeaconState) (target_epoch : Epoch) (index : ValidatorIndex)
    (flag_index : Nat) : SpecM BeaconState := do
  if target_epoch = get_current_epoch state then
    let epoch_participation := state.current_epoch_participation
    let flags ← getAt epoch_participation index
    if !has_flag flags flag_index then
      pure { state with
        current_epoch_participation := epoch_participation.set index (add_flag flags flag_index) }
    else pure state
  else
    let epoch_participation := state.previous_epoch_participation
    let flags ← getAt epoch_participation index
    if !has_flag flags flag_index then
      pure { state with
        previous_epoch_participation := epoch_participation.set index (add_flag flags flag_index) }
    else pure state

def set_slashed (state : BeaconState) (slashed_index : ValidatorIndex) : SpecM BeaconState := do
  let validator ← getAt state.validators slashed_index
  pure { state with
    validators := state.validators.set slashed_index { validator with slashed := true } }

def set_effective_balance (state : BeaconState) (index : ValidatorIndex)
    (effective_balance : Gwei) : SpecM BeaconState := do
  let validator ← getAt state.validators index
  pure { state with
    validators := state.validators.set index { validator with effective_balance } }

def process_participation_flag_updates (state : BeaconState) : BeaconState :=
  { state with
    previous_epoch_participation := state.current_epoch_participation
    current_epoch_participation := List.replicate state.validators.length 0 }

/-- `process_participation_flag_updates`, then the slot moves into the next epoch. -/
def next_epoch_participation (state : BeaconState) : BeaconState :=
  { process_participation_flag_updates state with current_epoch := state.current_epoch + 1 }

end CacheProofs.Spec
