import EpochProofs.Spec.Types

/-!
# Reference: `process_inactivity_updates`

Transcribed line by line from `specs/phase0/beacon-chain.md` and `specs/altair/beacon-chain.md`
(v1.7.0-beta.2). Later forks do not change these functions. Each definition quotes its pyspec.

A Python `set` is a `List` here. The reference only tests membership.
-/

namespace EpochProofs.Spec

/-- ```python
def compute_epoch_at_slot(slot: Slot) -> Epoch:
    return Epoch(slot // SLOTS_PER_EPOCH)
``` -/
def compute_epoch_at_slot (p : Preset) (slot : Slot) : SpecM Epoch :=
  uint64Div slot p.SLOTS_PER_EPOCH

/-- ```python
def get_current_epoch(state: BeaconState) -> Epoch:
    return compute_epoch_at_slot(state.slot)
``` -/
def get_current_epoch (p : Preset) (state : BeaconState) : SpecM Epoch :=
  compute_epoch_at_slot p state.slot

/-- ```python
def saturating_sub(a: Uint64, b: int) -> Any:
    return a - b if a > b else a - a
``` -/
def saturating_sub (a b : Uint64) : Uint64 :=
  if a > b then a - b else a - a

/-- ```python
def get_previous_epoch(state: BeaconState) -> Epoch:
    return saturating_sub(get_current_epoch(state), 1)
``` -/
def get_previous_epoch (p : Preset) (state : BeaconState) : SpecM Epoch := do
  pure (saturating_sub (← get_current_epoch p state) 1)

/-- ```python
def is_active_validator(validator: Validator, epoch: Epoch) -> bool:
    return validator.activation_epoch <= epoch < validator.exit_epoch
``` -/
def is_active_validator (validator : Validator) (epoch : Epoch) : Bool :=
  validator.activation_epoch ≤ epoch && epoch < validator.exit_epoch

/-- ```python
def get_active_validator_indices(state: BeaconState, epoch: Epoch) -> Sequence[ValidatorIndex]:
    return [
        ValidatorIndex(i) for i, v in enumerate(state.validators) if is_active_validator(v, epoch)
    ]
``` -/
def get_active_validator_indices (state : BeaconState) (epoch : Epoch) : List ValidatorIndex :=
  (state.validators.zipIdx.filter fun (v, _) => is_active_validator v epoch).map (·.2)

/-- ```python
def get_finality_delay(state: BeaconState) -> Uint64:
    return Uint64(get_previous_epoch(state) - state.finalized_checkpoint.epoch)
``` -/
def get_finality_delay (p : Preset) (state : BeaconState) : SpecM Uint64 := do
  uint64Sub (← get_previous_epoch p state) state.finalized_checkpoint.epoch

/-- ```python
def is_in_inactivity_leak(state: BeaconState) -> bool:
    return get_finality_delay(state) > MIN_EPOCHS_TO_INACTIVITY_PENALTY
``` -/
def is_in_inactivity_leak (p : Preset) (state : BeaconState) : SpecM Bool := do
  pure (decide ((← get_finality_delay p state) > p.MIN_EPOCHS_TO_INACTIVITY_PENALTY))

/-- ```python
def get_eligible_validator_indices(state: BeaconState) -> Sequence[ValidatorIndex]:
    previous_epoch = get_previous_epoch(state)
    return [
        ValidatorIndex(index)
        for index, v in enumerate(state.validators)
        if is_active_validator(v, previous_epoch)
        or (v.slashed and previous_epoch + 1 < v.withdrawable_epoch)
    ]
```

Python `or` and `and` do not evaluate their right side when the result is already known. So
`previous_epoch + 1` is only computed for a slashed validator that is not active. -/
def get_eligible_validator_indices (p : Preset) (state : BeaconState) :
    SpecM (List ValidatorIndex) := do
  let previous_epoch ← get_previous_epoch p state
  let mut indices := []
  for (v, index) in state.validators.zipIdx do
    if is_active_validator v previous_epoch then
      indices := indices ++ [index]
    else if v.slashed then
      if (← uint64Add previous_epoch 1) < v.withdrawable_epoch then
        indices := indices ++ [index]
  pure indices

/-- ```python
def has_flag(flags: ParticipationFlags, flag_index: int) -> bool:
    flag = ParticipationFlags(2**flag_index)
    return flags & flag == flag
```

`ParticipationFlags` is a `uint8`, so `ParticipationFlags(2**flag_index)` raises when
`flag_index ≥ 8`. -/
def has_flag (flags : ParticipationFlags) (flag_index : Nat) : SpecM Bool := do
  if 2 ^ flag_index < 256 then
    let flag : ParticipationFlags := UInt8.ofNat (2 ^ flag_index)
    pure (flags &&& flag == flag)
  else
    throw .overflow

/-- ```python
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
``` -/
def get_unslashed_participating_indices (p : Preset) (state : BeaconState) (flag_index : Nat)
    (epoch : Epoch) : SpecM (List ValidatorIndex) := do
  let previous_epoch ← get_previous_epoch p state
  let current_epoch ← get_current_epoch p state
  if !(epoch == previous_epoch || epoch == current_epoch) then
    throw .assertionFailed
  let epoch_participation :=
    if epoch == current_epoch then state.current_epoch_participation
    else state.previous_epoch_participation
  let active_validator_indices := get_active_validator_indices state epoch
  let mut participating_indices := []
  for i in active_validator_indices do
    if (← has_flag (← listGet epoch_participation i) flag_index) then
      participating_indices := participating_indices ++ [i]
  let mut unslashed := []
  for index in participating_indices do
    if !(← listGet state.validators index).slashed then
      unslashed := unslashed ++ [index]
  pure unslashed

/-- ```python
def process_inactivity_updates(state: BeaconState) -> None:
    # Skip the genesis epoch as score updates are based on the previous epoch participation
    if get_current_epoch(state) == GENESIS_EPOCH:
        return

    for index in get_eligible_validator_indices(state):
        # Increase the inactivity score of inactive validators
        if index in get_unslashed_participating_indices(
            state, TIMELY_TARGET_FLAG_INDEX, get_previous_epoch(state)
        ):
            state.inactivity_scores[index] = saturating_sub(state.inactivity_scores[index], 1)
        else:
            state.inactivity_scores[index] += INACTIVITY_SCORE_BIAS
        # Decrease the inactivity score of all eligible validators during a leak-free epoch
        if not is_in_inactivity_leak(state):
            state.inactivity_scores[index] = saturating_sub(
                state.inactivity_scores[index], INACTIVITY_SCORE_RECOVERY_RATE
            )
```

Python evaluates the membership test and `is_in_inactivity_leak` again in each iteration. So
does this loop. -/
def process_inactivity_updates (p : Preset) (state : BeaconState) : SpecM BeaconState := do
  if (← get_current_epoch p state) == GENESIS_EPOCH then
    return state

  let mut inactivity_scores := state.inactivity_scores
  for index in (← get_eligible_validator_indices p state) do
    let participating ← get_unslashed_participating_indices p state TIMELY_TARGET_FLAG_INDEX
      (← get_previous_epoch p state)
    if index ∈ participating then
      let score ← listGet inactivity_scores index
      inactivity_scores ← listSet inactivity_scores index (saturating_sub score 1)
    else
      let score ← listGet inactivity_scores index
      inactivity_scores ← listSet inactivity_scores index
        (← uint64Add score p.INACTIVITY_SCORE_BIAS)
    if !(← is_in_inactivity_leak p state) then
      let score ← listGet inactivity_scores index
      inactivity_scores ← listSet inactivity_scores index
        (saturating_sub score p.INACTIVITY_SCORE_RECOVERY_RATE)
  pure { state with inactivity_scores }

end EpochProofs.Spec
