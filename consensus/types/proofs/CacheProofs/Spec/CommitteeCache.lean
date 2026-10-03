import CacheProofs.Spec.Types

/-!
# Committees and the shuffle (phase0)

Transcribed from `specs/phase0/beacon-chain.md` and `specs/phase0/validator.md`
(v1.7.0-beta.2). Names and statement order match the spec.

The reference does not read the state. The caller gives the values that the spec reads from it:

- `indices` is `get_active_validator_indices(state, epoch)`.
- `seed` is `get_seed(state, epoch, DOMAIN_BEACON_ATTESTER)`.
- `sha256` is a parameter. The proofs hold for every hash function.

A byte is a `Nat` below 256. A byte string is a `List Nat`.

`compute_shuffled_permutation` keeps a dict `source_by_bucket`. The dict is a memo of a pure
function of `position_bucket`. The reference calls `sha256` each time. The result is the same.
-/

namespace CacheProofs.Spec.CommitteeCache

open CacheProofs.Spec

abbrev ValidatorIndex := Nat
abbrev Slot := Nat
abbrev CommitteeIndex := Nat

/-- Preset values that the reference reads. -/
structure Preset where
  SLOTS_PER_EPOCH : Nat
  MAX_COMMITTEES_PER_SLOT : Nat
  TARGET_COMMITTEE_SIZE : Nat
  SHUFFLE_ROUND_COUNT : Nat

def Preset.mainnet : Preset :=
  { SLOTS_PER_EPOCH := 32, MAX_COMMITTEES_PER_SLOT := 64, TARGET_COMMITTEE_SIZE := 128,
    SHUFFLE_ROUND_COUNT := 90 }

def Preset.minimal : Preset :=
  { SLOTS_PER_EPOCH := 8, MAX_COMMITTEES_PER_SLOT := 4, TARGET_COMMITTEE_SIZE := 4,
    SHUFFLE_ROUND_COUNT := 10 }

def uint64Mul (a b : Uint64) : SpecM Uint64 :=
  if a * b < UINT64_SIZE then pure (a * b) else throw .overflow

def uint64Div (a b : Uint64) : SpecM Uint64 :=
  if b = 0 then throw .divisionByZero else pure (a / b)

def uint64Mod (a b : Uint64) : SpecM Uint64 :=
  if b = 0 then throw .divisionByZero else pure (a % b)

/-- `l[i]` in Python. -/
def listGet {α : Type} (l : List α) (i : Nat) : SpecM α :=
  match l[i]? with
  | some a => pure a
  | none => throw .indexOutOfRange

/-- `uint_to_bytes(Uint8(x))`. `Uint8(x)` raises if `x` does not fit. -/
def uint8ToBytes (x : Nat) : SpecM (List Nat) :=
  if x < 256 then pure [x] else throw .overflow

/-- `uint_to_bytes(Uint32(x))`, little-endian. `Uint32(x)` raises if `x` does not fit. -/
def uint32ToBytes (x : Nat) : SpecM (List Nat) :=
  if x < 4294967296 then pure [x % 256, x / 256 % 256, x / 65536 % 256, x / 16777216 % 256]
  else throw .overflow

/-- `bytes_to_uint64(data)`, little-endian. -/
def bytes_to_uint64 (data : List Nat) : Uint64 :=
  data.foldr (fun b acc => b + 256 * acc) 0

/-!
```python
def compute_shuffled_permutation(index_count: Uint64, seed: Bytes32) -> Sequence[Uint64]:
    """
    Return the full shuffled permutation corresponding to ``seed`` (and ``index_count``).
    """
    # Swap or not (https://link.springer.com/content/pdf/10.1007%2F978-3-642-32009-5_1.pdf)
    # See the 'generalized domain' algorithm on page 3
    indices = [Uint64(i) for i in range(index_count)]
    for current_round in range(SHUFFLE_ROUND_COUNT):
        round_bytes = uint_to_bytes(Uint8(current_round))
        pivot = bytes_to_uint64(sha256(seed + round_bytes)[0:8]) % index_count
        source_by_bucket: Dict[Uint64, Bytes32] = {}
        for i in range(index_count):
            flip = (pivot + index_count - indices[i]) % index_count
            position = max(indices[i], flip)
            position_bucket = position // 256
            if position_bucket not in source_by_bucket:
                source_by_bucket[position_bucket] = sha256(
                    seed + round_bytes + uint_to_bytes(Uint32(position_bucket))
                )
            source = source_by_bucket[position_bucket]
            byte_val = source[(position % 256) // 8]
            bit = (byte_val >> (position % 8)) % 2
            indices[i] = flip if bit else indices[i]
    return indices
```
-/
def compute_shuffled_permutation (p : Preset) (sha256 : List Nat → List Nat)
    (index_count : Uint64) (seed : List Nat) : SpecM (List Uint64) := do
  let mut indices := List.range index_count
  for current_round in List.range p.SHUFFLE_ROUND_COUNT do
    let round_bytes ← uint8ToBytes current_round
    let pivot ← uint64Mod (bytes_to_uint64 ((sha256 (seed ++ round_bytes)).take 8)) index_count
    for i in List.range index_count do
      let index ← listGet indices i
      let flip ← uint64Mod (← uint64Sub (← uint64Add pivot index_count) index) index_count
      let position := max index flip
      let position_bucket := position / 256
      let source := sha256 (seed ++ round_bytes ++ (← uint32ToBytes position_bucket))
      let byte_val ← listGet source (position % 256 / 8)
      let bit := byte_val / 2 ^ (position % 8) % 2
      indices := indices.set i (if bit ≠ 0 then flip else index)
  return indices

/-!
```python
def compute_shuffled_index(index: Uint64, index_count: Uint64, seed: Bytes32) -> Uint64:
    """
    Return the shuffled index corresponding to ``seed`` (and ``index_count``).
    """
    assert index < index_count
    return compute_shuffled_permutation(index_count, seed)[index]
```
-/
def compute_shuffled_index (p : Preset) (sha256 : List Nat → List Nat)
    (index index_count : Uint64) (seed : List Nat) : SpecM Uint64 := do
  if ¬ index < index_count then throw .assertionFailed
  listGet (← compute_shuffled_permutation p sha256 index_count seed) index

/-!
```python
def compute_committee(
    indices: Sequence[ValidatorIndex], seed: Bytes32, index: Uint64, count: Uint64
) -> Sequence[ValidatorIndex]:
    """
    Return the committee corresponding to ``indices``, ``seed``, ``index``, and committee ``count``.
    """
    start = (len(indices) * index) // count
    end = (len(indices) * Uint64(index + 1)) // count
    return [
        indices[compute_shuffled_index(Uint64(i), Uint64(len(indices)), seed)]
        for i in range(start, end)
    ]
```
-/
def compute_committee (p : Preset) (sha256 : List Nat → List Nat)
    (indices : List ValidatorIndex) (seed : List Nat) (index count : Uint64) :
    SpecM (List ValidatorIndex) := do
  let start ← uint64Div (← uint64Mul indices.length index) count
  let «end» ← uint64Div (← uint64Mul indices.length (← uint64Add index 1)) count
  (List.range' start («end» - start)).mapM fun i => do
    listGet indices (← compute_shuffled_index p sha256 i indices.length seed)

/-!
```python
def get_committee_count_per_slot(state: BeaconState, epoch: Epoch) -> Uint64:
    """
    Return the number of committees in each slot for the given ``epoch``.
    """
    return max(
        Uint64(1),
        min(
            MAX_COMMITTEES_PER_SLOT,
            Uint64(len(get_active_validator_indices(state, epoch)))
            // Uint64(SLOTS_PER_EPOCH)
            // TARGET_COMMITTEE_SIZE,
        ),
    )
```

`active_count` is `len(get_active_validator_indices(state, epoch))`.
-/
def get_committee_count_per_slot (p : Preset) (active_count : Nat) : SpecM Uint64 := do
  return max 1 (min p.MAX_COMMITTEES_PER_SLOT
    (← uint64Div (← uint64Div active_count p.SLOTS_PER_EPOCH) p.TARGET_COMMITTEE_SIZE))

/-!
```python
def get_beacon_committee(
    state: BeaconState, slot: Slot, index: CommitteeIndex
) -> Sequence[ValidatorIndex]:
    """
    Return the beacon committee at ``slot`` for ``index``.
    """
    epoch = compute_epoch_at_slot(slot)
    committees_per_slot = get_committee_count_per_slot(state, epoch)
    return compute_committee(
        indices=get_active_validator_indices(state, epoch),
        seed=get_seed(state, epoch, DOMAIN_BEACON_ATTESTER),
        index=Uint64(slot % SLOTS_PER_EPOCH) * committees_per_slot + index,
        count=committees_per_slot * Uint64(SLOTS_PER_EPOCH),
    )
```

`indices` and `seed` are the values for `compute_epoch_at_slot(slot)`.
-/
def get_beacon_committee (p : Preset) (sha256 : List Nat → List Nat)
    (indices : List ValidatorIndex) (seed : List Nat) (slot : Slot) (index : CommitteeIndex) :
    SpecM (List ValidatorIndex) := do
  let committees_per_slot ← get_committee_count_per_slot p indices.length
  compute_committee p sha256 indices seed
    (← uint64Add (← uint64Mul (← uint64Mod slot p.SLOTS_PER_EPOCH) committees_per_slot) index)
    (← uint64Mul committees_per_slot p.SLOTS_PER_EPOCH)

/-!
```python
def get_committee_assignment(
    state: BeaconState, epoch: Epoch, validator_index: ValidatorIndex
) -> Optional[Tuple[Sequence[ValidatorIndex], CommitteeIndex, Slot]]:
    """
    Return the committee assignment in the ``epoch`` for ``validator_index``.
    ``assignment`` returned is a tuple of the following form:
        * ``assignment[0]`` is the list of validators in the committee
        * ``assignment[1]`` is the index to which the committee is assigned
        * ``assignment[2]`` is the slot at which the committee is assigned
    Return None if no assignment.
    """
    next_epoch = get_current_epoch(state) + 1
    assert epoch <= next_epoch

    start_slot = compute_start_slot_at_epoch(epoch)
    committee_count_per_slot = get_committee_count_per_slot(state, epoch)
    for slot in range(start_slot, start_slot + SLOTS_PER_EPOCH):
        for index in range(committee_count_per_slot):
            committee = get_beacon_committee(state, Slot(slot), CommitteeIndex(index))
            if validator_index in committee:
                return committee, CommitteeIndex(index), Slot(slot)
    return None
```

`current_epoch` is `get_current_epoch(state)`. `indices` and `seed` are the values for `epoch`.
`compute_start_slot_at_epoch(epoch)` is `Slot(epoch * SLOTS_PER_EPOCH)`.
-/
def get_committee_assignment (p : Preset) (sha256 : List Nat → List Nat)
    (indices : List ValidatorIndex) (seed : List Nat) (current_epoch epoch : Epoch)
    (validator_index : ValidatorIndex) :
    SpecM (Option (List ValidatorIndex × CommitteeIndex × Slot)) := do
  let next_epoch ← uint64Add current_epoch 1
  if ¬ epoch ≤ next_epoch then throw .assertionFailed
  let start_slot ← uint64Mul epoch p.SLOTS_PER_EPOCH
  let committee_count_per_slot ← get_committee_count_per_slot p indices.length
  for slot in List.range' start_slot ((← uint64Add start_slot p.SLOTS_PER_EPOCH) - start_slot) do
    for index in List.range committee_count_per_slot do
      let committee ← get_beacon_committee p sha256 indices seed slot index
      if validator_index ∈ committee then
        return some (committee, index, slot)
  return none

end CacheProofs.Spec.CommitteeCache
