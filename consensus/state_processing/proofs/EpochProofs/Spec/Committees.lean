import EpochProofs.Spec.Helpers
import EpochProofs.Spec.PendingDeposits

/-!
# Reference: shuffling, committees and balance weighted selection

Transcribed from `specs/phase0`, `specs/altair`, `specs/electra` and `specs/gloas`
`beacon-chain.md` (v1.7.0-beta.2). Each definition quotes the pyspec of its latest fork.
`sha256` is `o.hash`. A Python `set` is a `List` without duplicates here.
-/

namespace EpochProofs.Spec

/-- ```python
def uint_to_bytes(n: Uint) -> bytes:
    return ssz_serialize(n)
```

The SSZ serialization of a `Uint` is little endian. `width` is the byte size of its type:
1 for `Uint8`, 4 for `Uint32`, 8 for `Uint64`. -/
def uint_to_bytes (width : Nat) (n : Nat) : List UInt8 :=
  (List.range width).map fun i => UInt8.ofNat (n / 256 ^ i % 256)

/-- ```python
def bytes_to_uint64(data: bytes) -> Uint64:
    return Uint64(int.from_bytes(data, ENDIANNESS))
```

`ENDIANNESS` is `'little'`. `Uint64(...)` raises when the value does not fit. -/
def bytes_to_uint64 (data : List UInt8) : SpecM Uint64 :=
  let n := data.foldr (fun b acc => b.toNat + 256 * acc) 0
  if n < UINT64_SIZE then pure n else throw .overflow

/-- ```python
def add_flag(flags: ParticipationFlags, flag_index: int) -> ParticipationFlags:
    flag = ParticipationFlags(2**flag_index)
    return flags | flag
```

`ParticipationFlags(2**flag_index)` raises when `flag_index ≥ 8`. -/
def add_flag (flags : ParticipationFlags) (flag_index : Nat) : SpecM ParticipationFlags := do
  if 2 ^ flag_index < 256 then
    let flag : ParticipationFlags := UInt8.ofNat (2 ^ flag_index)
    pure (flags ||| flag)
  else
    throw .overflow

/-- ```python
def compute_shuffled_permutation(index_count: Uint64, seed: Bytes32) -> Sequence[Uint64]:
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

`source_by_bucket` is a cache of `sha256` values. The reference computes the hash each time.
`Uint8(current_round)` and `Uint32(position_bucket)` raise when the value does not fit. -/
def compute_shuffled_permutation (p : Preset) (o : Oracle) (index_count : Uint64)
    (seed : Bytes32) : SpecM (List Uint64) := do
  let mut indices := List.range index_count
  for current_round in List.range p.SHUFFLE_ROUND_COUNT do
    if ¬ current_round < 256 then throw .overflow
    let round_bytes := uint_to_bytes 1 current_round
    let pivot ← uint64Mod
      (← bytes_to_uint64 ((o.hash (seed.toList ++ round_bytes)).toList.take 8)) index_count
    for i in List.range index_count do
      let flip ← uint64Mod (← uint64Sub (← uint64Add pivot index_count) (← listGet indices i))
        index_count
      let position := max (← listGet indices i) flip
      let position_bucket ← uint64Div position 256
      if ¬ position_bucket < 2 ^ 32 then throw .overflow
      let source := o.hash (seed.toList ++ round_bytes ++ uint_to_bytes 4 position_bucket)
      let byte_val ← listGet source.toList ((position % 256) / 8)
      let bit := (byte_val >>> UInt8.ofNat (position % 8)) % 2
      let current ← listGet indices i
      indices ← listSet indices i (if bit != 0 then flip else current)
  pure indices

/-- ```python
def compute_shuffled_index(index: Uint64, index_count: Uint64, seed: Bytes32) -> Uint64:
    assert index < index_count
    return compute_shuffled_permutation(index_count, seed)[index]
``` -/
def compute_shuffled_index (p : Preset) (o : Oracle) (index index_count : Uint64)
    (seed : Bytes32) : SpecM Uint64 := do
  if ¬ index < index_count then throw .assertionFailed
  listGet (← compute_shuffled_permutation p o index_count seed) index

/-- ```python
def get_seed(state: BeaconState, epoch: Epoch, domain_type: DomainType) -> Bytes32:
    mix = get_randao_mix(
        state, epoch + EPOCHS_PER_HISTORICAL_VECTOR - MIN_SEED_LOOKAHEAD - 1
    )  # Avoid underflow
    return sha256(domain_type + uint_to_bytes(epoch) + mix)
``` -/
def get_seed (p : Preset) (o : Oracle) (state : BeaconState) (epoch : Epoch)
    (domain_type : DomainType) : SpecM Bytes32 := do
  let mix ← get_randao_mix p state
    (← uint64Sub (← uint64Sub (← uint64Add epoch p.EPOCHS_PER_HISTORICAL_VECTOR)
      p.MIN_SEED_LOOKAHEAD) 1)
  pure (o.hash (domain_type.toList ++ uint_to_bytes 8 epoch ++ mix.toList))

/-- ```python
def compute_committee(
    indices: Sequence[ValidatorIndex], seed: Bytes32, index: Uint64, count: Uint64
) -> Sequence[ValidatorIndex]:
    start = (len(indices) * index) // count
    end = (len(indices) * Uint64(index + 1)) // count
    return [
        indices[compute_shuffled_index(Uint64(i), Uint64(len(indices)), seed)]
        for i in range(start, end)
    ]
``` -/
def compute_committee (p : Preset) (o : Oracle) (indices : List ValidatorIndex) (seed : Bytes32)
    (index count : Uint64) : SpecM (List ValidatorIndex) := do
  let start ← uint64Div (← uint64Mul indices.length index) count
  let end_ ← uint64Div (← uint64Mul indices.length (← uint64Add index 1)) count
  (List.range' start (end_ - start)).mapM fun i => do
    listGet indices (← compute_shuffled_index p o i indices.length seed)

/-- ```python
def get_committee_count_per_slot(state: BeaconState, epoch: Epoch) -> Uint64:
    return max(
        Uint64(1),
        min(
            MAX_COMMITTEES_PER_SLOT,
            Uint64(len(get_active_validator_indices(state, epoch)))
            // Uint64(SLOTS_PER_EPOCH)
            // TARGET_COMMITTEE_SIZE,
        ),
    )
``` -/
def get_committee_count_per_slot (p : Preset) (state : BeaconState) (epoch : Epoch) :
    SpecM Uint64 := do
  let per_slot ← uint64Div
    (← uint64Div (get_active_validator_indices state epoch).length p.SLOTS_PER_EPOCH)
    p.TARGET_COMMITTEE_SIZE
  pure (max 1 (min p.MAX_COMMITTEES_PER_SLOT per_slot))

/-- ```python
def get_beacon_committee(
    state: BeaconState, slot: Slot, index: CommitteeIndex
) -> Sequence[ValidatorIndex]:
    epoch = compute_epoch_at_slot(slot)
    committees_per_slot = get_committee_count_per_slot(state, epoch)
    return compute_committee(
        indices=get_active_validator_indices(state, epoch),
        seed=get_seed(state, epoch, DOMAIN_BEACON_ATTESTER),
        index=Uint64(slot % SLOTS_PER_EPOCH) * committees_per_slot + index,
        count=committees_per_slot * Uint64(SLOTS_PER_EPOCH),
    )
``` -/
def get_beacon_committee (p : Preset) (o : Oracle) (state : BeaconState) (slot : Slot)
    (index : CommitteeIndex) : SpecM (List ValidatorIndex) := do
  let epoch ← compute_epoch_at_slot p slot
  let committees_per_slot ← get_committee_count_per_slot p state epoch
  compute_committee p o
    (get_active_validator_indices state epoch)
    (← get_seed p o state epoch DOMAIN_BEACON_ATTESTER)
    (← uint64Add (← uint64Mul (← uint64Mod slot p.SLOTS_PER_EPOCH) committees_per_slot) index)
    (← uint64Mul committees_per_slot p.SLOTS_PER_EPOCH)

/-- ```python
def get_committee_indices(committee_bits: BitVector) -> Sequence[CommitteeIndex]:
    return [CommitteeIndex(index) for index, bit in enumerate(committee_bits) if bit]
``` -/
def get_committee_indices (committee_bits : List Bool) : List CommitteeIndex :=
  (committee_bits.zipIdx.filter (·.1)).map (·.2)

/-- The set `{attester_index for i, attester_index in enumerate(committee) if
aggregation_bits[committee_offset + i]}`. Python reads each bit, so a short bitfield raises. -/
def committee_attesters (aggregation_bits : List Bool) (committee_offset : Nat)
    (committee : List ValidatorIndex) : SpecM (List ValidatorIndex) := do
  let selected ← committee.zipIdx.filterM fun (_, i) =>
    listGet aggregation_bits (committee_offset + i)
  pure (selected.map (·.1)).eraseDups

/-- ```python
def get_attesting_indices(state: BeaconState, attestation: Attestation) -> Set[ValidatorIndex]:
    output: Set[ValidatorIndex] = set()
    committee_indices = get_committee_indices(attestation.committee_bits)
    committee_offset = 0
    for committee_index in committee_indices:
        committee = get_beacon_committee(state, attestation.data.slot, committee_index)
        committee_attesters = {
            attester_index
            for i, attester_index in enumerate(committee)
            if attestation.aggregation_bits[committee_offset + i]
        }
        output = output.union(committee_attesters)

        committee_offset += len(committee)

    return output
```

`committee_offset` is a Python `int`, so it does not overflow. -/
def get_attesting_indices (p : Preset) (o : Oracle) (state : BeaconState)
    (attestation : Attestation) : SpecM (List ValidatorIndex) := do
  let mut output : List ValidatorIndex := []
  let committee_indices := get_committee_indices attestation.committee_bits
  let mut committee_offset := 0
  for committee_index in committee_indices do
    let committee ← get_beacon_committee p o state attestation.data.slot committee_index
    let attesters ← committee_attesters attestation.aggregation_bits committee_offset committee
    output := output ++ attesters.filter (· ∉ output)
    committee_offset := committee_offset + committee.length
  pure output

/-- ```python
def get_indexed_attestation(state: BeaconState, attestation: Attestation) -> IndexedAttestation:
    attesting_indices = get_attesting_indices(state, attestation)

    return IndexedAttestation(
        attesting_indices=AttestingIndices(data=sorted(attesting_indices)),
        data=attestation.data,
        signature=attestation.signature,
    )
``` -/
def get_indexed_attestation (p : Preset) (o : Oracle) (state : BeaconState)
    (attestation : Attestation) : SpecM IndexedAttestation := do
  let attesting_indices ← get_attesting_indices p o state attestation
  pure {
    attesting_indices := attesting_indices.mergeSort
    data := attestation.data
    signature := attestation.signature }

/-- One pass of the `while` loop of `compute_balance_weighted_selection`, with at most `fuel`
passes. `random_bytes` is the value of the Python variable before the pass.

Python `i` is a `Uint64`, so `i += 1` raises when `i = 2**64 - 1`. So the `while` raises or
ends within `2**64` passes, and `UINT64_SIZE` fuel gives the pyspec result. -/
def compute_balance_weighted_selection_loop (p : Preset) (o : Oracle)
    (indices : List ValidatorIndex) (seed : Bytes32) (size : Uint64) (shuffle_indices : Bool)
    (total : Uint64) (effective_balances : List Gwei) :
    Nat → List ValidatorIndex → Uint64 → Bytes32 → SpecM (List ValidatorIndex)
  | 0, _, _, _ => throw .overflow
  | fuel + 1, selected, i, random_bytes => do
    if selected.length < size then
      let MAX_RANDOM_VALUE := 2 ^ 16 - 1
      let offset ← uint64Mul (← uint64Mod i 16) 2
      let random_bytes :=
        if offset == 0 then o.hash (seed.toList ++ uint_to_bytes 8 (i / 16)) else random_bytes
      let mut next_index ← uint64Mod i total
      if shuffle_indices then
        next_index ← compute_shuffled_index p o next_index total seed
      let weight ← uint64Mul (← listGet effective_balances next_index) MAX_RANDOM_VALUE
      let random_value ← bytes_to_uint64 ((random_bytes.toList.drop offset).take 2)
      let threshold ← uint64Mul p.MAX_EFFECTIVE_BALANCE_ELECTRA random_value
      let selected ←
        if weight ≥ threshold then pure (selected ++ [← listGet indices next_index])
        else pure selected
      let i ← uint64Add i 1
      compute_balance_weighted_selection_loop p o indices seed size shuffle_indices total
        effective_balances fuel selected i random_bytes
    else
      pure selected

/-- ```python
def compute_balance_weighted_selection(
    state: BeaconState,
    indices: Sequence[ValidatorIndex],
    seed: Bytes32,
    size: Uint64,
    shuffle_indices: bool,
) -> Sequence[ValidatorIndex]:
    MAX_RANDOM_VALUE = 2**16 - 1
    total = Uint64(len(indices))
    assert total > 0
    effective_balances = [state.validators[index].effective_balance for index in indices]
    selected: list[ValidatorIndex] = []
    i = Uint64(0)
    while len(selected) < size:
        offset = i % 16 * 2
        if offset == 0:
            random_bytes = sha256(seed + uint_to_bytes(i // 16))
        next_index = i % total
        if shuffle_indices:
            next_index = compute_shuffled_index(next_index, total, seed)
        weight = effective_balances[next_index] * MAX_RANDOM_VALUE
        random_value = bytes_to_uint64(random_bytes[offset : offset + 2])
        threshold = MAX_EFFECTIVE_BALANCE_ELECTRA * random_value
        if weight >= threshold:
            selected.append(indices[next_index])
        i += 1
    return selected
```

The first pass has `i = 0`, so it sets `random_bytes`. The initial `Bytes32.zero` is never
read. -/
def compute_balance_weighted_selection (p : Preset) (o : Oracle) (state : BeaconState)
    (indices : List ValidatorIndex) (seed : Bytes32) (size : Uint64) (shuffle_indices : Bool) :
    SpecM (List ValidatorIndex) := do
  let total := indices.length
  if ¬ total > 0 then throw .assertionFailed
  let effective_balances ← indices.mapM fun index => do
    pure (← listGet state.validators index).effective_balance
  compute_balance_weighted_selection_loop p o indices seed size shuffle_indices total
    effective_balances UINT64_SIZE [] 0 Bytes32.zero

/-- ```python
def compute_proposer_indices(
    state: BeaconState, epoch: Epoch, seed: Bytes32, indices: Sequence[ValidatorIndex]
) -> ProposerIndices:
    start_slot = compute_start_slot_at_epoch(epoch)
    seeds = [sha256(seed + uint_to_bytes(start_slot + i)) for i in range(SLOTS_PER_EPOCH)]
    # [Modified in Gloas:EIP7732]
    return ProposerIndices(
        data=[
            compute_balance_weighted_selection(
                state,
                indices,
                seed,
                size=Uint64(1),
                shuffle_indices=True,
            )[0]
            for seed in seeds
        ]
    )
``` -/
def compute_proposer_indices (p : Preset) (o : Oracle) (state : BeaconState) (epoch : Epoch)
    (seed : Bytes32) (indices : List ValidatorIndex) : SpecM (List ValidatorIndex) := do
  let start_slot ← compute_start_slot_at_epoch p epoch
  let seeds ← (List.range p.SLOTS_PER_EPOCH).mapM fun i => do
    pure (o.hash (seed.toList ++ uint_to_bytes 8 (← uint64Add start_slot i)))
  seeds.mapM fun seed => do
    listGet (← compute_balance_weighted_selection p o state indices seed 1 true) 0

/-- ```python
def get_beacon_proposer_indices(state: BeaconState, epoch: Epoch) -> ProposerIndices:
    # [Modified in Gloas:EIP8045]
    indices = [
        index
        for index in get_active_validator_indices(state, epoch)
        if not state.validators[index].slashed
    ]
    seed = get_seed(state, epoch, DOMAIN_BEACON_PROPOSER)
    return compute_proposer_indices(state, epoch, seed, indices)
``` -/
def get_beacon_proposer_indices (p : Preset) (o : Oracle) (state : BeaconState)
    (epoch : Epoch) : SpecM (List ValidatorIndex) := do
  let indices ← (get_active_validator_indices state epoch).filterM fun index => do
    pure !(← listGet state.validators index).slashed
  let seed ← get_seed p o state epoch DOMAIN_BEACON_PROPOSER
  compute_proposer_indices p o state epoch seed indices

/-- ```python
def get_next_sync_committee_indices(state: BeaconState) -> Sequence[ValidatorIndex]:
    epoch = get_current_epoch(state) + 1
    seed = get_seed(state, epoch, DOMAIN_SYNC_COMMITTEE)
    indices = get_active_validator_indices(state, epoch)
    return compute_balance_weighted_selection(
        state, indices, seed, size=SYNC_COMMITTEE_SIZE, shuffle_indices=True
    )
``` -/
def get_next_sync_committee_indices (p : Preset) (o : Oracle) (state : BeaconState) :
    SpecM (List ValidatorIndex) := do
  let epoch ← uint64Add (← get_current_epoch p state) 1
  let seed ← get_seed p o state epoch DOMAIN_SYNC_COMMITTEE
  let indices := get_active_validator_indices state epoch
  compute_balance_weighted_selection p o state indices seed p.SYNC_COMMITTEE_SIZE true

/-- ```python
def compute_ptc(state: BeaconState, slot: Slot) -> PayloadTimelinessCommittee:
    epoch = compute_epoch_at_slot(slot)
    seed = sha256(get_seed(state, epoch, DOMAIN_PTC_ATTESTER) + uint_to_bytes(slot))
    indices: list[ValidatorIndex] = []
    # Concatenate all committees for this slot in order
    committees_per_slot = get_committee_count_per_slot(state, epoch)
    for i in range(committees_per_slot):
        committee = get_beacon_committee(state, slot, CommitteeIndex(i))
        indices.extend(committee)
    return PayloadTimelinessCommittee(
        data=compute_balance_weighted_selection(
            state, indices, seed, size=PTC_SIZE, shuffle_indices=False
        )
    )
``` -/
def compute_ptc (p : Preset) (o : Oracle) (state : BeaconState) (slot : Slot) :
    SpecM (List ValidatorIndex) := do
  let epoch ← compute_epoch_at_slot p slot
  let seed := o.hash
    ((← get_seed p o state epoch DOMAIN_PTC_ATTESTER).toList ++ uint_to_bytes 8 slot)
  let mut indices : List ValidatorIndex := []
  let committees_per_slot ← get_committee_count_per_slot p state epoch
  for i in List.range committees_per_slot do
    let committee ← get_beacon_committee p o state slot i
    indices := indices ++ committee
  compute_balance_weighted_selection p o state indices seed p.PTC_SIZE false

/-- ```python
def get_ptc(state: BeaconState, slot: Slot) -> PayloadTimelinessCommittee:
    epoch = compute_epoch_at_slot(slot)
    assert epoch >= GLOAS_FORK_EPOCH
    state_epoch = get_current_epoch(state)
    if epoch < state_epoch:
        assert epoch + 1 == state_epoch
        return state.ptc_window[slot % SLOTS_PER_EPOCH]
    assert epoch <= state_epoch + MIN_SEED_LOOKAHEAD
    offset = Uint64(epoch - state_epoch + 1) * SLOTS_PER_EPOCH
    return state.ptc_window[offset + slot % SLOTS_PER_EPOCH]
```

`GLOAS_FORK_EPOCH` is a config value that `Preset` does not hold, so it is a parameter. -/
def get_ptc (p : Preset) (GLOAS_FORK_EPOCH : Epoch) (state : BeaconState) (slot : Slot) :
    SpecM (List ValidatorIndex) := do
  let epoch ← compute_epoch_at_slot p slot
  if ¬ epoch ≥ GLOAS_FORK_EPOCH then throw .assertionFailed
  let state_epoch ← get_current_epoch p state
  if epoch < state_epoch then
    if ¬ (← uint64Add epoch 1) = state_epoch then throw .assertionFailed
    return ← listGet state.ptc_window (← uint64Mod slot p.SLOTS_PER_EPOCH)
  if ¬ epoch ≤ (← uint64Add state_epoch p.MIN_SEED_LOOKAHEAD) then throw .assertionFailed
  let offset ← uint64Mul (← uint64Add (← uint64Sub epoch state_epoch) 1) p.SLOTS_PER_EPOCH
  listGet state.ptc_window (← uint64Add offset (← uint64Mod slot p.SLOTS_PER_EPOCH))

end EpochProofs.Spec
