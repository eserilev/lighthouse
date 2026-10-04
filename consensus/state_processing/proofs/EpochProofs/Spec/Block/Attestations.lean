import EpochProofs.Spec.Committees
import EpochProofs.Spec.Block.Slashings

/-!
# Reference: `process_attestation` and `process_payload_attestation`

Transcribed from `specs/altair`, `specs/electra` and `specs/gloas/beacon-chain.md`
(v1.7.0-beta.2). Each definition quotes the pyspec of its latest fork. Hashes and BLS come from
the `Oracle`.
-/

namespace EpochProofs.Spec

/-- ```python
def is_attestation_same_slot(state: BeaconState, data: AttestationData) -> bool:
    if data.slot == 0:
        return True

    blockroot = data.beacon_block_root
    slot_blockroot = get_block_root_at_slot(state, data.slot)
    prev_blockroot = get_block_root_at_slot(state, data.slot - 1)

    return blockroot == slot_blockroot and blockroot != prev_blockroot
``` -/
def is_attestation_same_slot (p : Preset) (state : BeaconState) (data : AttestationData) :
    SpecM Bool := do
  if data.slot == 0 then
    return true

  let blockroot := data.beacon_block_root
  let slot_blockroot ← get_block_root_at_slot p state data.slot
  let prev_blockroot ← get_block_root_at_slot p state (← uint64Sub data.slot 1)

  pure (blockroot == slot_blockroot && blockroot != prev_blockroot)

/-- ```python
def get_attestation_participation_flag_indices(
    state: BeaconState,
    data: AttestationData,
    inclusion_delay: Uint64,
    # [New in Gloas:EIP7732]
    parent_slot: Slot,
) -> Sequence[int]:
    # Matching source
    if data.target.epoch == get_current_epoch(state):
        justified_checkpoint = state.current_justified_checkpoint
    else:
        justified_checkpoint = state.previous_justified_checkpoint
    is_matching_source = data.source == justified_checkpoint

    # Matching target
    target_root = get_block_root(state, data.target.epoch)
    target_root_matches = data.target.root == target_root
    is_matching_target = is_matching_source and target_root_matches

    # [New in Gloas:EIP7732]
    if is_attestation_same_slot(state, data):
        assert data.index == 0
        payload_matches = True
    else:
        slot_index = parent_slot % SLOTS_PER_HISTORICAL_ROOT
        payload_index = state.execution_payload_availability[slot_index]
        payload_matches = Uint64(data.index) == Uint64(payload_index)

    # Matching head
    head_root = get_block_root_at_slot(state, data.slot)
    head_root_matches = data.beacon_block_root == head_root
    # [Modified in Gloas:EIP7732]
    is_matching_head = is_matching_target and head_root_matches and payload_matches

    assert is_matching_source

    participation_flag_indices = []
    if is_matching_source and inclusion_delay <= integer_squareroot(SLOTS_PER_EPOCH):
        participation_flag_indices.append(TIMELY_SOURCE_FLAG_INDEX)
    if is_matching_target:
        participation_flag_indices.append(TIMELY_TARGET_FLAG_INDEX)
    if is_matching_head and inclusion_delay == MIN_ATTESTATION_INCLUSION_DELAY:
        participation_flag_indices.append(TIMELY_HEAD_FLAG_INDEX)

    return participation_flag_indices
```

`Uint64(payload_index)` of a `boolean` is 0 or 1. -/
def get_attestation_participation_flag_indices (p : Preset) (state : BeaconState)
    (data : AttestationData) (inclusion_delay : Uint64) (parent_slot : Slot) :
    SpecM (List Nat) := do
  let justified_checkpoint :=
    if data.target.epoch == (← get_current_epoch p state) then state.current_justified_checkpoint
    else state.previous_justified_checkpoint
  let is_matching_source := data.source == justified_checkpoint

  let target_root ← get_block_root p state data.target.epoch
  let target_root_matches := data.target.root == target_root
  let is_matching_target := is_matching_source && target_root_matches

  let payload_matches ←
    if (← is_attestation_same_slot p state data) then do
      if ¬ data.index = 0 then throw .assertionFailed
      pure true
    else do
      let slot_index ← uint64Mod parent_slot p.SLOTS_PER_HISTORICAL_ROOT
      let payload_index ← listGet state.execution_payload_availability slot_index
      pure (data.index == (if payload_index then 1 else 0))

  let head_root ← get_block_root_at_slot p state data.slot
  let head_root_matches := data.beacon_block_root == head_root
  let is_matching_head := is_matching_target && head_root_matches && payload_matches

  if ¬ is_matching_source then throw .assertionFailed

  let mut participation_flag_indices := []
  if is_matching_source && inclusion_delay ≤ integer_squareroot p.SLOTS_PER_EPOCH then
    participation_flag_indices := participation_flag_indices ++ [TIMELY_SOURCE_FLAG_INDEX]
  if is_matching_target then
    participation_flag_indices := participation_flag_indices ++ [TIMELY_TARGET_FLAG_INDEX]
  if is_matching_head && inclusion_delay == p.MIN_ATTESTATION_INCLUSION_DELAY then
    participation_flag_indices := participation_flag_indices ++ [TIMELY_HEAD_FLAG_INDEX]

  pure participation_flag_indices

/-- The loop over the attesters of `process_attestation`. It returns the new
`epoch_participation`, `proposer_reward_numerator` and `payment`.

`get_base_reward` and `is_attestation_same_slot` read `state`. The loop writes only
`epoch_participation` and `payment`, so they read the input `state`. -/
def process_attestation_loop (p : Preset) (state : BeaconState) (data : AttestationData)
    (participation_flag_indices : List Nat) (attesting_indices : List ValidatorIndex)
    (epoch_participation : List ParticipationFlags) (payment : BuilderPendingPayment) :
    SpecM (List ParticipationFlags × Gwei × BuilderPendingPayment) := do
  let mut epoch_participation := epoch_participation
  let mut payment := payment
  let mut proposer_reward_numerator := 0
  for index in attesting_indices do
    let had_no_participation := (← listGet epoch_participation index) == 0
    let mut will_set_new_flag := false

    for (weight, flag_index) in PARTICIPATION_FLAG_WEIGHTS.zipIdx do
      if flag_index ∈ participation_flag_indices then
        if !(← has_flag (← listGet epoch_participation index) flag_index) then
          epoch_participation ← listSet epoch_participation index
            (← add_flag (← listGet epoch_participation index) flag_index)
          let base_reward ←
            get_base_reward p (← get_total_active_balance p state) state index
          proposer_reward_numerator ←
            uint64Add proposer_reward_numerator (← uint64Mul base_reward weight)
          will_set_new_flag := true

    if will_set_new_flag && had_no_participation then
      if (← is_attestation_same_slot p state data) && payment.withdrawal.amount > 0 then
        let effective_balance := (← listGet state.validators index).effective_balance
        payment := { payment with weight := ← uint64Add payment.weight effective_balance }
  pure (epoch_participation, proposer_reward_numerator, payment)

/-- The asserts of `process_attestation`, up to the signature check. It returns
`participation_flag_indices`. -/
def process_attestation_checks (p : Preset) (o : Oracle) (state : BeaconState)
    (attestation : Attestation) (parent_slot : Slot) : SpecM (List Nat) := do
  let data := attestation.data
  let previous_epoch ← get_previous_epoch p state
  let current_epoch ← get_current_epoch p state
  if !(data.target.epoch == previous_epoch || data.target.epoch == current_epoch) then
    throw .assertionFailed
  if ¬ data.target.epoch = (← compute_epoch_at_slot p data.slot) then throw .assertionFailed
  if ¬ (← uint64Add data.slot p.MIN_ATTESTATION_INCLUSION_DELAY) ≤ state.slot then
    throw .assertionFailed

  if ¬ data.index < 2 then throw .assertionFailed
  let committee_indices := get_committee_indices attestation.committee_bits
  let mut committee_offset := 0
  for committee_index in committee_indices do
    if ¬ committee_index < (← get_committee_count_per_slot p state data.target.epoch) then
      throw .assertionFailed
    let committee ← get_beacon_committee p o state data.slot committee_index
    let attesters ← committee_attesters attestation.aggregation_bits committee_offset committee
    if ¬ attesters.length > 0 then throw .assertionFailed
    committee_offset := committee_offset + committee.length

  if ¬ attestation.aggregation_bits.length = committee_offset then throw .assertionFailed

  let participation_flag_indices ← get_attestation_participation_flag_indices p state data
    (← uint64Sub state.slot data.slot) parent_slot

  if ¬ (← is_valid_indexed_attestation p o state (← get_indexed_attestation p o state attestation))
  then throw .assertionFailed

  pure participation_flag_indices

/-- ```python
def process_attestation(
    state: BeaconState,
    attestation: Attestation,
    # [New in Gloas:EIP7732]
    parent_slot: Slot,
) -> None:
    data = attestation.data
    assert data.target.epoch in (get_previous_epoch(state), get_current_epoch(state))
    assert data.target.epoch == compute_epoch_at_slot(data.slot)
    assert data.slot + MIN_ATTESTATION_INCLUSION_DELAY <= state.slot

    # [Modified in Gloas:EIP7732]
    assert data.index < 2
    committee_indices = get_committee_indices(attestation.committee_bits)
    committee_offset = 0
    for committee_index in committee_indices:
        assert committee_index < get_committee_count_per_slot(state, data.target.epoch)
        committee = get_beacon_committee(state, data.slot, committee_index)
        committee_attesters = {
            attester_index
            for i, attester_index in enumerate(committee)
            if attestation.aggregation_bits[committee_offset + i]
        }
        assert len(committee_attesters) > 0
        committee_offset += len(committee)

    # Bitfield length matches total number of participants
    assert len(attestation.aggregation_bits) == committee_offset

    # Participation flag indices
    # [Modified in Gloas:EIP7732]
    participation_flag_indices = get_attestation_participation_flag_indices(
        state, data, state.slot - data.slot, parent_slot
    )

    # Verify signature
    assert is_valid_indexed_attestation(state, get_indexed_attestation(state, attestation))

    # [Modified in Gloas:EIP7732]
    if data.target.epoch == get_current_epoch(state):
        current_epoch_target = True
        epoch_participation = state.current_epoch_participation
        payment = state.builder_pending_payments[SLOTS_PER_EPOCH + data.slot % SLOTS_PER_EPOCH]
    else:
        current_epoch_target = False
        epoch_participation = state.previous_epoch_participation
        payment = state.builder_pending_payments[data.slot % SLOTS_PER_EPOCH]

    proposer_reward_numerator = 0
    for index in get_attesting_indices(state, attestation):
        # [New in Gloas:EIP7732]
        had_no_participation = epoch_participation[index] == 0b0000_0000
        will_set_new_flag = False

        for flag_index, weight in enumerate(PARTICIPATION_FLAG_WEIGHTS):
            if flag_index in participation_flag_indices and not has_flag(
                epoch_participation[index], flag_index
            ):
                epoch_participation[index] = add_flag(epoch_participation[index], flag_index)
                proposer_reward_numerator += get_base_reward(state, index) * weight
                # [New in Gloas:EIP7732]
                will_set_new_flag = True

        # [New in Gloas:EIP7732]
        if (
            will_set_new_flag
            and had_no_participation
            and is_attestation_same_slot(state, data)
            and payment.withdrawal.amount > 0
        ):
            payment.weight += state.validators[index].effective_balance

    # Reward proposer
    proposer_reward_denominator = (
        (WEIGHT_DENOMINATOR - PROPOSER_WEIGHT) * WEIGHT_DENOMINATOR // PROPOSER_WEIGHT
    )
    proposer_reward = Gwei(proposer_reward_numerator // proposer_reward_denominator)
    increase_balance(state, get_beacon_proposer_index(state), proposer_reward)

    # [New in Gloas:EIP7732]
    # Update builder payment weight
    if current_epoch_target:
        state.builder_pending_payments[SLOTS_PER_EPOCH + data.slot % SLOTS_PER_EPOCH] = payment
    else:
        state.builder_pending_payments[data.slot % SLOTS_PER_EPOCH] = payment
```

The asserts are `process_attestation_checks`. The loop over the attesters is
`process_attestation_loop`. `epoch_participation` is a view of a state field, so the reference
writes it back to that field. `committee_offset` is a Python `int`. `proposer_reward_numerator`
starts as an `int`, but `int + Uint64` is a `Uint64`, so the sum raises on overflow. -/
def process_attestation (p : Preset) (o : Oracle) (state : BeaconState)
    (attestation : Attestation) (parent_slot : Slot) : SpecM BeaconState := do
  let data := attestation.data
  let participation_flag_indices ←
    process_attestation_checks p o state attestation parent_slot

  let current_epoch_target := data.target.epoch == (← get_current_epoch p state)
  let epoch_participation :=
    if current_epoch_target then state.current_epoch_participation
    else state.previous_epoch_participation
  let payment_index ←
    if current_epoch_target then
      uint64Add p.SLOTS_PER_EPOCH (← uint64Mod data.slot p.SLOTS_PER_EPOCH)
    else uint64Mod data.slot p.SLOTS_PER_EPOCH
  let payment ← listGet state.builder_pending_payments payment_index

  let (epoch_participation, proposer_reward_numerator, payment) ←
    process_attestation_loop p state data participation_flag_indices
      (← get_attesting_indices p o state attestation) epoch_participation payment

  let proposer_reward_denominator ← uint64Div
    (← uint64Mul (← uint64Sub WEIGHT_DENOMINATOR PROPOSER_WEIGHT) WEIGHT_DENOMINATOR)
    PROPOSER_WEIGHT
  let proposer_reward ← uint64Div proposer_reward_numerator proposer_reward_denominator
  let balances ←
    increase_balance state.balances (← get_beacon_proposer_index p state) proposer_reward

  let builder_pending_payments ← listSet state.builder_pending_payments payment_index payment
  if current_epoch_target then
    pure { state with
      current_epoch_participation := epoch_participation, balances, builder_pending_payments }
  else
    pure { state with
      previous_epoch_participation := epoch_participation, balances, builder_pending_payments }

/-- ```python
def get_indexed_payload_attestation(
    state: BeaconState, payload_attestation: PayloadAttestation
) -> IndexedPayloadAttestation:
    slot = payload_attestation.data.slot
    ptc = get_ptc(state, slot)
    bits = payload_attestation.aggregation_bits
    attesting_indices = [index for i, index in enumerate(ptc) if bits[i]]

    return IndexedPayloadAttestation(
        attesting_indices=PayloadTimelinessCommitteeIndices(data=sorted(attesting_indices)),
        data=payload_attestation.data,
        signature=payload_attestation.signature,
    )
``` -/
def get_indexed_payload_attestation (p : Preset) (GLOAS_FORK_EPOCH : Epoch)
    (state : BeaconState) (payload_attestation : PayloadAttestation) :
    SpecM IndexedPayloadAttestation := do
  let slot := payload_attestation.data.slot
  let ptc ← get_ptc p GLOAS_FORK_EPOCH state slot
  let bits := payload_attestation.aggregation_bits
  let attesting_indices ← (ptc.zipIdx.filterM fun (_, i) => listGet bits i)

  pure {
    attesting_indices := (attesting_indices.map (·.1)).mergeSort
    data := payload_attestation.data
    signature := payload_attestation.signature }

/-- ```python
def is_valid_indexed_payload_attestation(
    state: BeaconState, attestation: IndexedPayloadAttestation
) -> bool:
    # Verify indices are non-empty and sorted
    indices = attestation.attesting_indices
    if len(indices) == 0 or list(indices) != sorted(indices):
        return False

    # Verify aggregate signature
    pubkeys = [state.validators[i].pubkey for i in indices]
    domain = get_domain(state, DOMAIN_PTC_ATTESTER, compute_epoch_at_slot(attestation.data.slot))
    signing_root = compute_signing_root(attestation.data, domain)
    return bls.FastAggregateVerify(pubkeys, signing_root, attestation.signature)
``` -/
def is_valid_indexed_payload_attestation (p : Preset) (o : Oracle) (state : BeaconState)
    (attestation : IndexedPayloadAttestation) : SpecM Bool := do
  let indices := attestation.attesting_indices
  if indices.length == 0 || indices != indices.mergeSort then
    return false

  let pubkeys ← indices.mapM fun i => do pure (← listGet state.validators i).pubkey
  let domain ← get_domain p o state DOMAIN_PTC_ATTESTER
    (some (← compute_epoch_at_slot p attestation.data.slot))
  let signing_root :=
    compute_signing_root o (o.hash_tree_root_PayloadAttestationData attestation.data) domain
  pure (o.bls_FastAggregateVerify pubkeys signing_root attestation.signature)

/-- ```python
def process_payload_attestation(
    state: BeaconState, payload_attestation: PayloadAttestation
) -> None:
    data = payload_attestation.data

    # Check that the attestation is for the parent beacon block
    assert data.beacon_block_root == state.latest_block_header.parent_root
    # Check that the attestation is for the previous slot
    assert data.slot + 1 == state.slot
    # Verify signature
    indexed_payload_attestation = get_indexed_payload_attestation(state, payload_attestation)
    assert is_valid_indexed_payload_attestation(state, indexed_payload_attestation)
```

`GLOAS_FORK_EPOCH` is a parameter for `get_ptc`. -/
def process_payload_attestation (p : Preset) (o : Oracle) (GLOAS_FORK_EPOCH : Epoch)
    (state : BeaconState) (payload_attestation : PayloadAttestation) : SpecM BeaconState := do
  let data := payload_attestation.data

  if ¬ data.beacon_block_root = state.latest_block_header.parent_root then
    throw .assertionFailed
  if ¬ (← uint64Add data.slot 1) = state.slot then throw .assertionFailed
  let indexed_payload_attestation ←
    get_indexed_payload_attestation p GLOAS_FORK_EPOCH state payload_attestation
  if ¬ (← is_valid_indexed_payload_attestation p o state indexed_payload_attestation) then
    throw .assertionFailed
  pure state

end EpochProofs.Spec
